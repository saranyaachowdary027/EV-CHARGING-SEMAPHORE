"""EV Charging Station Allocation System: single-process semaphore demo."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Semaphore, RLock
from urllib.parse import urlparse
import json, os, sqlite3, uuid
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DATABASE_PATH", os.path.join(ROOT, "data", "ev_charging.db"))
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
POINTS = max(1, int(os.environ.get("CHARGING_POINTS", "3")))
gate = Semaphore(POINTS)
lock = RLock()

def db():
    c = sqlite3.connect(DB_PATH, timeout=20)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c

def now(): return datetime.now(timezone.utc).isoformat(timespec="seconds")

def init():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, vehicle TEXT NOT NULL, status TEXT NOT NULL, point INTEGER, queued_at TEXT, started_at TEXT, ended_at TEXT);
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, kind TEXT NOT NULL, message TEXT NOT NULL);
        """)
        active = c.execute("SELECT COUNT(*) n FROM sessions WHERE status='charging'").fetchone()["n"]
        # Rebuild the in-memory counting semaphore from durable active sessions.
        for _ in range(min(active, POINTS)): gate.acquire()
        if active > POINTS:
            c.execute("UPDATE sessions SET status='queued', point=NULL, queued_at=? WHERE status='charging'", (now(),))
            log(c, "recovery", "Recovered after restart; excess sessions returned to queue")
        queued = c.execute("SELECT id FROM sessions WHERE status='queued' ORDER BY queued_at, id").fetchall()
        for row in queued:
            if gate.acquire(blocking=False):
                point = free_point(c)
                c.execute("UPDATE sessions SET status='charging', point=?, started_at=? WHERE id=?", (point, now(), row["id"]))
                log(c, "started", f"Queued session {row['id'][:8]} resumed on charger {point}")

def log(c, kind, message): c.execute("INSERT INTO events(at,kind,message) VALUES(?,?,?)", (now(),kind,message))
def free_point(c):
    busy={r[0] for r in c.execute("SELECT point FROM sessions WHERE status='charging'")}
    return next((n for n in range(1,POINTS+1) if n not in busy), None)
def snapshot():
    with lock, db() as c:
        sessions=[dict(r) for r in c.execute("SELECT * FROM sessions WHERE status IN ('charging','queued') ORDER BY CASE status WHEN 'charging' THEN 0 ELSE 1 END, queued_at, started_at")]
        events=[dict(r) for r in c.execute("SELECT * FROM events ORDER BY id DESC LIMIT 30")]
        busy=sum(s['status']=='charging' for s in sessions)
        return {"capacity":POINTS,"available":POINTS-busy,"active":busy,"sessions":sessions,"events":events}

def start(vehicle):
    vehicle=(vehicle or "").strip()[:40]
    if not vehicle: raise ValueError("Vehicle name or registration is required")
    sid=uuid.uuid4().hex
    with lock, db() as c:
        point=free_point(c) if gate.acquire(blocking=False) else None
        if point is None and c.execute("SELECT COUNT(*) FROM sessions WHERE status='charging'").fetchone()[0] < POINTS:
            # A slot may have become free just before the permit check; recover consistently.
            if gate.acquire(blocking=False): point=free_point(c)
        if point:
            c.execute("INSERT INTO sessions(id,vehicle,status,point,started_at) VALUES(?,?,'charging',?,?)",(sid,vehicle,point,now()))
            log(c,"started",f"{vehicle} started charging on charger {point}")
            return {"message":f"Charging started on charger {point}","id":sid}
        queued_at=now()
        c.execute("INSERT INTO sessions(id,vehicle,status,queued_at) VALUES(?,?,'queued',?)",(sid,vehicle,queued_at))
        place=c.execute("SELECT COUNT(*) FROM sessions WHERE status='queued'").fetchone()[0]
        log(c,"queued",f"{vehicle} added to the waiting queue at position {place}")
        return {"message":f"Added to waiting queue (position {place})","id":sid}

def finish(sid):
    with lock, db() as c:
        row=c.execute("SELECT * FROM sessions WHERE id=? AND status='charging'",(sid,)).fetchone()
        if not row: raise ValueError("Active charging session not found")
        c.execute("UPDATE sessions SET status='completed', ended_at=? WHERE id=?",(now(),sid))
        log(c,"completed",f"{row['vehicle']} completed charging on charger {row['point']}")
        gate.release()
        waiting=c.execute("SELECT * FROM sessions WHERE status='queued' ORDER BY queued_at,id LIMIT 1").fetchone()
        if waiting and gate.acquire(blocking=False):
            point=row['point']
            c.execute("UPDATE sessions SET status='charging', point=?, started_at=? WHERE id=?",(point,now(),waiting['id']))
            log(c,"started",f"{waiting['vehicle']} moved from queue and started on charger {point}")
        return {"message":"Session completed; next vehicle admitted if one was waiting"}

def release(sid):
    with lock, db() as c:
        row=c.execute("SELECT * FROM sessions WHERE id=? AND status='charging'",(sid,)).fetchone()
        if not row: raise ValueError("Active charging session not found")
        c.execute("UPDATE sessions SET status='released', ended_at=? WHERE id=?",(now(),sid))
        log(c,"released",f"{row['vehicle']} released charger {row['point']}")
        gate.release()
        waiting=c.execute("SELECT * FROM sessions WHERE status='queued' ORDER BY queued_at,id LIMIT 1").fetchone()
        if waiting and gate.acquire(blocking=False):
            c.execute("UPDATE sessions SET status='charging', point=?, started_at=? WHERE id=?",(row['point'],now(),waiting['id']))
            log(c,"started",f"{waiting['vehicle']} moved from queue and started on charger {row['point']}")
        return {"message":"Charger released; next vehicle admitted if one was waiting"}

def cancel_queued(sid):
    with lock, db() as c:
        row=c.execute("SELECT * FROM sessions WHERE id=? AND status='queued'",(sid,)).fetchone()
        if not row: raise ValueError("Waiting request not found")
        c.execute("UPDATE sessions SET status='cancelled', ended_at=? WHERE id=?",(now(),sid))
        log(c,"cancelled",f"{row['vehicle']} cancelled its waiting request")
        return {"message":f"Removed {row['vehicle']} from the waiting queue"}

def run_demo():
    """Send six sample vehicles through the real allocation path."""
    results=[start(f"EV-{i:02d}") for i in range(1,7)]
    return {"message":"Six demo requests submitted through the live semaphore and queue","results":results}

def advance_queue():
    with lock, db() as c:
        row=c.execute("SELECT id,vehicle FROM sessions WHERE status='charging' ORDER BY started_at,id LIMIT 1").fetchone()
    if not row: raise ValueError("No active session to complete")
    result=finish(row["id"])
    result["message"]=f"Completed {row['vehicle']}; the oldest queued vehicle was admitted if one was waiting"
    return result

class Handler(BaseHTTPRequestHandler):
    def send(self, code, obj, typ="application/json; charset=utf-8"):
        raw=obj if isinstance(obj,bytes) else json.dumps(obj).encode()
        self.send_response(code); self.send_header("Content-Type",typ); self.send_header("Content-Length",str(len(raw))); self.send_header("Cache-Control","no-store"); self.end_headers(); self.wfile.write(raw)
    def do_GET(self):
        path=urlparse(self.path).path
        if path=="/api/state": return self.send(200,snapshot())
        if path=="/health": return self.send(200,{"ok":True,"capacity":POINTS})
        if path=="/":
            try: raw=open(os.path.join(ROOT,"index.html"),"rb").read()
            except OSError: return self.send(500,{"error":"Dashboard file is missing"})
            return self.send(200,raw,"text/html; charset=utf-8")
        self.send(404,{"error":"Not found"})
    def do_POST(self):
        try:
            body=json.loads(self.rfile.read(int(self.headers.get("Content-Length",0))) or b"{}")
            path=urlparse(self.path).path
            if path=="/api/start": out=start(body.get("vehicle"))
            elif path.startswith("/api/complete/"): out=finish(path.rsplit("/",1)[1])
            elif path.startswith("/api/release/"): out=release(path.rsplit("/",1)[1])
            elif path.startswith("/api/cancel/"): out=cancel_queued(path.rsplit("/",1)[1])
            elif path=="/api/demo": out=run_demo()
            elif path=="/api/advance": out=advance_queue()
            else: return self.send(404,{"error":"Not found"})
            self.send(200,out)
        except (ValueError,json.JSONDecodeError) as e: self.send(400,{"error":str(e)})
        except Exception as e: self.send(500,{"error":"Server error","detail":str(e)})
    def log_message(self,*args): pass

if __name__=="__main__":
    init()
    port=int(os.environ.get("PORT","8000"))
    print(f"EV Charging dashboard listening on http://0.0.0.0:{port} ({POINTS} charging points)")
    ThreadingHTTPServer(("0.0.0.0",port),Handler).serve_forever()
