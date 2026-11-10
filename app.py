
"""EV Charging Station Allocation System: single-process semaphore demo."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Semaphore, RLock
from urllib.parse import urlparse
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get(
    "DATABASE_PATH",
    os.path.join(ROOT, "data", "ev_charging.db")
)
os.makedirs(os.path.dirname(DB_PATH) or ROOT, exist_ok=True)

POINTS = max(1, int(os.environ.get("CHARGING_POINTS", "3")))
gate = Semaphore(POINTS)
lock = RLock()


def db():
    connection = sqlite3.connect(DB_PATH, timeout=20)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    return connection


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def log(connection, kind, message):
    connection.execute(
        "INSERT INTO events(at, kind, message) VALUES (?, ?, ?)",
        (now(), kind, message),
    )


def free_point(connection):
    busy = {
        row[0]
        for row in connection.execute(
            "SELECT point FROM sessions WHERE status='charging'"
        )
    }
    return next(
        (number for number in range(1, POINTS + 1) if number not in busy),
        None
    )


def init():
    """Create tables and rebuild semaphore permits from saved active sessions."""
    with lock, db() as connection:
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS sessions(
                id TEXT PRIMARY KEY,
                vehicle TEXT NOT NULL,
                status TEXT NOT NULL,
                point INTEGER,
                queued_at TEXT,
                started_at TEXT,
                ended_at TEXT
            );

            CREATE TABLE IF NOT EXISTS events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at TEXT NOT NULL,
                kind TEXT NOT NULL,
                message TEXT NOT NULL
            );
        """)

        active = connection.execute(
            "SELECT COUNT(*) FROM sessions WHERE status='charging'"
        ).fetchone()[0]

        for _ in range(min(active, POINTS)):
            gate.acquire()

        if active > POINTS:
            overflow = connection.execute("""
                SELECT id FROM sessions
                WHERE status='charging'
                ORDER BY started_at, rowid
                LIMIT -1 OFFSET ?
            """, (POINTS,)).fetchall()

            for row in overflow:
                connection.execute("""
                    UPDATE sessions
                    SET status='queued', point=NULL, queued_at=?
                    WHERE id=?
                """, (now(), row["id"]))

            log(
                connection,
                "recovery",
                "Recovered after restart; excess sessions returned to queue"
            )

        # SQLite rowid preserves the insertion order of queued requests.
        queued = connection.execute("""
            SELECT id FROM sessions
            WHERE status='queued'
            ORDER BY rowid
        """).fetchall()

        for row in queued:
            if gate.acquire(blocking=False):
                point = free_point(connection)
                if point is None:
                    gate.release()
                    break

                connection.execute("""
                    UPDATE sessions
                    SET status='charging', point=?, started_at=?
                    WHERE id=?
                """, (point, now(), row["id"]))

                log(
                    connection,
                    "started",
                    f"Queued session {row['id'][:8]} resumed on charger {point}"
                )


def snapshot():
    with lock, db() as connection:
        sessions = [
            dict(row)
            for row in connection.execute("""
                SELECT * FROM sessions
                WHERE status IN ('charging', 'queued')
                ORDER BY
                    CASE status WHEN 'charging' THEN 0 ELSE 1 END,
                    CASE WHEN status='charging' THEN started_at END,
                    CASE WHEN status='queued' THEN rowid END
            """)
        ]

        events = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT 30"
            )
        ]

        busy = sum(
            session["status"] == "charging"
            for session in sessions
        )

        return {
            "capacity": POINTS,
            "available": POINTS - busy,
            "active": busy,
            "sessions": sessions,
            "events": events,
        }


def start(vehicle):
    vehicle = (vehicle or "").strip()[:40]

    if not vehicle:
        raise ValueError("Vehicle name or registration is required")

    session_id = uuid.uuid4().hex

    with lock, db() as connection:
        point = None

        if gate.acquire(blocking=False):
            point = free_point(connection)

            if point is None:
                gate.release()

        if point is not None:
            connection.execute("""
                INSERT INTO sessions(
                    id, vehicle, status, point, started_at
                )
                VALUES (?, ?, 'charging', ?, ?)
            """, (session_id, vehicle, point, now()))

            log(
                connection,
                "started",
                f"{vehicle} started charging on charger {point}"
            )

            return {
                "message": f"Charging started on charger {point}",
                "id": session_id
            }

        queued_at = now()

        connection.execute("""
            INSERT INTO sessions(id, vehicle, status, queued_at)
            VALUES (?, ?, 'queued', ?)
        """, (session_id, vehicle, queued_at))

        position = connection.execute(
            "SELECT COUNT(*) FROM sessions WHERE status='queued'"
        ).fetchone()[0]

        log(
            connection,
            "queued",
            f"{vehicle} added to the waiting queue at position {position}"
        )

        return {
            "message": f"Added to waiting queue (position {position})",
            "id": session_id
        }


def admit_oldest_waiting(connection, charger_point):
    waiting = connection.execute("""
        SELECT id, vehicle FROM sessions
        WHERE status='queued'
        ORDER BY rowid
        LIMIT 1
    """).fetchone()

    if waiting and gate.acquire(blocking=False):
        connection.execute("""
            UPDATE sessions
            SET status='charging', point=?, started_at=?
            WHERE id=?
        """, (charger_point, now(), waiting["id"]))

        log(
            connection,
            "started",
            f"{waiting['vehicle']} moved from queue and started on charger {charger_point}"
        )

        return waiting["vehicle"]

    return None


def finish(session_id):
    with lock, db() as connection:
        row = connection.execute(
            "SELECT * FROM sessions WHERE id=? AND status='charging'",
            (session_id,)
        ).fetchone()

        if not row:
            raise ValueError("Active charging session not found")

        connection.execute(
            "UPDATE sessions SET status='completed', ended_at=? WHERE id=?",
            (now(), session_id)
        )

        log(
            connection,
            "completed",
            f"{row['vehicle']} completed charging on charger {row['point']}"
        )

        gate.release()
        admit_oldest_waiting(connection, row["point"])

        return {
            "message": "Session completed; next waiting vehicle admitted if available"
        }


def release(session_id):
    with lock, db() as connection:
        row = connection.execute(
            "SELECT * FROM sessions WHERE id=? AND status='charging'",
            (session_id,)
        ).fetchone()

        if not row:
            raise ValueError("Active charging session not found")

        connection.execute(
            "UPDATE sessions SET status='released', ended_at=? WHERE id=?",
            (now(), session_id)
        )

        log(
            connection,
            "released",
            f"{row['vehicle']} released charger {row['point']}"
        )

        gate.release()
        admit_oldest_waiting(connection, row["point"])

        return {
            "message": "Charger released; next waiting vehicle admitted if available"
        }


def cancel_queued(session_id):
    with lock, db() as connection:
        row = connection.execute(
            "SELECT * FROM sessions WHERE id=? AND status='queued'",
            (session_id,)
        ).fetchone()

        if not row:
            raise ValueError("Waiting request not found")

        connection.execute(
            "UPDATE sessions SET status='cancelled', ended_at=? WHERE id=?",
            (now(), session_id)
        )

        log(
            connection,
            "cancelled",
            f"{row['vehicle']} cancelled its waiting request"
        )

        return {
            "message": f"Removed {row['vehicle']} from the waiting queue"
        }


def run_demo():
    """Submit six sample vehicles through the live allocation path."""
    results = [
        start(f"EV-{number:02d}")
        for number in range(1, 7)
    ]

    return {
        "message": "Six demo requests submitted through the live semaphore and queue",
        "results": results
    }


def advance_queue():
    with lock, db() as connection:
        row = connection.execute("""
            SELECT id, vehicle FROM sessions
            WHERE status='charging'
            ORDER BY started_at, rowid
            LIMIT 1
        """).fetchone()

    if not row:
        raise ValueError("No active session to complete")

    finish(row["id"])

    return {
        "message": (
            f"Completed {row['vehicle']}; the oldest queued vehicle "
            "was admitted if one was waiting"
        )
    }


class Handler(BaseHTTPRequestHandler):

    def send(self, code, obj,
             content_type="application/json; charset=utf-8"):

        raw = obj if isinstance(obj, bytes) else json.dumps(obj).encode()

        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/api/state":
            return self.send(200, snapshot())

        if path == "/health":
            return self.send(200, {"ok": True, "capacity": POINTS})

        if path == "/":
            try:
                with open(os.path.join(ROOT, "index.html"), "rb") as file:
                    raw = file.read()
            except OSError:
                return self.send(
                    500,
                    {"error": "Dashboard file is missing"}
                )

            return self.send(
                200,
                raw,
                "text/html; charset=utf-8"
            )

        return self.send(404, {"error": "Not found"})

    def do_POST(self):
        try:
            body = json.loads(
                self.rfile.read(
                    int(self.headers.get("Content-Length", 0))
                ) or b"{}"
            )

            path = urlparse(self.path).path

            if path == "/api/start":
                output = start(body.get("vehicle"))

            elif path.startswith("/api/complete/"):
                output = finish(path.rsplit("/", 1)[1])

            elif path.startswith("/api/release/"):
                output = release(path.rsplit("/", 1)[1])

            elif path.startswith("/api/cancel/"):
                output = cancel_queued(path.rsplit("/", 1)[1])

            elif path == "/api/demo":
                output = run_demo()

            elif path == "/api/advance":
                output = advance_queue()

            else:
                return self.send(404, {"error": "Not found"})

            return self.send(200, output)

        except (ValueError, json.JSONDecodeError) as error:
            return self.send(400, {"error": str(error)})

        except Exception as error:
            return self.send(
                500,
                {"error": "Server error", "detail": str(error)}
            )

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    init()

    port = int(os.environ.get("PORT", "8000"))

    print(
        f"EV Charging dashboard listening on http://0.0.0.0:{port} "
        f"({POINTS} charging points)"
    )

    ThreadingHTTPServer(
        ("0.0.0.0", port),
        Handler
    ).serve_forever()
