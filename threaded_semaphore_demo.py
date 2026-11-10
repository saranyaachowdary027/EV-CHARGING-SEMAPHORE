"""Week 3 guided practical: 3 charger permits shared by 6 EV threads."""
import threading
import time

CHARGERS = 3
EV_REQUESTS = 6
CHARGING_SECONDS = 1

charging_sem = threading.Semaphore(CHARGERS)
count_lock = threading.Lock()
active = 0
peak_active = 0

def charge_ev(ev):
    global active, peak_active
    print(f"{ev} requests a charging point")
    charging_sem.acquire()
    try:
        with count_lock:
            active += 1
            peak_active = max(peak_active, active)
            if active > CHARGERS:
                raise AssertionError("Semaphore allowed more EVs than chargers")
            in_use = active
        print(f"{ev} charging started ({in_use}/{CHARGERS} points in use)")
        time.sleep(CHARGING_SECONDS)  # Simulated charging time for the lab demo.
        print(f"{ev} charging complete")
    finally:
        with count_lock:
            active -= 1
        charging_sem.release()
        print(f"{ev} released its charger permit")

if __name__ == "__main__":
    threads = [threading.Thread(target=charge_ev, args=(f"EV-{i:02d}",)) for i in range(1, EV_REQUESTS + 1)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print(f"All requests finished. Peak simultaneous charging: {peak_active}/{CHARGERS}.")
