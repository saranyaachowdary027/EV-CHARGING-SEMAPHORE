# EV Charging Station Allocation System

A full-stack educational simulation for the Product 2 / Week 3 Python counting-semaphore practical.

## Scenario from the practical PDF
- 3 charging points
- 6 EV requests
- Python `threading.Semaphore(3)`
- Extra requests wait until a charging point is released
- Each EV request follows request -> acquire -> charge -> release

## Technology
- Frontend: HTML, CSS, JavaScript
- Backend: Python 3 standard library (`http.server`, `threading`, `sqlite3`)
- Synchronization: `threading.Semaphore` plus an `RLock` for consistent application/database transitions
- Persistence: SQLite
- Third-party packages: none

## Run on Windows
1. Extract this ZIP using Windows File Explorer: right-click the ZIP -> **Extract All...**.
2. Open the extracted `EV_Charging_Station_Submission` folder.
3. Double-click `START_ON_WINDOWS.bat`, or open PowerShell in the folder and run `python app.py`.
4. Open Chrome on the same computer at http://127.0.0.1:8000.
5. Keep the terminal window open while using the dashboard.

If `python` is not recognized, install Python 3 from python.org and enable **Add Python to PATH**, then reopen the terminal.

## Presentation demo
1. Click **Run Six EV Demo** on the dashboard.
2. On a clean database, three vehicles charge and three wait.
3. Click **Complete next**. The oldest waiting EV should enter the freed charging point.
4. Run `python threaded_semaphore_demo.py` in a second terminal to show six actual Python threads and the blocking semaphore practical.
5. Expected final line: `All requests finished. Peak simultaneous charging: 3/3.`

## Core code
The dashboard backend in `app.py` creates `Semaphore(POINTS)` (default 3), checks permits before allocating a charging point, queues requests when all points are busy, and releases/reassigns permits when a session ends.

The separate `threaded_semaphore_demo.py` is the literal six-thread version of the assigned scenario. It creates six `threading.Thread` objects, uses `threading.Semaphore(3)`, calls `acquire()` before simulated charging and `release()` afterward, and uses `join()` to wait for all six threads.

## API
- `GET /` dashboard
- `GET /health` health check
- `GET /api/state` live station state
- `POST /api/start` start/queue a vehicle (`{"vehicle":"EV-101"}`)
- `POST /api/demo` submit six sample vehicles
- `POST /api/complete/{session_id}` complete a charging session
- `POST /api/release/{session_id}` release a charging point early
- `POST /api/cancel/{session_id}` cancel a queued request
- `POST /api/advance` complete the oldest active session and admit the oldest queued request

## Deployment
`render.yaml` is included as a Render Blueprint configuration. It requests a single Python web service and a persistent disk mounted at `/data`; the database is stored at `/data/ev_charging.db`. The semaphore is process-local, so use exactly one service instance/worker. Deployment still requires the project to be pushed to a Git repository and a hosting account to create the service. This package does not contain a public URL or hosting credentials.

## Limitations
This is a simulation, not a live physical charging controller. It has no charger hardware integration, sensor telemetry, authentication, payment processing, or multi-instance distributed locking. Do not use it to control real charging equipment.


## Four-member presentation split
- B. SARANYA — 2503A52637: overview, problem, semaphore definition and capacity.
- T. BINDHU — 2503A52638: acquire/wait/release and Python thread code.
- B. ANANYA — 2503A52639: architecture and live dashboard walkthrough.
- M. ARUN — 2503452640: testing, results, limitations and conclusion.

The dashboard includes a Semaphore learning section and the four-speaker plan.
