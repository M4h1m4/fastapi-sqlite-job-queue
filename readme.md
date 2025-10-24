FastAPI + SQLite Job Queue

Upload a text file → get a job_id → poll status/result.
Two builds:

Sync version → on main branch

Async version → on asyncv2 branch

🧭 Overview

A tiny service that accepts a text file, enqueues a job, and returns the number of characters when done. It demonstrates:

Clean FastAPI endpoints

Durable job state in SQLite

Background processing with a queue + workers

Retries, failure injection (for testing), and in the async build: leases + reaper + supervisor

Both versions expose the same API.

🌿 Branches

main (sync) — FastAPI + sqlite3 + threading.Queue + worker thread

asyncv2 (async) — FastAPI (async) + aiosqlite + asyncio.Queue + worker tasks, with connection pool, lease/claim, reaper, supervisor

✨ Features (common)

POST /jobs — upload a UTF-8 text file, returns { job_id, status:"pending" }

GET /jobs/{id}/status — returns { job_id, status, created_at, updated_at, ... }

GET /jobs/{id}/result —

200 when done → { job_id, status:"done", characters }

202 while in progress → { job_id, status, "Result not ready" }

409 when permanently failed → { job_id, status:"failed", attempts, error }

404 if unknown

Statuses: pending → started → processing → done (or failed)

🗃️ Data model (jobs table)
Column	Type	Notes
id	CHAR(32)	UUID (hex, no hyphens) primary key
status	TEXT	pending/started/processing/done/failed
text	TEXT	Uploaded file content
result_chars	INTEGER	Character count (set on success)
attempts	INTEGER	Number of failures so far
last_error	TEXT	Last failure message
processing_by	TEXT	Worker label (w-1, w-2, …)
lease_until	TEXT	ISO timestamp; reaper requeues expired leases
created_at	TEXT	ISO timestamp (UTC)
updated_at	TEXT	ISO timestamp (UTC)

Indexes: status, updated_at, and (status, lease_until).


🔌 API quickstart (works for both versions)
# 1) Start server (see per-branch commands below)
# 2) Health
curl http://127.0.0.1:8000/healthz

# 3) Create a job
echo "hello async world" > notes.txt
curl -F "file=@notes.txt" http://127.0.0.1:8000/jobs
# => {"job_id":"<UUID>", "status":"pending"}

# 4) Check status
curl "http://127.0.0.1:8000/jobs/<UUID>/status"

# 5) Get result
curl -i "http://127.0.0.1:8000/jobs/<UUID>/result"
# 202 while pending/processing; 200 with {characters} when done; 409 if failed


🧱 Sync version (branch: main)

Stack

FastAPI (sync routes)

Python stdlib sqlite3

threading.Thread workers + queue.Queue (FIFO)

Simple retries (immediate, capped)

How it works

POST /jobs inserts a row (pending) and puts job_id on a **threading.Queue`.

A background worker thread does the work (len(text)), updating status through phases.

Retries: on error, increments attempts and re-enqueues up to a cap; then marks failed.

Run 

# get the sync code
git checkout main

# install deps
pip install -r requirements.txt
# (sync requirements: fastapi, uvicorn, pydantic, python-multipart)

# start
uvicorn main:app --reload  # or uvicorn app.app:app --reload if packaged

Test
echo "hello" > notes.txt
curl -F "file=@notes.txt" http://127.0.0.1:8000/jobs
The sync build is great for understanding fundamentals (CRUD, queue, worker, retries). It uses one process and blocks threads when sleeping/doing I/O.

Async version (branch: asyncv2)

Stack

FastAPI (async endpoints)

aiosqlite with a tiny connection pool

asyncio.Queue + async worker tasks

Leases (lease_until) to claim work, reaper to requeue expired jobs

Supervisor restarts crashed workers

Immediate retries with cap

Fault injection (optional) to test robustness

Why async?

No thread blocking: awaited DB I/O and sleeps yield to the loop.

Connection pool reuses a few open SQLite connections efficiently.

Leases + reaper + supervisor provide self-healing behavior.

Run
git checkout asyncv2
pip install -r requirements.txt
# (async adds: aiosqlite, httpx for tests; plus fastapi, uvicorn, python-multipart)

# From repo root (package layout: app/)
PYTHONPATH=. uvicorn app.app:app --reload --host 127.0.0.1 --port 8000


Test(curl)

echo "hello async world" > notes.txt
curl -F "file=@notes.txt" http://127.0.0.1:8000/jobs


Test Script (async)
python3 scripts/test.py --api http://127.0.0.1:8000 --file notes.txt --count 3 --log client.log


Async Architecture 
Client → POST /jobs
  → insert row (pending) → put job_id on asyncio.Queue → 201 response

Workers (N tasks)
  loop:
    job_id = await queue.get()
    set_lease_started(job_id, processing_by='w-i', lease=now+LEASE_SEC)
    update_status('processing')
    text = fetch_job_text(job_id)
    await asyncio.sleep(2.0)
    update_status('done', result_chars=len(text))
    clear_lease(job_id)
    queue.task_done()

Reaper (every REAPER_SEC)
  finds rows with lease_until < now (started/processing) → reset to pending → re-enqueue

Supervisor
  restarts any crashed worker task

Failure handling

Handled exceptions: attempts += 1, status → pending, clear lease, re-enqueue (until cap).

Crash: worker task dies → supervisor restarts it. Reaper re-enqueues any job whose lease expired (so work isn’t lost).

Idempotent work (len(text)) means late “done” updates are safe.

