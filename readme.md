In SQLite, WAL (Write-Ahead Logging) mode lets you have:

Many concurrent readers, and

At most one writer at a time (writers are still serialized).

How it works (quickly):

Writers append changes to a separate dbname-wal file.

Readers keep reading a consistent snapshot from the main DB file while the writer appends—so reads don’t block writes, and writes don’t block reads (most of the time).

Only one writer can hold the write lock at commit time, so two simultaneous writes still contend. That’s why keeping write transactions short (and using a small busy_timeout) helps.



[App start]
  → create_app()
  → include_router(api)
  → on_startup:
      → init_db()  ⟶ (WAL, table/indexes, add attempts/last_error if missing)
      → start_workers(n=1) ⟶ spawn worker thread(s)

[Client → POST /jobs (file)]
  → read bytes → if >1MB → 413
  → decode UTF-8 → if fail → 400
  → insert_job(text)
      → uuid4() → job_id
      → DB INSERT { id=job_id, status="pending", attempts=0, last_error=NULL, ... }
  → enqueue job_id
  → 201 { job_id, status:"pending" }

[Worker thread: worker_loop()]
  → job_id = queue.get()  (FIFO remove)
  → try:
      → update_status(job_id,"started")
      → sleep 0.5
      → update_status(job_id,"processing")
      → text = fetch_job_text(job_id)
      → sleep 2.0 (simulate work)
      → chars = len(text)
      → update_status(job_id,"done", result_chars=chars)
    except Exception as e:
      → attempts = get_attempts(job_id)   # failures so far
      → if attempts < MAX_RETRIES:
            record_retry(job_id, str(e))  # attempts += 1, last_error set, status="pending"
            enqueue same job_id again
        else:
            record_failed(job_id, str(e)) # status="failed" (attempts unchanged here)
    finally:
      → queue.task_done()

[Client → GET /jobs/{job_id}/status]
  → DB SELECT (*)
    ↳ 404 if not found
    ↳ 200 { job_id, status, created_at, updated_at }

[Client → GET /jobs/{job_id}/result]
  → DB SELECT (*)
    ↳ 404 if not found
    ↳ if status == "failed" → 409 { job_id, status:"failed", attempts, error:last_error }
    ↳ else if status != "done" OR result_chars is NULL → 202 { job_id, status, "Result not ready" }
    ↳ else → 200 { job_id, status, characters: result_chars }


<!-- rewrite this with basic sql queries # done
introduce failure in worker loop using random
worker loop should be restarted
job_queue.task_done()--> look into this -->

install and use uv for running code
testing script and stores all the logs into a file 


more scenarios
Graceful failure: Worker loop can gracefully exit - when a exception is raised then exit the workerloop. implement logic to start new workerloop
worker abruptly stopped 
implement the maximum number of retries in a scenario where the worker loops are exited. 


