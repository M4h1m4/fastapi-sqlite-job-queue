from __future__ import annotations
import logging
import threading
import time
import random
from queue import Queue 
from uuid import UUID
from .db import fetch_job_text, update_status, get_attempts, record_retry, record_failed

log = logging.getLogger("text-jobs")

job_queue: "Queue[UUID]" = Queue()
MAX_RETRIES = 2

def worker_loop()-> None:
    log.info("worker started")
    while True:
        job_id = job_queue.get()
        try:
            if random.randint(1,1000) < 100: #(~10%)
                raise Exception("Injected failure: after_get")

            update_status(job_id, "started")
            if random.randint(1, 1000) <= 200: #(~20%)
                raise Exception("Injected failure: after_started")

            time.sleep(0.5)

            update_status(job_id, "processing")
            if random.randint(1, 1000) <= 150: #(~15%)
                raise Exception("Injected failure: during_processing")

            text = fetch_job_text(job_id) or ""
            time.sleep(2.0)
            chars = len(text)

            if random.randint(1, 1000) <= 50: #(~5%)
                raise Exception("Injected failure: before_done")

            update_status(job_id, "done", result_chars=chars)
        except Exception as e:
            try:
                attempts = get_attempts(job_id)
            except Exception:
                attempts=0
            if attempts < MAX_RETRIES:
                record_retry(job_id, str(e))
                job_queue.put(job_id)
                log.warning("job %s failed (failures=%d/%d) — requeued",
                            job_id, attempts + 1, MAX_RETRIES)
            else:
                record_failed(job_id, str(e))
                log.error("job %s failed permanently (failures=%d)", job_id, attempts)

        finally:
            job_queue.task_done()

def start_workers(n: int=1) -> None:
    for i in range(n):
        t = threading.Thread(target=worker_loop, name=f"joba_worker-{i+1}", daemon=True)
        t.start()