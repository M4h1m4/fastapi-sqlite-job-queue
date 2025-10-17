from __future__ import annotations
import logging
import threading
import time
import random
from queue import Queue 
from uuid import UUID
from .db import fetch_job_text, update_status, get_attempts, record_retry, record_failed, set_lease_started, extend_lease, clear_lease, reap_expired_ids, reset_to_pending

log = logging.getLogger("text-jobs")

job_queue: "Queue[UUID]" = Queue()

MAX_RETRIES = 2
LEASE_SEC = 5
REAPER_SEC = 5

class SimulatedCrash(Exception):
    pass

def worker_loop(thread_index: int, crash_after_dequeues: Optional[int]=None)-> None:
    label = f"w-{thread_index+1}"
    log.info("worker %d started", thread_index+1)
    dequeues = 0

    while True:
        job_id = job_queue.get()
        dequeues+=1
        set_lease_started(job_id, processing_by=label, lease_seconds=LEASE_SEC)
        
        if crash_after_dequeues is not None and thread_index == 0 and dequeues >= crash_after_dequeues:
            log.error("Simulated crash in %s after %d dequeues (job %s)", label, dequeues, job_id)
            raise SimulatedCrash("boom")

        try:
            if random.randint(1,1000) < 100: #(~10%)
                raise Exception("Injected failure: after_get")
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
            clear_lease(job_id)
        
        except SimulatedCrash:
            raise

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



def reaper_loop() -> None:
    log.info("reaper started (interval=%ss)", REAPER_SEC)
    while True:
        now_iso = datetime.utcnow().replace(microsecond=0).isoformat()
        expired = reap_expired_ids(now_iso)
        for idhex in expired:
            reset_to_pending(idhex)                 # clear lease + set pending
            job_queue.put(UUID(idhex))              # put back for workers
            log.warning("reaper: returned expired job %s to queue", idhex)
        time.sleep(REAPER_SEC)

def start_workers(n: int, crash_thread_index: int = 0, crash_after_dequeues: Optional[int] = None) -> None:
    for i in range(n):
        target = lambda i=1: worker_loop(i, crash_after_dequeues if i==crash_thread_index else None)
        t = threading.Thread(target=target, name=f"joba_worker-{i+1}", daemon=True)
        t.start()

def start_reaper() -> None:
    t = threading.Thread(target=reaper_loop, name="reaper", daemon=True)
    t.start()