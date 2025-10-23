from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime
from typing import NoReturn, Optional
from uuid import UUID 

from .db import (
    fetch_job_text,
    fetch_job,
    update_status,
    get_attempts,
    record_retry,
    record_failed,
    set_lease_started,
    extend_lease,
    clear_lease,
    reap_expired_ids,
    reset_to_pending,
)
log = logging.getLogger("text-jobs")
job_queue: "asyncio.Queue[UUID]" = asyncio.Queue()


MAX_RETRIES : int = 2
LEASE_SEC: int = 15
REAPER_SEC: int = 5
CRASH_P: float= 0.10 
AFTER_CLAIM_FAIL_P: float = 0.10
DURING_PROCESS_FAIL_P: float = 0.15
BEFORE_DONE_FAIL_P: float = 0.05

class SimulatedCrash(RuntimeError):
    """Used to kill a worker task abruptly (supervisor will restart it)."""

async def enqueue_job(job_id:UUID) -> None:
    await job_queue.put(job_id)

async def worker_loop(index:int) -> NoReturn:
    label: str = f"w-{index+1}"
    log.info("worker %s started", label)

    while True:
        job_id: UUID = await job_queue.get()
        await set_lease_started(job_id, processing_by=label, lease_seconds= LEASE_SEC)
        if random.random() < CRASH_P:
            log.error("Simulated hard Crash while owning the job %s", label, job_id)
            raise SimulatedCrash("boom")

        try:
            if random.random() < AFTER_CLAIM_FAIL_P:
                raise RuntimeError("Injected failure: after_claim")
            await asyncio.sleep(0.5)

            await update_status(job_id, "processing")
            if random.random() < DURING_PROCESS_FAIL_P:
                raise RuntimeError("Injected failure during processing")

            text: str = await fetch_job_text(job_id) or ""
            await asyncio.sleep(2.0)
            chars: int = len(text)

            if random.random() < BEFORE_DONE_FAIL_P:
                raise RuntimeError("Injected failure: before_done")

            await update_status(job_id, "done", result_chars=chars)
            await clear_lease(job_id)

        except Exception as e :
            try:
                failures: int = await get_attempts(job_id)
            except Exception:
                failures = 0
            if failures < MAX_RETRIES:
                await record_retry(job_id, str(e))
                await job_queue.put(job_id)
                log.warning("job %s failed (failures=%d/%d) — requeued", job_id, failures + 1, MAX_RETRIES)
            else:
                await record_failed(job_id, str(e))
                log.error("job %s failed permanently (failures=%d)", job_id, failures)
        finally:
            job_queue.task_done()

async def worker_supervisor(index:int) -> NoReturn:
    while True:
        try:
            await worker_loop(index)
        except SimulatedCrash:
            log.error("worker w-%d crashed — restarting soon", index + 1)
        except Exception as e:
            log.exception("worker w-%d died unexpectedly (%s) — restarting soon", index + 1, e)
        await asyncio.sleep(1.0)
    
async def start_workers(n: int) -> None:
    for i in range(n):
        asyncio.create_task(worker_supervisor(i), name=f"worker-supervisor-{i+1}")

async def reaper_loop() -> NoReturn:
    log.info("reaper started (interval=%ss)", REAPER_SEC)
    while True:
        now_iso: str = datetime.utcnow().replace(microsecond=0).isoformat()
        expired_ids: list[str] = await reap_expired_ids(now_iso)
        for idhex in expired_ids:
            await reset_to_pending(idhex)
            await job_queue.put(UUID(idhex))
            log.warning("reaper: returned expired job %s to queue", idhex)
        await asyncio.sleep(REAPER_SEC)


async def start_reaper() -> None:
    asyncio.create_task(reaper_loop(), name="reaper")

