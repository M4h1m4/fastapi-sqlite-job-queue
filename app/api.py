from __future__ import annotations
import logging
from datetime import datetime
from uuid import UUID
from fastapi import APIRouter, File, HTTPException, UploadFile 
from fastapi.responses import JSONResponse

from .db import insert_job, fetch_job
from .schemas import CreateJobResponse, JobResultResponse, JobStatusResponse
from .worker import job_queue

logging = logging.getLogger("text-jobs")
router = APIRouter()

MAX_UPLOAD_BYTES = 1_000_000
@router.post("/jobs", response_model=CreateJobResponse, status_code=201)
async def create_job(file=UploadFile=File(...)):
    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File is too large")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded text")
    job_id = insert_job(text)
    job_queue.put(job_id)
    log.info("enqueued job %s", job_id)
    return CreateJobResponse(job_id=job_id, status="pending")

@router.get("/jobs/{job_id}/status", response_model=JobStatusResponse)
def get_status(job_id: UUID):
    row = fetch_job(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatusResponse(
        job_id=job_id,
        status=row["status"]
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        processing_by=row["processing_by"],
        lease_until = (datetime.fromisoformat(row["lease_until"]))
                        if row["lease_until"] else None,
    )

@router.get("/jobs/{job_id}/result", response_model=JobResultResponse)
def get_result(job_id: UUID):
    row = fetch_job(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    status = row["status"]
    rc = row["result_chars"]
    if status == "failed":
        return JSONResponse(
            status_code = 409,
            content={
                "job_id": str(job_id),
                "status": "failed",
                "attempts": int(row["attempts"] or 0),
                "error": row["last_error"] or "unknown",
            },
        )
    if status != "done" or rc is None:
        return JSONResponse(
            status_code=202,
            content={"job_id": str(job_id), "status": status, "message": "Result not ready"},
        )
    return JobResultResponse(job_id=job_id, status=status, characters=int(rc))

    