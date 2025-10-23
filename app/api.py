from types import EllipsisType
from fastapi import APIRouter, UploadFile, File, HTTPException, Request
from uuid import UUID
import logging

from .schemas import CreateJobResponse, JobResultResponse, JobStatusResponse
from .worker import enqueue_job
from .db import insert_job

log = logging.getLogger("text-jobs")
router = APIRouter()
MAX_UPLOAD_BYTES = 1_000_000  # ~1MB

@router.post("/jobs", response_model=CreateJobResponse, status_code=201)
async def create_job(request: Request, file: UploadFile | None = File(None)) -> CreateJobResponse:
    # Log incoming content type
    log.info("CT: %s", request.headers.get("content-type"))

    # Recover if FastAPI didn't bind UploadFile (ellipsis case) or field missing
    if file is None or isinstance(getattr(file, "file", None), EllipsisType):
        form = await request.form()
        candidate = form.get("file")
        if candidate is None:
            raise HTTPException(status_code=400, detail="Expected multipart form field 'file'")
        if not isinstance(candidate, UploadFile):
            raise HTTPException(status_code=400, detail="Form field 'file' is not an UploadFile")
        file = candidate

    raw: bytes = await file.read()
    log.info("upload bytes=%d name=%s", len(raw), file.filename)

    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large (max 1MB)")

    try:
        text: str = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded text")

    try:
        job_id: UUID = await insert_job(text)
    except Exception as e:
        log.exception("insert_job failed: %s", e)
        raise HTTPException(status_code=500, detail="DB error")

    await enqueue_job(job_id)
    log.info("enqueued job %s", job_id)
    return CreateJobResponse(job_id=job_id, status="pending")


@router.post("/jobs/{job_id}/status", response_model=JobStatusResponse)
async def get_status(job_id: UUID) -> JobStatusResponse:
    row = await fetch_job(job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Page Not Found")
    return JobStatusResponse(
        job_id=job_id,
        status= row["status"],
        created_at = datetime.fromisoformat(row["created_at"]),
        updated_at = datetime.fromisoformat(row["updated_at"]),
        processing_by = row["processing_by"],
        leas_until= (datetime.fromisoformat(row["lease_until"]) if row["lease_until"] else None),
    )

@router.post("/jobs/{job_id}/result", response_model=JobResultResponse)
async def get_result(job_id: UUID) -> JobResultResponse:
    row = await fetch_job(job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Page Not Found")
    status: str = row["status"]
    rc: Any = row["result_chars"]
    if status=="failed":
        return JSONResponse(
            status_code=409,
            content={
                "job_id":str(job_id),
                "status":"failed",
                "attempts": int(row["attempts"] or 0),
                "error":row["last_error"] or "unknown", 
            },
        )
    return JobResultResponse(job_id=job_id, status=status, characters=int(rc))

    

