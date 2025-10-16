from __future__ import annotations

import threading
import sqlite3
import time
from dataclasses import dataclass 
from queue import Queue 
from datetime import datetime
from typing import Optional, List
from uuid import UUID, uuid4
import logging
from pathlib import Path

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlmodel import SQLModel, Field, create_engine, Session, select

DB_PATH = Path(__file__).with_name("jobs.db")
#---logging----
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("text-jobs")


STATUSES = ("pending", "started", "processing", "done")
MAX_UPLOAD_BYTES = 1_000_000 #~1 MB


#do not use orm just use native sql 
class Job(SQLModel, table=True):
    __tablename__="jobs"
    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)
    status: str = Field(default="pending", index=True, max_length=20)
    text: Optional[str]=Field(default=None)
    result_chars: Optional[int] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, index=True)
# default is for constants, by default the value is assigned to it 
#default_factory means that call this function on initialization. This is for dynamic or mutable values
#for example dynamic: uuid, datetime.utcnow , mutable: list, dict
#UUID means Universally unique identifier 


#Engine and bootstrap
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
#creates engine and returns DB engine 
#Each request (and the worker) should still use its own Session(engine); don’t share Session objects across threads.

def init_db() -> None:
    SQLModel.metadata.create_all(engine)



job_queue: "Queue[UUID]" = Queue() # A fifo queue to that holds job_ids to be processed,
#in memory fifo queue
#"Queue[UUID]" this actually says that the queue items are of type UUID and it helps to check the parameter being passed

def _update_status(job_id: UUID, new_status: str, result_chars:Optional[int]=None) -> None:
    if new_status not in STATUSES:
        raise ValueError(f"Invalid Status:{new_status}")
    with Session(engine) as session:
        job = session.exec(select(Job).where(Job.id == job_id)).first()
        # if not job:
        #     return 
        job.status = new_status
        if result_chars is not None:
            job.result_chars = result_chars
        job.updated_at = datetime.utcnow()
        session.add(job)
        session.commit()
        session.refresh(job)
    log.info("job %s -> %s", job_id, new_status)
#flow of the above function validate status → 2) open session → 3) fetch job → 4) mutate fields → 5) commit.

def worker_loop() -> None:
    log.info("worker started")
    while True:
        job_id = job_queue.get()
        if random.randint(1,1000) < 300:
            raise Exception("Phase 1 failure")
        try:
            _update_status(job_id, "started")
            time.sleep(0.5)
            _update_status(job_id, "processing")
            with Session(engine) as session:
                job = session.exec(select(Job).where(Job.id == job_id)).first()
                # if not job:
                #     continue
                text = job.text or ""
            time.sleep(2.0)
            chars = len(text)
            _update_status(job_id, "done", result_chars=chars)
        finally:
            job_queue.task_done() #look at this


#---API Schemas------
class CreateJobResponse(BaseModel):
    job_id: UUID
    status: str

class JobStatusResponse(BaseModel):
    job_id: UUID
    status: str
    created_at: datetime
    updated_at: datetime

class JobResultResponse(BaseModel):
    job_id: UUID
    status: str
    characters: int

class JobView(BaseModel):
    job_id: UUID
    status: str
    created_at: datetime
    updated_at: datetime
    result_chars: Optional[int]

#FastAPI app
app = FastAPI(title="Text Job Service", version="1.0.0")

@app.on_event("startup")
def on_startup():
    init_db()
    t = threading.Thread(target=worker_loop, name="job-worker", daemon=True)
    t.start()

@app.get("/healthz")
def healthz():
    return {"status":"ok"}
#create_all(engine) tells SQLAlchemy to create any tables that don’t already exist for every model defined with table=True (e.g., Job).


#response_model=CreateJobResponse makes FastAPI validate/shape the outgoing JSON.
@app.post("/jobs", response_model=CreateJobResponse, status_code=201)
async def create_job(file: UploadFile = File(...)):
    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code = 413, detail="File too large max(1 MB)")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded text")
    with Session(engine) as session:
        job = Job(status="pending",text=text)
        session.add(job)
        session.commit()
        session.refresh(job)
        job_id = job.id 

    job_queue.put(job_id)
    log.info("enqueued job %s", job_id)
    return CreateJobResponse(job_id=job.id, status="pending")

@app.get("/jobs/{job_id}/status", response_model=JobStatusResponse)
def get_status(job_id: UUID):
    with Session(engine) as session:
        job = session.exec(select(Job).where(Job.id == job_id)).first()
        if not job:
            raise HTTPException(status_code = 404, detail="Job Not found")
        return JobStatusResponse(
            job_id=job.id, status=job.status, created_at=job.created_at, updated_at=job.updated_at
        )

@app.get("/jobs/{job_id}/result", response_model= JobResultResponse)
def get_result(job_id: UUID):
    with Session(engine) as session:
        job = session.exec(select(Job).where(Job.id == job_id)).first()
        if not job:
            raise HTTPException(status_code = 404, detail="Job Not found")
        if job.status != "done" or job.result_chars is None:
            return JSONResponse(
                status_code=202,
                content={"job_id": str(job.id), "status": job.status, "message": "Result not ready"},
            )
        return JobResultResponse(job_id=job.id, status=job.status, characters=job.result_chars)

@app.get("/jobs/{job_id}", response_model=JobView)
def get_job(job_id: UUID):
    with Session(engine) as session:
        job = session.exec(select(Job).where(Job.id == job_id)).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return JobView(
            job_id=job.id,
            status=job.status,
            created_at=job.created_at,
            updated_at=job.updated_at,
            result_chars=job.result_chars,
        )

@app.get("/jobs", response_model=List[JobView])
def list_jobs(limit: int = 20):
    with Session(engine) as session:
        rows = session.exec(select(Job).order_by(Job.created_at.desc()).limit(limit)).all()
        return [
            JobView(
                job_id=j.id,
                status=j.status,
                created_at=j.created_at,
                updated_at=j.updated_at,
                result_chars=j.result_chars,
            )
            for j in rows
        ]