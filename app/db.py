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


#---logging----
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("text-jobs")

DB_PATH = (Path(__file__).resolve().parent.parent./"jobs.db")
STATUSES = ("pending", "started", "processing", "done", "failed")

def get_conn() -> sqlite3.Connection: #a connection is created on every call
    conn = sqlite3.connect(DB_PATH, isolation_level=None)  # autocommit mode 
    conn.row_factory = sqlite3.Row #helps you access each column by name like rows["status"]
    conn.execute("PRAGMA busy_timeout=3000") #if DB is locked by another write it waits for 3 seconds to error out 
    return conn

def init_db() ->None:
    with get_conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL;") #this is write ahead logging mode
        conn.execute(
            """
            CREATE TABEL IF NOT EXISTS jobs(
            id  CHAR(32) PRIMARY KEY,   --UUID hex (no hyphens)
            status VARCHAR(20) NOTNULL,
            text TEXT,
            results_chars INTEGER, 
            created_at TEXT NOTNULL,    -- ISO8601 (UTC)
            updated_at TEXT NOTNULL
            );

            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS ix_jobs_status     ON jobs(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_jobs_created_at ON jobs(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_jobs_updated_at ON jobs(updated_at)")


        # ---- add missing columns for retries/failures (safe on existing DBs) ----
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        if "attempts" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        if "last_error" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN last_error TEXT")

def _utcnow_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()

def insert_job(text:str) -> UUID:
    job_uuid = uuid4()
    now = _utcnow_iso()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO jobs (id, status, text, result_chars, created_at, updated_at) "
            "VALUES (?, 'pending', ?, NULL, ?, ?)", 
            (job_uuid.hex, text, now, now), 
        )
    return job_uuid

def fetch_job(job_id: UUID) -> Optional[sqlite3.row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id.hex,)).fetchone()

def fetch_job_text(job_id: UUID) -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute("SELECT text FROM jobs WHERE id=?",(job_id.hex,)).fetchone()
        return None if row is None else (row["text"] or "")

def update_status(job_id: UUID, new_status:str, result_chars: Optional[int] = None) -> None:
    if new_status not in STATUSES:
        raise ValueError("Invalid status")
    now = _utcnow_iso()
    with get_conn() as conn:
        if results_chars is None:
            conn.execute(
                "UPDATE jobs SET status =?, updated_at=?, WHERE id =?",
                (new_status, now, job_id.hex),
            )
        else:
            conn.execute(
                "UPDATE jobs SET status=?, results_chars=?, updated_at=?, WHERE id=?".
                (new_status, int(result_chars), now, job_id.hex),
            )
    log.info("job %s-> %s%s", job_id, new_status, 
            f"(result_chars={result_chars})" if result_chars is not None else ""
    )

def get_attempts(job_id: UUID) -> int:
    with get_conn() as conn:
        row = conn.execute("SELECT attempts FROM jobs WHERE id=?", (job_id.hex,)).fetchone()
        return 0 if row is None or row["attempts"] is None else int(row["attempts"])

def record_retry(jon_id: UUID, error_text:str) -> None:
    #Increment attempts, store last_error, set back the status to pending
    now = _utcnow_iso()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET attempts  = COALESCE(attempts, 0) + 1,
                last_error =?,
                status = 'pending',
                updated_at = ?, 
            WHERE id =?
            """,
            (error_text[:1000], now, job_id.hex), 
        )

def record_failed(job_id: UUID, error_text:str) -> None:
    #terminal failure at the current attempts count
    now = _utcnow_iso()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE jobs
                SET last_error=?,
                status='failed',
                updated_at=?
            WHERE id=?
            """,
            (error_text[:2000], now, job_id.hex),
        )