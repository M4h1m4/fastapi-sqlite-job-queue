from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path 
from typing import AsyncIterator, List, Optional, Sequence
from uuid import UUID, uuid4 

import aiosqlite
from aiosqlite import Row

DB_PATH = (Path(__file__).resolve().parent/"jobs.db")

STATUSES: set[str]= {"pending", "started", "processing", "done", "failed"}

def _utcnow_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()

def _plus_seconds_iso(sec:int) -> str:
    return (datetime.utcnow() + timedelta(seconds=sec)).replace(microsecond=0).isoformat()

@dataclass
class AioSqlitePool:
    db_path: Path 
    size: int
    _queue: asyncio.Queue[aiosqlite.Connection]=asyncio.Queue()
    _all :list[aiosqlite.Connection] = None
    _inited: bool = False
    _init_lock : asyncio.Lock = asyncio.Lock() #ensures idempotent initialization

    async def init(self) -> None:
        if self._inited:
            return 
        async with self._init_lock:
            if self._inited:
                return 
            self._all=[]
            for _ in range(self.size):
                conn = await aiosqlite.connect(str(self.db_path), isolation_level = None) #autocommit mode on
                conn.row_factory = aiosqlite.Row  #behave like dicts row["col"]
                await conn.execute("PRAGMA journal_mode=WAL;")
                await conn.execute("PRAGMA busy_timeout=3000;")
                await self._queue.put(conn)
                self._all.append(conn) #rach connection is pushed into the pool 
                #tracked using all
            self._inited = True

    @asynccontextmanager #this is to make sure the connection is returned to the pool 
    async def acquire(self) -> AsyncIterator[aioqlite.Connection]:
        conn = await self._queue.get()
        try:
            yield conn
        finally:
            await self._queue.put(conn)
    
    async def close(self) -> None: #closes all connections if fastapi calls shutdown hook
        if not self_inited:
            return 
        for conn in self._all:
            await conn.close()


_pool: Optional[AioSqlitePool]=None

#Helper functions to fetch the pool instances
def set_pool(pool: AioSqlitePool) -> None:
    global _pool
    _pool = pool

def require_pool() -> AioSqlitePool:
    assert _pool is not None, "DB pool not initalized"
    return _pool

async def init_db() -> None:
    pool = require_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs(
            id      CHAR(32)    PRIMARY KEY,
            status  VARAHCAR(20)    NOT NULL,
            text    TEXT,
            result_chars INTEGER,
            attempts        INTEGER NOT NULL DEFAULT 0,
            last_error      TEXT,
            processing_by   TEXT,
            lease_until     TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
            );
            """
        )
        await conn.execute("CREATE INDEX IF NOT EXISTS ix_jobs_status       ON jobs(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS ix_jobs_updated_at   ON jobs(updated_at)")
        await conn.execute("CREATE INDEX IF NOT EXISTS ix_jobs_status_lease ON jobs(status, lease_until)")

        # Startup recovery: put abandoned in-flight jobs back to pending
        now: str = _utcnow_iso()
        await conn.execute(
            """
            UPDATE jobs
               SET status='pending', lease_until=NULL, processing_by=NULL, updated_at=?
             WHERE status IN ('started','processing')
               AND (lease_until IS NULL OR lease_until < ?)
            """,
            (now, now),
        )
        await conn.commit()

async def insert_job(text: str) -> UUID:
    job_id: UUID = uuid4()
    now: str = _utcnow_iso()
    pool = require_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, status, text, result_chars, attempts, last_error, processing_by, lease_until, created_at, updated_at) "
            "VALUES (?, 'pending', ?, NULL, 0, NULL, NULL, NULL, ?, ?)",
            (job_id.hex, text, now, now),
        )
        await conn.commit()
    return job_id

async def fetch_job(job_id: UUID) -> Optional[Row]:
    pool = require_pool()
    async with pool.acquire() as conn:
        cur = await conn.execute("SELECT * from job WHERE id=?", (job_id.hex))
        row: Optional[Row] = await cur.fetchone()
        await cur.close()
        return row 


async def fetch_job_text(job_id: UUID) -> Optional[str]:
    pool = require_pool()
    async with pool.acquire() as conn:
        cur = await conn.execute("SELECT text FROM jobs WHERE id=?", (job_id.hex))
        row: Optional[Row] = await cur.fetchone()
        await cur.close()
        return row


async def update_status(job_id: UUID, new_status: str, result_chars: Optional[int]=None) -> None:
    if new_status not in STATUSES:
        raise ValueERROR(f"Invalid status:{new_status}")
    now: str = _utcnow_iso()
    pool = require_pool
    async with pool.acquire() as conn:
        if result_chars is None:
            await conn.execute(
                "UPDATE jobs SET status=?, updated_at=?, WHERE id=?",
                (new_status, now, job_id.hex),
            )
        else:
            await conn.execute(
                "UPDATE jobs SET status=?, results_char=?, updated_at=?, WHERE id=?",
                (new_status, int(result_chars), now, job_id.hex),
            )
        await conn.commit()

##Retry book keeping

async def get_attempts(job_id: UUID) -> int:
    pool = require_pool()
    async with pool.acquire() as conn:
        cur = await conn.execute("SELECT attempts FROM jobs WHERE id = ?", (job_id.hex,))
        row: Optional[Row] = await cur.fetchone()
        await cut.close()
        return 0 if row is None or row["attmepts"] is None else int(row["attempts"])

async def record_retry(job_id: UUID, error_text: str) -> None:
    now: str = _utcnow_iso()
    pool = require_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE jobs "
            "SET attempts   = COALESCE(attempts, 0) + 1, "
            "    last_error = ?, "
            "    status     = 'pending', "
            "    lease_until= NULL, "
            "    processing_by = NULL, "
            "    updated_at = ? "
            "WHERE id = ?",
            (error_text[:1000],now, job_id.hex),
        )
        await conn.commit()

async def record_failed(job_id: UUID, error_text:str) -> None:
    now: str = _utcnow_iso()
    pool = require_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs
                SET last_error =?,
                    status='failed',
                    lease_until = NULL,
                    processing_by = NULL,
                    updated_at=?,
            WHERE   id =?
            """,
            (error_text[:2000],now, job_id.hex)
        )
        await conn.commit()

async def set_lease_started(job_id: UUID, processing_by: str, lease_seconds: int) -> None:
    now: str = _utcnow_iso()
    until: str = _plus_seconds_iso(lease_seconds)
    pool =require_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs 
                SET status ='started',
                    processing_by=?,
                    lease_until=?,
                    updated_at=?
            WHERE id=?
            """,
            (processing_by, until, now, job_id.hex)
        )
        await conn.commit()

async def extend_lease(job_id: UUID, lease_seonds: int) -> None:
    now: str = _utcnow_iso()
    until: str= _plus_seconds_iso()
    pool = require_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET lease_until=?, updated_at=?, WHERE id=?",
            (until, now, job_id.hex),
        )
        await conn.commit()

async def clear_lease(job_id: UUID) -> None:
    pool = require_pool()
    await conn.execute(
            "UPDATE jobs SET lease_until=NULL, processing_by=NULL, updated_at=? WHERE id=?",
            (_utcnow_iso(), job_id.hex),
        )
    await conn.commit()


#----------------Reaper Queries----------

async def reap_expired_ids(now_iso: str) -> list[str]:
    pool = require_pool()
    async with pool.acquire() as conn:
        cur = await conn.execute(
            """
            SELECT id FROM jobs
             WHERE status IN ('started','processing')
               AND lease_until IS NOT NULL
               AND lease_until < ?
            """,
            (now_iso,),
        )
        rows: Sequence[Row] = await cur.fetchall()
        await cur.close()
    return [str(row["id"]) for row in rows]


async def reset_to_pending(job_hex: str) -> None:
    pool = require_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs
               SET status='pending',
                   lease_until=NULL,
                   processing_by=NULL,
                   updated_at=?
             WHERE id=?
            """,
            (_utcnow_iso(), job_hex),
        )
        await conn.commit()

