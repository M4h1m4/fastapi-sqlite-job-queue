from __future__ import annotations

import logging
from typing import Any
from fastapi import FastAPI 

from .api import router
from .db import AioSqlitePool, init_db, set_pool, DB_PATH
from .worker import start_workers, start_reaper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("text-jobs") 

def create_app()-> FastAPI:
    app: FastAPI = FastAPI(title="Text Job Service (async + pool + leases)", version="2.0.0")
    app.include_router(router)
    
    @app.on_event("startup")
    async def _startup() -> None:
        pool = AioSqlitePool(db_path=DB_PATH, size=5)
        await pool.init()
        log.info("pool init ok")
        set_pool(pool)

        await init_db()
        log.info("init_db ok")

        await start_workers(n=3)
        log.info("workers started")

        await start_reaper()
        log.info("reaper started")

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        pass

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"status":"ok"}
    return app 

app: FastAPI = create_app()