from __future__ import annotations

import logging
from fastapi import FastAPI

from .api import router
from .db import init_db
from .worker import start_workers

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def create_app() -> FastAPI:
    app = FastAPI(title="Text Job Service (modular, pure SQL)", version="1.0.0")
    app.include_router(router)

    @app.on_event("startup")
    def _startup():
        init_db()
        start_workers(n=1)  # bump to 2..N if you want parallel processing

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app

# Expose 'app' for 'uvicorn main:app'
app = create_app()
