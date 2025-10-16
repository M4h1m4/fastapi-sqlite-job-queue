from __future__ import annotations
from app.app import app  # uvicorn will import this

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", reload=True, port=8000)
