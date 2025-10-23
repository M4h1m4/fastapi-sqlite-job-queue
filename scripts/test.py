from __future__ import annotations
import argparse
import asyncio
import json 
from datetime import datetime 
from pathlib import Path
from typing import Any 

import httpx 

def ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

async def create_job(client: httpx.AsyncClient, api: str, file_path: Path) -> str:
    files = {
        "file": (file_path.name, file_path.read_bytes(), "text/plain")
        # OR streaming: "file": (file_path.name, open(file_path, "rb"), "text/plain")
    }
    r = await client.post(f"{api}/jobs", files=files)   # <-- must be files=, field "file"
    print("POST /jobs ->", r.status_code, r.text)
    r.raise_for_status()
    return r.json()["job_id"]

async def poll_result(clinet: httpx.AsyncClient, api: str, job_id: str, poll_delay: float, poll_limit: int) -> dict[str, Any]:
    for _ in range(poll_limit):
        r = await client.get(f"{api}/jobs/{job_id}/result")
        if r.status_code in (200, 202, 409, 404):
            data: dict[str, Any]= r.json()
        else:
            data={"error":r.text}
        if r.status_code == 200:
            return {"status":"done", "chars":data["characters"]}
        if r.status_code == 409:
            return {"status":"failed", **data}
        if r.status_code == 404:
            return {"status": "not_found"}

        await asyncio.sleep(poll_delay)

async def main() -> None:
    parser = argparse.ArgumentParser(description="Test Text Job Service")
    parser.add_argument("--api", default="http://127.0.0.1:8000", help="Base API URL")
    parser.add_argument("--file", default="notes.txt", help="Path to a text file to upload")
    parser.add_argument("--count", type=int, default=3, help="How many jobs to create")
    parser.add_argument("--poll-delay", type=float, default=1.0, help="Seconds between polls")
    parser.add_argument("--poll-limit", type=int, default=40, help="Max polls per job")
    parser.add_argument("--log", default="client.log", help="Where to write logs")
    args = parser.parse_args()

    file_path: Path = Path(args.file).resolve()
    if not file_path.exists():
        raise SystemExit(f"File not found: {file_path}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        with open(args.log, "a", encoding="utf-8") as logf:
            logf.write(f"==== TEST START {ts()} ====\n")
            for i in range(1, args.count + 1):
                logf.write(f"{ts()} [create #{i}] POST /jobs file={file_path}\n")
                job_id: str = await create_job(client, args.api, file_path)
                logf.write(f"{ts()} [create #{i}] job_id={job_id}\n")
                result: dict[str, Any] = await poll_result(
                    client, args.api, job_id, args.poll_delay, args.poll_limit
                )
                logf.write(f"{ts()} [result #{i}] {json.dumps(result)}\n")
            logf.write(f"==== TEST END {ts()} ====\n")


if __name__ == "__main__":
    asyncio.run(main())