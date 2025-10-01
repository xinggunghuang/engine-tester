from __future__ import annotations

from typing import List

from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import AnyHttpUrl, BaseModel

from .processor import ProcessingError, ProcessSummary, relay_requests, resolve_directory

app = FastAPI(title="Engine Tester", version="0.1.0")


class ProcessResponse(BaseModel):
    status: str
    processed: int
    responses: List[str]


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/enginepost", response_model=ProcessResponse)
async def process_directory(
    inputfolder: str = Query(..., description="Relative directory containing *_req.json files"),
    engineurl: AnyHttpUrl = Query(..., description="Upstream HTTP endpoint that processes the requests"),
) -> ProcessResponse:
    try:
        directory = resolve_directory(inputfolder)
    except ProcessingError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        summary: ProcessSummary = await run_in_threadpool(
            relay_requests, str(engineurl), directory
        )
    except ProcessingError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    response_paths = [
        processed_file.response_path.as_posix() for processed_file in summary.processed_files
    ]

    return ProcessResponse(status="ok", processed=summary.processed_count, responses=response_paths)
