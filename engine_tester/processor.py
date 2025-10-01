from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Iterable, List, Optional

import httpx


@dataclass(slots=True)
class ProcessedFile:
    """Details about a processed request file."""

    request_path: Path
    response_path: Path


@dataclass(slots=True)
class ProcessSummary:
    """Summary of a processing run."""

    target_url: str
    base_directory: Path
    processed_files: List[ProcessedFile]

    @property
    def processed_count(self) -> int:
        return len(self.processed_files)


class ProcessingError(RuntimeError):
    """Raised when processing fails."""


def resolve_directory(directory: str | Path) -> Path:
    """Resolve a client-provided directory path.

    Raises ``ProcessingError`` if the directory doesn't exist.
    """

    candidate = Path(directory).expanduser().resolve()

    if not candidate.is_dir():
        raise ProcessingError(f"Directory not found: {candidate}")

    return candidate


def iter_request_files(root: Path) -> Iterable[Path]:
    """Yield request files (_req.json) under ``root`` and its subdirectories."""

    yield from sorted(root.rglob("*_req.json"))


def build_response_path(request_path: Path) -> Path:
    stem = request_path.stem
    if not stem.endswith("_req"):
        raise ProcessingError(f"File name does not end with '_req.json': {request_path}")
    prefix = stem[:-4]
    return request_path.with_name(f"{prefix}_res.json")


def load_request_payload(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as exc:
        raise ProcessingError(f"Invalid JSON in {path}") from exc


def save_response_payload(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=4, sort_keys=False)
        fh.write("\n")


def relay_requests(
    target_url: str,
    directory: Path,
    *,
    timeout: float = 30.0,
    client: Optional[httpx.Client] = None,
) -> ProcessSummary:
    processed: List[ProcessedFile] = []

    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=httpx.Timeout(timeout), follow_redirects=True)

    try:
        for request_path in iter_request_files(directory):
            payload = load_request_payload(request_path)
            response = client.post(target_url, json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise ProcessingError(
                    f"Downstream server responded with status {exc.response.status_code}"
                ) from exc

            try:
                response_payload = response.json()
            except (json.JSONDecodeError, ValueError) as exc:
                raise ProcessingError("Downstream response is not valid JSON") from exc

            response_path = build_response_path(request_path)
            save_response_payload(response_path, response_payload)

            processed.append(ProcessedFile(request_path=request_path, response_path=response_path))
    finally:
        if owns_client:
            client.close()

    return ProcessSummary(target_url=target_url, base_directory=directory, processed_files=processed)
