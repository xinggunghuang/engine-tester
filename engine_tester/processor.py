from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import re
from typing import Iterable, List, Optional, Set
from urllib.parse import urlsplit, urlunsplit

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
    """Yield request files (_req.json or _req3.json) under ``root`` and its subdirectories."""

    candidates: Set[Path] = set(root.rglob("*_req.json"))
    candidates.update(root.rglob("*_req3.json"))
    yield from sorted(candidates)


_IDOU_ROUTE_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^03(?:_.*)?_req\.json$"), "chkkeiyakuOver"),
    (re.compile(r"^05(?:_.*)?_req\.json$"), "jissekicalc"),
    (re.compile(r"^06(?:_.*)?_req\.json$"), "adjustget"),
    (re.compile(r"^08(?:_.*)?_req\.json$"), "jissekiif"),
    (re.compile(r"^09(?:_.*)?_req\.json$"), "jissekirep"),
    (re.compile(r"^11(?:_.*)?_req\.json$"), "kekkarep"),
    (re.compile(r"^15_1(?:_.*)?_req\.json$"), "meisaiif"),
    # (re.compile(r"^15_2(?:_.*)?_req\.json$"), "meisairep"),
    (re.compile(r"^15_2(?:_.*)?_req\.json$"), "meisaiif"),
]


def resolve_post_url(base_url: str, request_path: Path) -> str:
    """Determine the downstream POST URL for a given request file."""

    parts = urlsplit(base_url)
    if "/idou/" not in parts.path:
        return base_url

    filename = request_path.name
    for pattern, suffix in _IDOU_ROUTE_RULES:
        if pattern.match(filename):
            new_path = f"{parts.path.rstrip('/')}/{suffix}"
            return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))

    return base_url


def build_response_path(request_path: Path) -> Path:
    stem = request_path.stem
    if stem.endswith("_req"):
        prefix = stem[:-4]
        return request_path.with_name(f"{prefix}_res.json")
    if stem.endswith("_req3"):
        prefix = stem[:-5]
        return request_path.with_name(f"{prefix}_res3.json")
    raise ProcessingError(f"File name does not end with '_req.json' or '_req3.json': {request_path}")


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
            post_url = resolve_post_url(target_url, request_path)
            response = client.post(post_url, json=payload)
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
