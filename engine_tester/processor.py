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
    execution_results: List["ExecutionResult"]

    @property
    def processed_count(self) -> int:
        return len(self.processed_files)

    @property
    def succeeded_count(self) -> int:
        return sum(1 for result in self.execution_results if result.succeeded)

    @property
    def failed_count(self) -> int:
        return sum(1 for result in self.execution_results if not result.succeeded)


@dataclass(slots=True)
class ExecutionResult:
    """Per-request execution outcome, including success and failure details."""

    request_path: Path
    post_url: str
    succeeded: bool
    response_path: Optional[Path] = None
    message: Optional[str] = None


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
    (re.compile(r"^03.*_req\.json$"), "chkkeiyakuOver"),
    (re.compile(r"^05.*_req\.json$"), "jissekicalc"),
    (re.compile(r"^06.*_req\.json$"), "adjustget"),
    (re.compile(r"^08.*_req\.json$"), "jissekiif"),
    (re.compile(r"^09.*_req\.json$"), "jissekirep"),
    (re.compile(r"^11.*_req\.json$"), "kekkarep"),
    (re.compile(r"^15_1.*_req\.json$"), "meisaiif"),
    # (re.compile(r"^15_2.*_req\.json$"), "meisairep"),
    (re.compile(r"^15_2.*_req\.json$"), "meisaiif"),
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
    execution_results: List[ExecutionResult] = []

    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=httpx.Timeout(timeout), follow_redirects=True)

    try:
        for request_path in iter_request_files(directory):
            post_url = resolve_post_url(target_url, request_path)
            response_path = build_response_path(request_path)
            wrote_response_file = False
            try:
                payload = load_request_payload(request_path)
                response = client.post(post_url, json=payload)

                try:
                    response_payload = response.json()
                    is_valid_json = True
                except (json.JSONDecodeError, ValueError):
                    # Preserve the exact downstream body when it isn't JSON.
                    response_payload = {"raw_response": response.text}
                    is_valid_json = False

                save_response_payload(response_path, response_payload)
                wrote_response_file = True

                processed.append(ProcessedFile(request_path=request_path, response_path=response_path))
                if response.status_code >= 400:
                    execution_results.append(
                        ExecutionResult(
                            request_path=request_path,
                            post_url=post_url,
                            succeeded=False,
                            response_path=response_path,
                            message=f"Downstream server responded with status {response.status_code}",
                        )
                    )
                elif not is_valid_json:
                    execution_results.append(
                        ExecutionResult(
                            request_path=request_path,
                            post_url=post_url,
                            succeeded=False,
                            response_path=response_path,
                            message="Downstream response is not valid JSON",
                        )
                    )
                else:
                    execution_results.append(
                        ExecutionResult(
                            request_path=request_path,
                            post_url=post_url,
                            succeeded=True,
                            response_path=response_path,
                            message="Processed successfully",
                        )
                    )
            except (ProcessingError, httpx.HTTPError) as exc:
                if not wrote_response_file:
                    save_response_payload(
                        response_path,
                        {
                            "status": "failed",
                            "request": request_path.as_posix(),
                            "message": str(exc),
                        },
                    )
                    processed.append(ProcessedFile(request_path=request_path, response_path=response_path))
                execution_results.append(
                    ExecutionResult(
                        request_path=request_path,
                        post_url=post_url,
                        succeeded=False,
                        response_path=response_path,
                        message=str(exc),
                    )
                )
    finally:
        if owns_client:
            client.close()

    return ProcessSummary(
        target_url=target_url,
        base_directory=directory,
        processed_files=processed,
        execution_results=execution_results,
    )
