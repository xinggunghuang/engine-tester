from __future__ import annotations

import json
from pathlib import Path

import pytest

from engine_tester import processor


class FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class FakeClient:
    def __init__(self, response_payload: dict):
        self.response_payload = response_payload
        self.calls: list[dict] = []

    def post(self, url: str, json: dict):  # type: ignore[override]
        self.calls.append({"url": url, "json": json})
        return FakeResponse(self.response_payload)

    def close(self) -> None:  # pragma: no cover - nothing to close
        pass


@pytest.fixture()
def sample_structure(tmp_path: Path) -> Path:
    base = tmp_path / "データ"
    nested = base / "レベル" / "深い"
    nested.mkdir(parents=True)

    payload = {"message": "hello", "value": 42}
    (nested / "依頼_req.json").write_text(json.dumps(payload), encoding="utf-8")

    return base


def test_relay_requests_creates_response_files(sample_structure: Path) -> None:
    response_body = {"result": "ok"}
    client = FakeClient(response_body)
    summary = processor.relay_requests(
        target_url="http://example.com/api",
        directory=sample_structure,
        client=client,
    )

    assert summary.processed_count == 1
    response_file = sample_structure / "レベル" / "深い" / "依頼_res.json"
    assert response_file.exists()
    saved_text = response_file.read_text(encoding="utf-8")
    assert json.loads(saved_text) == response_body
    assert saved_text.endswith("\n")


def test_resolve_directory_accepts_absolute(tmp_path: Path) -> None:
    resolved = processor.resolve_directory(tmp_path)
    assert resolved == tmp_path.resolve()


def test_resolve_directory_missing_path(tmp_path: Path) -> None:
    missing = tmp_path / "absent"
    with pytest.raises(processor.ProcessingError):
        processor.resolve_directory(missing)
