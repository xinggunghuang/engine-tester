from __future__ import annotations

import json
from pathlib import Path

import httpx
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


class MixedFakeResponse:
    def __init__(self, payload: dict | None = None, *, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class MixedFakeClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def post(self, url: str, json: dict):  # type: ignore[override]
        self.calls.append(json.get("id", "unknown"))
        if json.get("id") == "bad":
            return MixedFakeResponse(payload={"error": "downstream failed"}, status_code=500)
        return MixedFakeResponse(payload={"ok": True})

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
    assert summary.succeeded_count == 1
    assert summary.failed_count == 0
    assert client.calls[0]["url"] == "http://example.com/api"
    response_file = sample_structure / "レベル" / "深い" / "依頼_res.json"
    assert response_file.exists()
    saved_text = response_file.read_text(encoding="utf-8")
    assert json.loads(saved_text) == response_body
    assert saved_text.endswith("\n")
    assert summary.execution_results[0].succeeded is True
    assert summary.execution_results[0].response_path == response_file


def test_relay_requests_adjusts_idou_routes(tmp_path: Path) -> None:
    directory = tmp_path / "requests"
    directory.mkdir()
    request_file = directory / "03sample_req.json"
    request_file.write_text(json.dumps({"foo": "bar"}), encoding="utf-8")

    response_body = {"result": "ok"}
    client = FakeClient(response_body)

    summary = processor.relay_requests(
        target_url="http://example.com/idou/service",
        directory=directory,
        client=client,
    )

    assert summary.processed_count == 1
    assert summary.succeeded_count == 1
    assert summary.failed_count == 0
    assert client.calls[0]["url"] == "http://example.com/idou/service/chkkeiyakuOver"
    saved = (directory / "03sample_res.json").read_text(encoding="utf-8")
    assert json.loads(saved)["result"] == "ok"


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
    ("05alpha_req.json", "jissekicalc"),
    ("06beta_req.json", "adjustget"),
    ("09gamma_req.json", "jissekirep"),
    ("11delta_req.json", "kekkarep"),
    ("15_1_epsilon_req.json", "meisaiif"),
    ("15_1_req.json", "meisaiif"),
    ("15_2_zeta_req.json", "meisairep"),
    ("15_2_req.json", "meisairep"),
    ],
)
def test_resolve_post_url_suffixes(tmp_path: Path, filename: str, expected: str) -> None:
    path = tmp_path / filename
    path.write_text("{}", encoding="utf-8")

    result = processor.resolve_post_url("http://example.com/idou/service", path)
    assert result == f"http://example.com/idou/service/{expected}"


def test_resolve_post_url_non_matching_suffix(tmp_path: Path) -> None:
    path = tmp_path / "13omega_req.json"
    path.write_text("{}", encoding="utf-8")

    result = processor.resolve_post_url("http://example.com/idou/service", path)
    assert result == "http://example.com/idou/service"


@pytest.mark.parametrize(
    ("request_name", "expected_name"),
    [
        ("sample_req.json", "sample_res.json"),
        ("sample_req3.json", "sample_res3.json"),
        ("sample_req03.json", "sample_res03.json"),
        ("sample_req05.json", "sample_res05.json"),
        ("sample_req06.json", "sample_res06.json"),
        ("sample_req08.json", "sample_res08.json"),
        ("sample_req09.json", "sample_res09.json"),
        ("sample_req11.json", "sample_res11.json"),
        ("sample_req15.json", "sample_res15.json"),
    ],
)
def test_build_response_path_supported_patterns(
    tmp_path: Path,
    request_name: str,
    expected_name: str,
) -> None:
    request_path = tmp_path / request_name
    assert processor.build_response_path(request_path) == (tmp_path / expected_name)


def test_resolve_directory_accepts_absolute(tmp_path: Path) -> None:
    resolved = processor.resolve_directory(tmp_path)
    assert resolved == tmp_path.resolve()


def test_resolve_directory_missing_path(tmp_path: Path) -> None:
    missing = tmp_path / "absent"
    with pytest.raises(processor.ProcessingError):
        processor.resolve_directory(missing)


def test_relay_requests_reports_success_and_failure(tmp_path: Path) -> None:
    directory = tmp_path / "requests"
    directory.mkdir()
    (directory / "01_ok_req.json").write_text(json.dumps({"id": "ok"}), encoding="utf-8")
    (directory / "02_bad_req.json").write_text(json.dumps({"id": "bad"}), encoding="utf-8")

    summary = processor.relay_requests(
        target_url="http://example.com/api",
        directory=directory,
        client=MixedFakeClient(),
    )

    assert summary.succeeded_count == 1
    assert summary.failed_count == 1
    assert len(summary.execution_results) == 2
    assert summary.processed_count == 2
    assert any(result.succeeded for result in summary.execution_results)
    assert any(not result.succeeded for result in summary.execution_results)

    failed_response = directory / "02_bad_res.json"
    assert failed_response.exists()
    failed_payload = json.loads(failed_response.read_text(encoding="utf-8"))
    assert failed_payload == {"error": "downstream failed"}
