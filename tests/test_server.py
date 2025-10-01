from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from engine_tester import processor
from engine_tester import server


@pytest.fixture()
def client() -> TestClient:
    return TestClient(server.app)


def test_process_directory_uses_query_parameters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    target_dir = tmp_path / "入力"
    target_dir.mkdir(parents=True)

    def fake_relay(target_url: str, directory: Path, **_: object) -> processor.ProcessSummary:
        assert target_url == "http://example.com/api"
        assert directory == target_dir

        processed_file = processor.ProcessedFile(
            request_path=directory / "依頼_req.json",
            response_path=directory / "依頼_res.json",
        )
        return processor.ProcessSummary(
            target_url=target_url,
            base_directory=directory,
            processed_files=[processed_file],
        )

    monkeypatch.setattr(server, "relay_requests", fake_relay)

    response = client.post(
        "/api/enginepost",
        params={"inputfolder": str(target_dir), "engineurl": "http://example.com/api"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "status": "ok",
        "processed": 1,
        "responses": [(target_dir / "依頼_res.json").as_posix()],
    }
