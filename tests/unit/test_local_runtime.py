from __future__ import annotations

import json
from pathlib import Path

import pytest

from threadsense.config import RuntimeConfig
from threadsense.errors import NetworkBoundaryError, SchemaBoundaryError
from threadsense.inference.local_runtime import (
    LocalRuntimeClient,
    extract_message_content,
    parse_structured_output,
    send_json_request,
    validate_chat_completion_response,
)
from threadsense.inference.prompts import build_analysis_summary_request
from threadsense.models.canonical import load_canonical_thread
from threadsense.pipeline.analyze import analyze_thread


def test_probe_payload_matches_fixture() -> None:
    fixture_path = Path("tests/fixtures/inference/chat_completion_request.json")
    expected = json.loads(fixture_path.read_text(encoding="utf-8"))
    client = LocalRuntimeClient(
        RuntimeConfig(
            enabled=True,
            base_url="http://127.0.0.1:8080",
            chat_path="/v1/chat/completions",
            model="local-model",
            timeout_seconds=10,
            repair_retries=1,
            json_mode=False,
        )
    )

    assert client.build_probe_payload() == expected


def test_validate_chat_completion_response_accepts_known_contract() -> None:
    fixture_path = Path("tests/fixtures/inference/chat_completion_response.json")
    response = json.loads(fixture_path.read_text(encoding="utf-8"))

    parsed = validate_chat_completion_response(response)

    assert parsed["id"] == "chatcmpl-local-123"
    assert parsed["model"] == "local-model"


def test_validate_chat_completion_response_rejects_invalid_object_type() -> None:
    with pytest.raises(SchemaBoundaryError):
        validate_chat_completion_response(
            {
                "id": "bad",
                "object": "list",
                "model": "local-model",
                "choices": [{"message": {"content": "READY"}, "finish_reason": "stop"}],
            }
        )


def test_parse_structured_output_rejects_non_json_content() -> None:
    with pytest.raises(SchemaBoundaryError):
        parse_structured_output("not-json")


def test_complete_repairs_invalid_json_response() -> None:
    canonical_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    thread = load_canonical_thread(canonical_path)
    analysis = analyze_thread(thread, canonical_path)
    invalid_response = json.loads(
        Path("tests/fixtures/inference/analysis_summary_invalid_response.json").read_text(
            encoding="utf-8"
        )
    )
    valid_response = json.loads(
        Path("tests/fixtures/inference/analysis_summary_response.json").read_text(encoding="utf-8")
    )
    client = LocalRuntimeClient(
        RuntimeConfig(
            enabled=True,
            base_url="http://127.0.0.1:8080",
            chat_path="/v1/chat/completions",
            model="local-model",
            timeout_seconds=10,
            repair_retries=1,
            json_mode=False,
        )
    )
    request = build_analysis_summary_request(
        analysis=analysis,
        required=True,
        repair_retries=1,
    )
    responses = iter([(200, invalid_response), (200, valid_response)])

    def fake_opener(
        url: str,
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> tuple[int, dict[str, object]]:
        assert url == "http://127.0.0.1:8080/v1/chat/completions"
        assert payload["model"] == "local-model"
        assert timeout_seconds == 10
        return next(responses)

    response = client.complete(request, opener=fake_opener)

    assert response.used_fallback is False
    assert response.output["headline"] == "Performance and docs dominate the thread"


def test_extract_message_content_reads_chat_message() -> None:
    fixture_path = Path("tests/fixtures/inference/chat_completion_response.json")
    response = json.loads(fixture_path.read_text(encoding="utf-8"))
    parsed = validate_chat_completion_response(response)

    assert extract_message_content(parsed) == "READY"


def test_send_json_request_translates_timeout_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx

    def raise_timeout(*args: object, **kwargs: object) -> object:
        raise httpx.ReadTimeout("timed out")

    monkeypatch.setattr(httpx.Client, "post", raise_timeout)

    with pytest.raises(NetworkBoundaryError) as raised:
        send_json_request(
            "http://127.0.0.1:8080/v1/chat/completions",
            {"model": "local-model"},
            1.0,
        )

    assert raised.value.code == "network_error"
    assert raised.value.details["timeout_seconds"] == 1.0
