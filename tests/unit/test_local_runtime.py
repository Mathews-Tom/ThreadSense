from __future__ import annotations

import json
from pathlib import Path

import pytest

from threadsense.config import RuntimeConfig
from threadsense.errors import SchemaBoundaryError
from threadsense.inference.local_runtime import (
    LocalRuntimeClient,
    validate_chat_completion_response,
)


def test_probe_payload_matches_fixture() -> None:
    fixture_path = Path("tests/fixtures/inference/chat_completion_request.json")
    expected = json.loads(fixture_path.read_text(encoding="utf-8"))
    client = LocalRuntimeClient(
        RuntimeConfig(
            base_url="http://127.0.0.1:8080",
            chat_path="/v1/chat/completions",
            model="local-model",
            timeout_seconds=10,
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
