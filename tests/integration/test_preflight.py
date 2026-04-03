from __future__ import annotations

import json

import pytest

from threadsense.cli import main
from threadsense.inference.local_runtime import RuntimeProbeResult


def test_preflight_reports_ready_when_runtime_probe_succeeds(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_send_json_request(
        url: str,
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> tuple[int, dict[str, object]]:
        assert url == "http://127.0.0.1:8080/v1/chat/completions"
        assert payload["model"] == "local-model"
        assert timeout_seconds == 30.0
        return (
            200,
            {
                "id": "chatcmpl-local-123",
                "object": "chat.completion",
                "model": "local-model",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "READY"},
                        "finish_reason": "stop",
                    }
                ],
            },
        )

    monkeypatch.setattr(
        "threadsense.inference.local_runtime.send_json_request",
        fake_send_json_request,
    )

    exit_code = main(["preflight"])
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 0
    assert payload["status"] == "ready"
    assert payload["runtime_check"]["ok"] is True


def test_preflight_reports_degraded_when_runtime_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "threadsense.inference.local_runtime.LocalRuntimeClient.probe",
        lambda self: RuntimeProbeResult(
            ok=False,
            endpoint=self._config.chat_endpoint,
            model=self._config.model,
            status_code=None,
            latency_ms=1.5,
            response_id=None,
            response_model=None,
            finish_reason=None,
            stream=False,
            error="network_error: runtime endpoint is unreachable",
        ),
    )

    exit_code = main(["preflight"])
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 1
    assert payload["status"] == "degraded"
    assert payload["runtime_check"]["ok"] is False
