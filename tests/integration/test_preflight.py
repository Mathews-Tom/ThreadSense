from __future__ import annotations

import json
import sys

import pytest

from threadsense.cli import main
from threadsense.config import AppConfig
from threadsense.inference.local_runtime import RuntimeProbeResult
from threadsense.preflight import (
    DiagnosticCheck,
    check_python_version,
    check_storage_directory,
    run_diagnostic_checks,
)


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
        assert timeout_seconds == 90.0
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
    assert "diagnostics" in payload
    statuses = {d["name"]: d["status"] for d in payload["diagnostics"]}
    assert statuses["python_version"] == "pass"
    assert statuses["storage_directory"] == "pass"


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
    assert "diagnostics" in payload


def test_preflight_skip_runtime_includes_diagnostics_without_network(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(["preflight", "--skip-runtime"])
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 0
    assert payload["status"] == "ready"
    assert "runtime_check" not in payload
    assert "diagnostics" in payload
    names = [d["name"] for d in payload["diagnostics"]]
    assert "python_version" in names
    assert "storage_directory" in names
    assert "reddit_reachable" not in names


def test_check_python_version_passes_on_supported_runtime() -> None:
    result = check_python_version()
    assert result.status == "pass"
    version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    assert version in result.message


def test_check_storage_directory_passes_for_writable_dir(
    tmp_path: object,
) -> None:
    config = AppConfig(
        storage={"root_dir": str(tmp_path)},  # type: ignore[arg-type]
    )
    result = check_storage_directory(config)
    assert result.status == "pass"
    assert "writable" in result.message


def test_run_diagnostic_checks_skip_network_excludes_reddit() -> None:
    config = AppConfig()
    checks = run_diagnostic_checks(config, skip_network=True)
    names = [c.name for c in checks]
    assert "python_version" in names
    assert "storage_directory" in names
    assert "reddit_reachable" not in names


def test_diagnostic_check_to_dict_structure() -> None:
    check = DiagnosticCheck("test_name", "pass", "all good")
    d = check.to_dict()
    assert d == {"name": "test_name", "status": "pass", "message": "all good"}
