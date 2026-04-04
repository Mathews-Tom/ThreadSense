from __future__ import annotations

import json

import pytest

from threadsense.cli_display import (
    OutputMode,
    emit_error,
    emit_payload,
    resolve_output_mode,
    set_output_mode,
    use_rich_output,
)
from threadsense.errors import ThreadSenseError


@pytest.fixture(autouse=True)
def _reset_output_mode() -> None:
    set_output_mode(None)


class TestResolveOutputMode:
    def test_returns_explicit_mode_when_set(self) -> None:
        set_output_mode(OutputMode.QUIET)
        assert resolve_output_mode() is OutputMode.QUIET

    def test_falls_back_to_json_when_not_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import sys

        monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
        assert resolve_output_mode() is OutputMode.JSON

    def test_falls_back_to_human_when_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import sys

        monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
        assert resolve_output_mode() is OutputMode.HUMAN


class TestUseRichOutput:
    def test_true_when_human(self) -> None:
        set_output_mode(OutputMode.HUMAN)
        assert use_rich_output() is True

    def test_false_when_json(self) -> None:
        set_output_mode(OutputMode.JSON)
        assert use_rich_output() is False

    def test_false_when_quiet(self) -> None:
        set_output_mode(OutputMode.QUIET)
        assert use_rich_output() is False


class TestEmitPayloadJson:
    def test_json_mode_prints_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        set_output_mode(OutputMode.JSON)
        payload = {"status": "ready", "value": 42}
        emit_payload(payload)
        captured = capsys.readouterr()
        assert json.loads(captured.out) == payload

    def test_json_mode_suppresses_rich_for_run_payload(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        set_output_mode(OutputMode.JSON)
        payload = {
            "status": "ready",
            "source": "reddit",
            "thread_url": "https://example.com",
            "fetch": {"status": "ok", "output_path": "/tmp/f.json"},
            "normalize": {"status": "ok", "output_path": "/tmp/n.json"},
            "analyze": {"status": "ok", "output_path": "/tmp/a.json"},
            "report": {"status": "ok", "output_path": "/tmp/r.md", "summary_provider": "none"},
        }
        emit_payload(payload)
        captured = capsys.readouterr()
        assert json.loads(captured.out) == payload


class TestEmitPayloadQuiet:
    def test_quiet_prints_status_only(self, capsys: pytest.CaptureFixture[str]) -> None:
        set_output_mode(OutputMode.QUIET)
        emit_payload({"status": "ready", "extra": "ignored"})
        assert capsys.readouterr().out.strip() == "ready"

    def test_quiet_prints_error_message(self, capsys: pytest.CaptureFixture[str]) -> None:
        set_output_mode(OutputMode.QUIET)
        emit_payload({"status": "error", "error": {"message": "boom"}})
        assert capsys.readouterr().out.strip() == "error: boom"

    def test_quiet_prints_unknown_when_no_status(self, capsys: pytest.CaptureFixture[str]) -> None:
        set_output_mode(OutputMode.QUIET)
        emit_payload({"value": 1})
        assert capsys.readouterr().out.strip() == "unknown"

    def test_quiet_handles_string_error(self, capsys: pytest.CaptureFixture[str]) -> None:
        set_output_mode(OutputMode.QUIET)
        emit_payload({"status": "error", "error": "simple string"})
        assert capsys.readouterr().out.strip() == "error: simple string"


class TestEmitError:
    def test_json_mode_prints_json_error(self, capsys: pytest.CaptureFixture[str]) -> None:
        set_output_mode(OutputMode.JSON)
        error = ThreadSenseError(code="TEST_ERR", message="test failure")
        emit_error(error)
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "TEST_ERR"

    def test_quiet_mode_prints_error_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        set_output_mode(OutputMode.QUIET)
        error = ThreadSenseError(code="TEST_ERR", message="test failure")
        emit_error(error)
        assert capsys.readouterr().out.strip() == "error: test failure"
