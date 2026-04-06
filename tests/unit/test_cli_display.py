from __future__ import annotations

import json

import pytest
from rich.console import Console

from threadsense.cli_display import (
    OutputMode,
    emit_error,
    emit_payload,
    render_research_panel,
    render_run_panel,
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


class TestRenderRunPanel:
    def test_run_panel_includes_terminal_summary_when_present(self) -> None:
        panel = render_run_panel(
            {
                "status": "ready",
                "source": "reddit",
                "thread_url": "https://example.com/thread",
                "fetch": {"status": "ok", "output_path": "/tmp/f.json"},
                "normalize": {"status": "ok", "output_path": "/tmp/n.json"},
                "analyze": {"status": "ok", "output_path": "/tmp/a.json"},
                "report": {
                    "status": "ok",
                    "output_path": "/tmp/r.md",
                    "summary_provider": "local_openai_compatible",
                    "terminal_summary": {
                        "headline": "Performance and docs dominate the thread",
                        "summary": "Latency and onboarding are the main blockers.",
                        "priority": "high",
                        "recommended_owner": "engineering",
                        "action_type": "fix",
                        "next_steps": ["Profile search latency", "Expand onboarding docs"],
                        "top_findings": [
                            {
                                "theme_label": "performance",
                                "severity": "high",
                                "recommended_owner": "engineering",
                                "action_type": "fix",
                            }
                        ],
                    },
                },
            }
        )

        console = Console(record=True, width=120)
        console.print(panel)
        rendered = console.export_text(clear=False)
        assert "Summary" in rendered
        assert "Performance and docs dominate the thread" in rendered
        assert "Next Steps" in rendered
        assert "Top Findings" in rendered

    def test_run_panel_keeps_stage_table_without_terminal_summary(self) -> None:
        panel = render_run_panel(
            {
                "status": "ready",
                "source": "reddit",
                "thread_url": "https://example.com/thread",
                "fetch": {"status": "ok", "output_path": "/tmp/f.json"},
                "normalize": {"status": "ok", "output_path": "/tmp/n.json"},
                "analyze": {"status": "ok", "output_path": "/tmp/a.json"},
                "report": {"status": "ok", "output_path": "/tmp/r.md", "summary_provider": "none"},
            }
        )

        console = Console(record=True, width=120)
        console.print(panel)
        rendered = console.export_text(clear=False)
        assert "fetch" in rendered
        assert "report" in rendered
        assert "Summary" not in rendered


class TestRenderResearchPanel:
    def test_research_panel_includes_terminal_summary_when_present(self) -> None:
        panel = render_research_panel(
            {
                "status": "ready",
                "artifact_type": "research",
                "query": "second brain OR agentic PKM",
                "subreddits": ["ClaudeCode", "LocalLLaMA"],
                "time_window": "30d",
                "reddit_time_bucket": "month",
                "discovered_thread_count": 7,
                "selected_thread_count": 5,
                "fetched_thread_count": 5,
                "corpus_report_path": "/tmp/report.md",
                "terminal_summary": {
                    "headline": "Second-brain discussions center on workflow capture and memory retrieval",
                    "key_patterns": ["PKM demand is concentrated in ClaudeCode and LocalLLaMA"],
                    "recommended_actions": ["Review note capture workflows"],
                    "confidence_note": "Based on five selected threads.",
                    "top_threads": [
                        {
                            "subreddit": "ClaudeCode",
                            "title": "Anyone actually built a second brain?",
                            "match_source": "title_phrase",
                        }
                    ],
                },
            }
        )

        console = Console(record=True, width=120)
        console.print(panel)
        rendered = console.export_text(clear=False)
        assert "ThreadSense Research: reddit" in rendered
        assert "Key Patterns" in rendered
        assert "Recommended Actions" in rendered
        assert "Top Threads" in rendered

    def test_research_panel_keeps_overview_without_terminal_summary(self) -> None:
        panel = render_research_panel(
            {
                "status": "ready",
                "artifact_type": "research",
                "query": "second brain",
                "subreddits": ["ClaudeCode"],
                "time_window": "30d",
                "reddit_time_bucket": "month",
                "discovered_thread_count": 3,
                "selected_thread_count": 2,
                "fetched_thread_count": 2,
                "corpus_report_path": "/tmp/report.md",
            }
        )

        console = Console(record=True, width=120)
        console.print(panel)
        rendered = console.export_text(clear=False)
        assert "Query" in rendered
        assert "Corpus Report" in rendered
        assert "Top Threads" not in rendered

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
