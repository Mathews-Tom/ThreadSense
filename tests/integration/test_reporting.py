from __future__ import annotations

import json
from pathlib import Path

import pytest

from threadsense.cli import main
from threadsense.config import RedditConfig
from threadsense.connectors.reddit import RedditConnector, RedditThreadRequest


def build_analysis_artifact(tmp_path: Path) -> Path:
    fixture = json.loads(
        Path("tests/fixtures/reddit/raw/normal_thread.json").read_text(encoding="utf-8")
    )
    assert isinstance(fixture, list)
    connector = RedditConnector(
        config=RedditConfig(
            user_agent="threadsense/test",
            timeout_seconds=15,
            max_retries=0,
            backoff_seconds=0.1,
            request_delay_seconds=0,
            listing_limit=500,
        ),
        transport=lambda url, headers, params, timeout: fixture,
        sleeper=lambda value: None,
    )
    raw_payload = connector.fetch_thread(
        RedditThreadRequest(
            post_url="https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread",
        )
    ).to_dict()
    raw_path = tmp_path / "raw.json"
    raw_path.write_text(json.dumps(raw_payload), encoding="utf-8")
    normalized_path = tmp_path / "normalized.json"
    analysis_path = tmp_path / "analysis.json"
    assert (
        main(["normalize", "reddit", "--input", str(raw_path), "--output", str(normalized_path)])
        == 0
    )
    assert (
        main(
            [
                "analyze",
                "normalized",
                "--input",
                str(normalized_path),
                "--output",
                str(analysis_path),
            ]
        )
        == 0
    )
    return analysis_path


def test_report_analysis_writes_json_artifact(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    report_path = tmp_path / "report.json"
    capsys.readouterr()

    exit_code = main(
        [
            "report",
            "analysis",
            "--input",
            str(analysis_path),
            "--format",
            "json",
            "--output",
            str(report_path),
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["format"] == "json"
    assert report_payload["artifact_kind"] == "thread_report"


def test_report_analysis_writes_markdown_with_live_summary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    report_path = tmp_path / "report.md"
    capsys.readouterr()

    exit_code = main(
        [
            "report",
            "analysis",
            "--input",
            str(analysis_path),
            "--format",
            "markdown",
            "--output",
            str(report_path),
            "--with-summary",
            "--summary-required",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    markdown = report_path.read_text(encoding="utf-8")

    assert exit_code == 0
    assert payload["summary_provider"] == "local_openai_compatible"
    assert "## Executive Summary" in markdown
    assert "## Findings" in markdown


def test_end_to_end_fetch_normalize_analyze_and_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = json.loads(
        Path("tests/fixtures/reddit/raw/normal_thread.json").read_text(encoding="utf-8")
    )
    assert isinstance(fixture, list)

    class FixtureConnector:
        def __init__(self, config: RedditConfig) -> None:
            self._connector = RedditConnector(
                config=RedditConfig(
                    user_agent=config.user_agent,
                    timeout_seconds=config.timeout_seconds,
                    max_retries=0,
                    backoff_seconds=config.backoff_seconds,
                    request_delay_seconds=0,
                    listing_limit=config.listing_limit,
                ),
                transport=lambda url, headers, params, timeout: fixture,
                sleeper=lambda value: None,
            )

        def fetch_thread(self, scrape_request: RedditThreadRequest) -> object:
            return self._connector.fetch_thread(scrape_request)

    monkeypatch.setattr(
        "threadsense.cli.build_reddit_connector",
        lambda config: FixtureConnector(config.reddit),
    )
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(tmp_path / "store"))

    assert (
        main(
            [
                "fetch",
                "reddit",
                "https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread",
            ]
        )
        == 0
    )
    fetch_report = json.loads(capsys.readouterr().out)
    raw_path = Path(fetch_report["output_path"])

    assert main(["normalize", "reddit", "--input", str(raw_path)]) == 0
    normalize_report = json.loads(capsys.readouterr().out)
    normalized_path = Path(normalize_report["output_path"])

    assert main(["analyze", "normalized", "--input", str(normalized_path)]) == 0
    analyze_report = json.loads(capsys.readouterr().out)
    analysis_path = Path(analyze_report["output_path"])

    report_exit = main(
        [
            "report",
            "analysis",
            "--input",
            str(analysis_path),
            "--format",
            "markdown",
            "--with-summary",
            "--summary-required",
        ]
    )
    report_output = json.loads(capsys.readouterr().out)
    markdown_path = Path(report_output["output_path"])

    assert report_exit == 0
    assert markdown_path.exists()
    assert "Representative Quotes" in markdown_path.read_text(encoding="utf-8")
