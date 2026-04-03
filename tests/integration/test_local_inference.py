from __future__ import annotations

import json
from pathlib import Path

import pytest

from threadsense.cli import main
from threadsense.config import RedditConfig
from threadsense.connectors.reddit import RedditConnector, RedditThreadRequest
from threadsense.models.canonical import load_canonical_thread
from threadsense.pipeline.analyze import analyze_thread


def build_analysis_artifact(tmp_path: Path) -> Path:
    canonical_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    thread = load_canonical_thread(canonical_path)
    analysis = analyze_thread(thread, canonical_path)
    analysis_path = tmp_path / "analysis.json"
    analysis_path.write_text(json.dumps(analysis.to_dict()), encoding="utf-8")
    return analysis_path


def test_infer_analysis_uses_live_local_runtime(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    analysis_path = build_analysis_artifact(tmp_path)

    exit_code = main(
        [
            "infer",
            "analysis",
            "--input",
            str(analysis_path),
            "--task",
            "analysis_summary",
            "--required",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["provider"] == "local_openai_compatible"
    assert payload["used_fallback"] is False
    assert payload["output"]["headline"]
    assert payload["output"]["cited_theme_keys"]


def test_infer_analysis_falls_back_when_runtime_is_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "false")

    exit_code = main(
        [
            "infer",
            "analysis",
            "--input",
            str(analysis_path),
            "--task",
            "analysis_summary",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == "degraded"
    assert payload["provider"] == "deterministic_fallback"
    assert payload["used_fallback"] is True


def test_end_to_end_fetch_normalize_analyze_and_infer(
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

    fetch_exit = main(
        [
            "fetch",
            "reddit",
            "https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread",
        ]
    )
    fetch_report = json.loads(capsys.readouterr().out)
    raw_path = Path(fetch_report["output_path"])

    normalize_exit = main(["normalize", "reddit", "--input", str(raw_path)])
    normalize_report = json.loads(capsys.readouterr().out)
    normalized_path = Path(normalize_report["output_path"])

    analyze_exit = main(["analyze", "normalized", "--input", str(normalized_path)])
    analyze_report = json.loads(capsys.readouterr().out)
    analysis_path = Path(analyze_report["output_path"])

    infer_exit = main(
        [
            "infer",
            "analysis",
            "--input",
            str(analysis_path),
            "--task",
            "analysis_summary",
            "--required",
        ]
    )
    infer_report = json.loads(capsys.readouterr().out)

    assert fetch_exit == 0
    assert normalize_exit == 0
    assert analyze_exit == 0
    assert infer_exit == 0
    assert raw_path.exists()
    assert normalized_path.exists()
    assert analysis_path.exists()
    assert infer_report["provider"] == "local_openai_compatible"
    assert infer_report["output"]["headline"]
