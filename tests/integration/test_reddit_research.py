from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from threadsense.cli import main
from threadsense.cli_display import set_output_mode
from threadsense.config import RedditConfig
from threadsense.connectors.reddit import (
    RedditConnector,
    RedditSearchMatch,
    RedditSearchRequest,
    RedditSearchResult,
    RedditThreadRequest,
)


@pytest.fixture(autouse=True)
def _reset_output_mode() -> None:
    set_output_mode(None)


def load_fixture(name: str) -> list[dict[str, Any]]:
    payload = json.loads(Path(f"tests/fixtures/reddit/raw/{name}").read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    return payload


def test_research_reddit_builds_corpus_from_selected_subreddits(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixtures = {
        "abc123": load_fixture("normal_thread.json"),
        "jkl012": load_fixture("large_thread.json"),
    }

    class FixtureConnector(RedditConnector):
        def __init__(self, config: RedditConfig) -> None:
            self._config = config

        def search_threads(self, search_request: RedditSearchRequest) -> RedditSearchResult:
            assert search_request.query == "second brain OR agentic PKM"
            assert search_request.time_window == "30d"
            return RedditSearchResult(
                query=search_request.query,
                subreddits=search_request.subreddits,
                sort=search_request.sort,
                time_window=search_request.time_window,
                reddit_time_bucket="month",
                matches=[
                    RedditSearchMatch(
                        post_id="abc123",
                        title="Second brain workflow for Claude Code",
                        selftext="Agentic PKM workflow with prompts and notes.",
                        subreddit="ClaudeCode",
                        author="alpha",
                        permalink="https://reddit.com/r/ClaudeCode/comments/abc123/example/",
                        thread_url="https://reddit.com/r/ClaudeCode/comments/abc123/example/",
                        normalized_url="https://reddit.com/r/ClaudeCode/comments/abc123/example.json",
                        score=42,
                        num_comments=12,
                        created_utc=1710000000.0,
                    ),
                    RedditSearchMatch(
                        post_id="jkl012",
                        title="Agentic PKM stack with local models",
                        selftext="A second brain discussion for local agents.",
                        subreddit="LocalLLaMA",
                        author="beta",
                        permalink="https://reddit.com/r/LocalLLaMA/comments/jkl012/example/",
                        thread_url="https://reddit.com/r/LocalLLaMA/comments/jkl012/example/",
                        normalized_url="https://reddit.com/r/LocalLLaMA/comments/jkl012/example.json",
                        score=39,
                        num_comments=8,
                        created_utc=1710000100.0,
                    ),
                ],
            )

        def fetch_thread(self, scrape_request: RedditThreadRequest):
            for marker, fixture in fixtures.items():
                if marker in scrape_request.post_url:
                    connector = RedditConnector(
                        config=RedditConfig(
                            user_agent=self._config.user_agent,
                            timeout_seconds=self._config.timeout_seconds,
                            max_retries=0,
                            backoff_seconds=self._config.backoff_seconds,
                            request_delay_seconds=0,
                            listing_limit=self._config.listing_limit,
                        ),
                        transport=lambda url, headers, params, timeout, fixture=fixture: fixture,
                        sleeper=lambda value: None,
                    )
                    return connector.fetch_thread(scrape_request)
            raise AssertionError(f"unexpected fixture request: {scrape_request.post_url}")

    monkeypatch.setattr(
        "threadsense.cli.build_reddit_connector",
        lambda config: FixtureConnector(config.reddit),
    )
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(tmp_path / "store"))

    exit_code = main(
        [
            "research",
            "reddit",
            "--query",
            "second brain OR agentic PKM",
            "--subreddit",
            "ClaudeCode",
            "--subreddit",
            "LocalLLaMA",
            "--time-window",
            "30d",
            "--limit",
            "2",
            "--per-subreddit-limit",
            "2",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "research"
    assert payload["discovered_thread_count"] == 2
    assert payload["selected_thread_count"] == 2
    assert payload["fetched_thread_count"] == 2
    assert payload["time_window"] == "30d"
    assert payload["reddit_time_bucket"] == "month"
    assert payload["terminal_summary"]["headline"]
    assert payload["terminal_summary"]["top_threads"]
    assert len(payload["selected_threads"]) == 2
    assert Path(payload["manifest_path"]).exists()
    assert Path(payload["corpus_analysis_path"]).exists()
    assert Path(payload["corpus_report_path"]).exists()


def test_research_reddit_rejects_unsupported_advanced_query_syntax(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(tmp_path / "store"))

    exit_code = main(
        [
            "research",
            "reddit",
            "--query",
            'title:"second brain"',
            "--subreddit",
            "ClaudeCode",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["error"]["code"] == "analysis_error"
