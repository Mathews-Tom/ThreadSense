from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

import pytest

from threadsense.cli import main
from threadsense.config import RedditConfig
from threadsense.connectors.reddit import RedditConnector, RedditThreadRequest

LIVE_REDDIT_URL = (
    "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/"
    "anyone_actually_built_a_second_brain_that_isnt/"
)


def load_fixture(name: str) -> object:
    return json.loads(Path(f"tests/fixtures/reddit/raw/{name}").read_text(encoding="utf-8"))


def test_connector_matches_reference_comment_tree_against_fixture() -> None:
    fixture = load_fixture("normal_thread.json")
    assert isinstance(fixture, list)

    def fake_transport(
        url: str,
        headers: Mapping[str, str],
        params: Mapping[str, str | int | float | bool],
        timeout: float,
    ) -> object:
        assert url.endswith(".json")
        assert headers["User-Agent"] == "threadsense/test"
        assert params["limit"] == 500
        assert timeout == 15
        return fixture

    connector = RedditConnector(
        config=RedditConfig(
            user_agent="threadsense/test",
            timeout_seconds=15,
            max_retries=0,
            backoff_seconds=0.1,
            request_delay_seconds=0,
            listing_limit=500,
        ),
        transport=fake_transport,
        sleeper=lambda value: None,
    )

    result = connector.fetch_thread(
        RedditThreadRequest(
            post_url="https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread",
        )
    )

    assert result.post.id == "abc123"
    assert result.total_comment_count == 3
    assert len(result.comments) == 2
    assert result.comments[0].replies[0].id == "c1a"


def test_cli_fetch_reddit_writes_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = load_fixture("normal_thread.json")
    assert isinstance(fixture, list)
    output_path = tmp_path / "thread.json"

    class FixtureConnector:
        def __init__(self, config: object, cache: object | None = None) -> None:
            self._connector = RedditConnector(
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

        def fetch_thread(self, scrape_request: RedditThreadRequest) -> object:
            return self._connector.fetch_thread(scrape_request)

    monkeypatch.setattr("threadsense.cli.RedditConnector", FixtureConnector)

    exit_code = main(
        [
            "fetch",
            "reddit",
            "https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread",
            "--output",
            str(output_path),
        ]
    )
    report = json.loads(capsys.readouterr().out)
    artifact = json.loads(output_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert report["status"] == "ready"
    assert artifact["post"]["id"] == "abc123"
    assert artifact["total_comment_count"] == 3
    assert len(artifact["comments"]) == 2


@pytest.mark.live_reddit
def test_live_reddit_smoke() -> None:
    output_path = Path("tests/fixtures/reddit/raw/live-smoke-output.json")
    exit_code = main(
        [
            "fetch",
            "reddit",
            LIVE_REDDIT_URL,
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    assert artifact["source"] == "reddit"
    output_path.unlink(missing_ok=True)
