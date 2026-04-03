from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from threadsense.cli import main
from threadsense.config import RedditConfig
from threadsense.connectors.reddit import RedditConnector, RedditThreadRequest
from threadsense.pipeline.storage import (
    build_storage_paths,
    load_analysis_artifact,
    load_normalized_artifact,
    load_raw_artifact,
)


def load_fixture(name: str) -> list[dict[str, Any]]:
    payload = json.loads(Path(f"tests/fixtures/reddit/raw/{name}").read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    return payload


def test_normalize_and_reload_persisted_thread(tmp_path: Path) -> None:
    fixture = load_fixture("normal_thread.json")
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

    normalize_exit = main(
        [
            "normalize",
            "reddit",
            "--input",
            str(raw_path),
            "--output",
            str(normalized_path),
        ]
    )

    assert normalize_exit == 0
    raw_loaded = load_raw_artifact(raw_path)
    normalized_loaded = load_normalized_artifact(normalized_path)
    assert raw_loaded["post"]["id"] == normalized_loaded.source.source_thread_id
    assert normalized_loaded.comment_count == raw_loaded["total_comment_count"]


def test_end_to_end_fetch_normalize_analyze_and_inspect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = load_fixture("normal_thread.json")
    assert isinstance(fixture, list)
    store_root = tmp_path / "store"
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(store_root))

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

    inspect_exit = main(["inspect", "analysis", "--input", str(analysis_path)])
    inspect_report = json.loads(capsys.readouterr().out)

    assert fetch_exit == 0
    assert normalize_exit == 0
    assert analyze_exit == 0
    assert inspect_exit == 0
    assert raw_path.exists()
    assert normalized_path.exists()
    assert analysis_path.exists()
    assert inspect_report["thread_id"] == "reddit:abc123"
    assert inspect_report["distinct_comment_count"] == 3
    assert inspect_report["duplicate_group_count"] == 0
    assert inspect_report["top_findings"]


def test_build_storage_paths_keep_raw_and_normalized_separate(tmp_path: Path) -> None:
    from threadsense.config import StorageConfig

    paths = build_storage_paths(
        StorageConfig(
            root_dir=tmp_path,
            raw_dirname="raw",
            normalized_dirname="normalized",
            analysis_dirname="analysis",
            report_dirname="reports",
        ),
        source_name="reddit",
        source_thread_id="abc123",
    )

    assert paths.raw_path != paths.normalized_path
    assert paths.normalized_path != paths.analysis_path
    assert paths.raw_path.name == "abc123.json"
    assert paths.normalized_path.name == "abc123.json"
    assert paths.analysis_path.name == "abc123.json"


def test_persisted_analysis_artifact_reloads(tmp_path: Path) -> None:
    fixture = load_fixture("normal_thread.json")
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

    analysis = load_analysis_artifact(analysis_path)

    assert analysis.thread_id == "reddit:abc123"
    assert analysis.total_comments == 3
    assert analysis.provenance.source_thread_id == "abc123"
