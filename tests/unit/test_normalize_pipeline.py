from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from threadsense.config import RedditConfig
from threadsense.connectors.reddit import RedditConnector, RedditThreadRequest
from threadsense.errors import SchemaBoundaryError
from threadsense.pipeline.normalize import normalize_parent_id, normalize_reddit_artifact_file
from threadsense.pipeline.storage import calculate_sha256


def load_raw_fixture(name: str) -> list[dict[str, Any]]:
    payload = json.loads(Path(f"tests/fixtures/reddit/raw/{name}").read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    return payload


def test_normalize_reddit_artifact_file_maps_fields(tmp_path: Path) -> None:
    fixture = load_raw_fixture("normal_thread.json")
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

    thread = normalize_reddit_artifact_file(raw_path)

    assert thread.thread_id == "reddit:abc123"
    assert thread.body == "Exploring a second brain workflow with agents."
    assert thread.comment_count == 3
    assert thread.comments[0].comment_id == "reddit:c1"
    assert thread.comments[1].parent_comment_id == "reddit:c1"
    assert thread.provenance.raw_sha256 == calculate_sha256(raw_path)


def test_normalize_parent_id_rejects_unknown_prefix() -> None:
    with pytest.raises(SchemaBoundaryError):
        normalize_parent_id("tx_bad")


def test_normalize_reddit_artifact_file_rejects_v1_raw_payload_without_selftext(
    tmp_path: Path,
) -> None:
    fixture = load_raw_fixture("normal_thread.json")
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
    raw_payload["artifact_version"] = 1
    raw_payload["post"].pop("selftext")
    raw_path = tmp_path / "raw-v1.json"
    raw_path.write_text(json.dumps(raw_payload), encoding="utf-8")

    with pytest.raises(SchemaBoundaryError):
        normalize_reddit_artifact_file(raw_path)


def nested_list(payload: Mapping[str, Any], *keys: str) -> list[Any]:
    current: Any = payload
    for key in keys:
        assert isinstance(current, dict)
        current = current[key]
    assert isinstance(current, list)
    return current
