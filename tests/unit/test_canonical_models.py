from __future__ import annotations

import json
from pathlib import Path

import pytest

from threadsense.errors import SchemaBoundaryError
from threadsense.models.canonical import (
    CANONICAL_ARTIFACT_KIND,
    CANONICAL_NORMALIZATION_VERSION,
    CANONICAL_SCHEMA_VERSION,
    load_canonical_thread,
)


def test_load_canonical_thread_round_trips_valid_payload(tmp_path: Path) -> None:
    artifact_path = tmp_path / "thread.json"
    artifact_path.write_text(
        json.dumps(
            {
                "artifact_kind": CANONICAL_ARTIFACT_KIND,
                "schema_version": CANONICAL_SCHEMA_VERSION,
                "normalization_version": CANONICAL_NORMALIZATION_VERSION,
                "thread": {
                    "thread_id": "reddit:abc123",
                    "source": {
                        "source_name": "reddit",
                        "community": "ThreadSense",
                        "source_thread_id": "abc123",
                        "thread_url": "https://www.reddit.com/r/ThreadSense/comments/abc123/thread.json",
                    },
                    "title": "Example thread",
                    "body": "Example post body",
                    "permalink": "https://reddit.com/r/ThreadSense/comments/abc123/thread/",
                    "author": {"username": "op", "source_author_id": None},
                    "comments": [
                        {
                            "thread_id": "reddit:abc123",
                            "comment_id": "reddit:c1",
                            "parent_comment_id": None,
                            "author": {"username": "user1", "source_author_id": None},
                            "body": "Example comment",
                            "score": 3,
                            "created_utc": 1710000000.0,
                            "depth": 0,
                            "permalink": "https://reddit.com/r/ThreadSense/comments/abc123/thread/c1/",
                        }
                    ],
                    "comment_count": 1,
                    "provenance": {
                        "raw_artifact_path": "/tmp/raw.json",
                        "raw_sha256": "abc",
                        "retrieved_at_utc": 1710000000.0,
                        "normalized_at_utc": 1710000001.0,
                        "schema_version": 1,
                        "normalization_version": CANONICAL_NORMALIZATION_VERSION,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    thread = load_canonical_thread(artifact_path)

    assert thread.thread_id == "reddit:abc123"
    assert thread.body == "Example post body"
    assert thread.comment_count == 1
    assert thread.comments[0].comment_id == "reddit:c1"


def test_load_canonical_thread_rejects_v1_artifact(tmp_path: Path) -> None:
    artifact_path = tmp_path / "thread.json"
    artifact_path.write_text(
        json.dumps(
            {
                "artifact_kind": CANONICAL_ARTIFACT_KIND,
                "schema_version": 1,
                "normalization_version": "reddit-to-canonical-v1",
                "thread": {
                    "thread_id": "reddit:abc123",
                    "source": {
                        "source_name": "reddit",
                        "community": "ThreadSense",
                        "source_thread_id": "abc123",
                        "thread_url": "https://www.reddit.com/r/ThreadSense/comments/abc123/thread.json",
                    },
                    "title": "Example thread",
                    "permalink": "https://reddit.com/r/ThreadSense/comments/abc123/thread/",
                    "author": {"username": "op", "source_author_id": None},
                    "comments": [],
                    "comment_count": 0,
                    "provenance": {
                        "raw_artifact_path": "/tmp/raw.json",
                        "raw_sha256": "abc",
                        "retrieved_at_utc": 1710000000.0,
                        "normalized_at_utc": 1710000001.0,
                        "schema_version": 1,
                        "normalization_version": "reddit-to-canonical-v1",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SchemaBoundaryError):
        load_canonical_thread(artifact_path)


def test_load_canonical_thread_rejects_unsupported_schema_version(tmp_path: Path) -> None:
    artifact_path = tmp_path / "thread.json"
    artifact_path.write_text(
        json.dumps(
            {
                "artifact_kind": CANONICAL_ARTIFACT_KIND,
                "schema_version": 99,
                "normalization_version": CANONICAL_NORMALIZATION_VERSION,
                "thread": {},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SchemaBoundaryError):
        load_canonical_thread(artifact_path)
