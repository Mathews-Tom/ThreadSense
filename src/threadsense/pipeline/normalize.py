from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from time import time
from typing import Any

from threadsense.connectors.reddit import RedditComment
from threadsense.errors import SchemaBoundaryError
from threadsense.models.canonical import (
    CANONICAL_NORMALIZATION_VERSION,
    CANONICAL_SCHEMA_VERSION,
    AuthorRef,
    Comment,
    ProvenanceMetadata,
    SourceRef,
    Thread,
)
from threadsense.pipeline.storage import calculate_sha256, load_raw_artifact


def normalize_reddit_artifact(raw_artifact: Mapping[str, Any], raw_artifact_path: Path) -> Thread:
    post = nested_object(raw_artifact, "post")
    comments_payload = nested_list(raw_artifact, "comments")
    flattened_comments_payload = flatten_raw_comment_payloads(comments_payload)
    thread_id = f"reddit:{required_str(post, 'id')}"
    source = SourceRef(
        source_name="reddit",
        community=required_str(post, "subreddit"),
        source_thread_id=required_str(post, "id"),
        thread_url=required_str(raw_artifact, "normalized_url"),
    )
    author = AuthorRef(
        username=required_str(post, "author"),
        source_author_id=None,
    )
    comments = [
        normalize_comment(thread_id=thread_id, payload=comment_payload)
        for comment_payload in flattened_comments_payload
    ]
    provenance = ProvenanceMetadata(
        raw_artifact_path=str(raw_artifact_path),
        raw_sha256=calculate_sha256(raw_artifact_path),
        retrieved_at_utc=required_float(raw_artifact, "fetched_at_utc"),
        normalized_at_utc=time(),
        schema_version=CANONICAL_SCHEMA_VERSION,
        normalization_version=CANONICAL_NORMALIZATION_VERSION,
    )
    comment_count = required_int(raw_artifact, "total_comment_count")
    if comment_count != len(comments):
        raise SchemaBoundaryError(
            "normalized comment count does not match raw artifact",
            details={"expected": comment_count, "actual": len(comments)},
        )
    return Thread(
        thread_id=thread_id,
        source=source,
        title=required_str(post, "title"),
        permalink=required_str(post, "permalink"),
        author=author,
        comments=comments,
        comment_count=comment_count,
        provenance=provenance,
    )


def normalize_reddit_artifact_file(raw_artifact_path: Path) -> Thread:
    raw_artifact = load_raw_artifact(raw_artifact_path)
    return normalize_reddit_artifact(raw_artifact, raw_artifact_path)


def normalize_comment(thread_id: str, payload: Mapping[str, Any]) -> Comment:
    parent_id = required_str(payload, "parent_id")
    comment_id = required_str(payload, "id")
    return Comment(
        thread_id=thread_id,
        comment_id=f"reddit:{comment_id}",
        parent_comment_id=normalize_parent_id(parent_id),
        author=AuthorRef(
            username=required_str(payload, "author"),
            source_author_id=None,
        ),
        body=required_str(payload, "body"),
        score=required_int(payload, "score"),
        created_utc=required_float(payload, "created_utc"),
        depth=required_int(payload, "depth"),
        permalink=required_str(payload, "permalink"),
    )


def normalize_parent_id(parent_id: str) -> str | None:
    if parent_id.startswith("t3_"):
        return None
    if parent_id.startswith("t1_"):
        return f"reddit:{parent_id.removeprefix('t1_')}"
    raise SchemaBoundaryError(
        "reddit parent_id has unsupported prefix",
        details={"parent_id": parent_id},
    )


def flatten_reddit_comments(comments: list[RedditComment]) -> list[RedditComment]:
    flattened: list[RedditComment] = []
    queue = list(comments)
    while queue:
        comment = queue.pop(0)
        flattened.append(comment)
        queue = comment.replies + queue
    return flattened


def flatten_raw_comment_payloads(comments: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    flattened: list[Mapping[str, Any]] = []
    queue = list(comments)
    while queue:
        comment = queue.pop(0)
        flattened.append(comment)
        replies = comment.get("replies", [])
        if not isinstance(replies, list):
            raise SchemaBoundaryError(
                "raw artifact replies field is invalid",
                details={"comment_id": comment.get("id")},
            )
        queue = replies + queue
    return flattened


def nested_object(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise SchemaBoundaryError(
            "raw artifact object field is invalid",
            details={"key": key},
        )
    return value


def nested_list(payload: Mapping[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise SchemaBoundaryError(
            "raw artifact list field is invalid",
            details={"key": key},
        )
    return value


def required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise SchemaBoundaryError(
            "raw artifact string field is invalid",
            details={"key": key},
        )
    return value


def required_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise SchemaBoundaryError(
            "raw artifact integer field is invalid",
            details={"key": key},
        )
    return value


def required_float(payload: Mapping[str, Any], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise SchemaBoundaryError(
            "raw artifact float field is invalid",
            details={"key": key},
        )
    return value
