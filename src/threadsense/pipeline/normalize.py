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
from threadsense.schema_utils import SchemaReader

_schema = SchemaReader(SchemaBoundaryError, "raw artifact")


def normalize_artifact(raw_artifact: Mapping[str, Any], raw_artifact_path: Path) -> Thread:
    source_name = _schema.required_str(raw_artifact, "source")
    if source_name == "reddit":
        return normalize_reddit_artifact(raw_artifact, raw_artifact_path)
    if source_name == "hackernews":
        return normalize_hackernews_artifact(raw_artifact, raw_artifact_path)
    if source_name == "github_discussions":
        return normalize_github_discussions_artifact(raw_artifact, raw_artifact_path)
    if source_name == "github_gist":
        return normalize_github_gist_artifact(raw_artifact, raw_artifact_path)
    raise SchemaBoundaryError(
        "raw artifact source is unsupported",
        details={"source": source_name},
    )


def normalize_reddit_artifact(raw_artifact: Mapping[str, Any], raw_artifact_path: Path) -> Thread:
    post = _schema.nested_object(raw_artifact, "post")
    comments_payload = _schema.nested_list(raw_artifact, "comments")
    flattened_comments_payload = flatten_raw_comment_payloads(comments_payload)
    thread_id = f"reddit:{_schema.required_str(post, 'id')}"
    source = SourceRef(
        source_name="reddit",
        community=_schema.required_str(post, "subreddit"),
        source_thread_id=_schema.required_str(post, "id"),
        thread_url=_schema.required_str(raw_artifact, "normalized_url"),
    )
    author = AuthorRef(
        username=_schema.required_str(post, "author"),
        source_author_id=None,
    )
    comments = [
        normalize_comment(thread_id=thread_id, payload=comment_payload)
        for comment_payload in flattened_comments_payload
    ]
    provenance = ProvenanceMetadata(
        raw_artifact_path=str(raw_artifact_path),
        raw_sha256=calculate_sha256(raw_artifact_path),
        retrieved_at_utc=_schema.required_float(raw_artifact, "fetched_at_utc"),
        normalized_at_utc=time(),
        schema_version=CANONICAL_SCHEMA_VERSION,
        normalization_version=CANONICAL_NORMALIZATION_VERSION,
    )
    comment_count = _schema.required_int(raw_artifact, "total_comment_count")
    if comment_count != len(comments):
        raise SchemaBoundaryError(
            "normalized comment count does not match raw artifact",
            details={"expected": comment_count, "actual": len(comments)},
        )
    post_body = required_present_nullable_str(post, "selftext")
    return Thread(
        thread_id=thread_id,
        source=source,
        title=_schema.required_str(post, "title"),
        body=post_body,
        permalink=_schema.required_str(post, "permalink"),
        author=author,
        comments=comments,
        comment_count=comment_count,
        provenance=provenance,
    )


def normalize_reddit_artifact_file(raw_artifact_path: Path) -> Thread:
    raw_artifact = load_raw_artifact(raw_artifact_path)
    return normalize_artifact(raw_artifact, raw_artifact_path)


def normalize_hackernews_artifact(
    raw_artifact: Mapping[str, Any],
    raw_artifact_path: Path,
) -> Thread:
    story = _schema.nested_object(raw_artifact, "story")
    comments_payload = _schema.nested_list(raw_artifact, "comments")
    flattened_comments_payload = flatten_raw_comment_payloads(comments_payload)
    story_id = _schema.required_int(story, "id")
    thread_id = f"hn:{story_id}"
    source = SourceRef(
        source_name="hackernews",
        community="hackernews",
        source_thread_id=str(story_id),
        thread_url=_schema.required_str(raw_artifact, "normalized_url"),
    )
    author = AuthorRef(
        username=_schema.required_str(story, "author"),
        source_author_id=None,
    )
    comments = [
        normalize_hackernews_comment(thread_id=thread_id, payload=comment_payload)
        for comment_payload in flattened_comments_payload
    ]
    provenance = ProvenanceMetadata(
        raw_artifact_path=str(raw_artifact_path),
        raw_sha256=calculate_sha256(raw_artifact_path),
        retrieved_at_utc=_schema.required_float(raw_artifact, "fetched_at_utc"),
        normalized_at_utc=time(),
        schema_version=CANONICAL_SCHEMA_VERSION,
        normalization_version=CANONICAL_NORMALIZATION_VERSION,
    )
    comment_count = _schema.required_int(raw_artifact, "total_comment_count")
    if comment_count != len(comments):
        raise SchemaBoundaryError(
            "normalized comment count does not match raw artifact",
            details={"expected": comment_count, "actual": len(comments)},
        )
    story_body = required_present_nullable_str(story, "body")
    return Thread(
        thread_id=thread_id,
        source=source,
        title=_schema.required_str(story, "title"),
        body=story_body,
        permalink=_schema.required_str(story, "permalink"),
        author=author,
        comments=comments,
        comment_count=comment_count,
        provenance=provenance,
    )


def normalize_comment(thread_id: str, payload: Mapping[str, Any]) -> Comment:
    parent_id = _schema.required_str(payload, "parent_id")
    comment_id = _schema.required_str(payload, "id")
    return Comment(
        thread_id=thread_id,
        comment_id=f"reddit:{comment_id}",
        parent_comment_id=normalize_parent_id(parent_id),
        author=AuthorRef(
            username=_schema.required_str(payload, "author"),
            source_author_id=None,
        ),
        body=_schema.required_str(payload, "body"),
        score=_schema.required_int(payload, "score"),
        created_utc=_schema.required_float(payload, "created_utc"),
        depth=_schema.required_int(payload, "depth"),
        permalink=_schema.required_str(payload, "permalink"),
    )


def normalize_hackernews_comment(thread_id: str, payload: Mapping[str, Any]) -> Comment:
    parent_id = _schema.required_int(payload, "parent")
    comment_id = _schema.required_int(payload, "id")
    return Comment(
        thread_id=thread_id,
        comment_id=f"hn:{comment_id}",
        parent_comment_id=normalize_hackernews_parent_id(thread_id, parent_id),
        author=AuthorRef(
            username=_schema.required_str(payload, "author"),
            source_author_id=None,
        ),
        body=_schema.required_str(payload, "body"),
        score=0,
        created_utc=_schema.required_float(payload, "created_utc"),
        depth=_schema.required_int(payload, "depth"),
        permalink=_schema.required_str(payload, "permalink"),
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


def normalize_hackernews_parent_id(thread_id: str, parent_id: int) -> str | None:
    story_id = int(thread_id.removeprefix("hn:"))
    if parent_id == story_id:
        return None
    return f"hn:{parent_id}"


def normalize_github_discussions_artifact(
    raw_artifact: Mapping[str, Any],
    raw_artifact_path: Path,
) -> Thread:
    discussion = _schema.nested_object(raw_artifact, "discussion")
    comments_payload = _schema.nested_list(raw_artifact, "comments")
    flattened_comments_payload = flatten_raw_comment_payloads(comments_payload)
    owner = _schema.required_str(discussion, "owner")
    repo = _schema.required_str(discussion, "repo")
    number = _schema.required_int(discussion, "number")
    thread_id = f"gh:{owner}/{repo}/discussions/{number}"
    source = SourceRef(
        source_name="github_discussions",
        community=f"{owner}/{repo}",
        source_thread_id=f"{owner}/{repo}/discussions/{number}",
        thread_url=_schema.required_str(raw_artifact, "normalized_url"),
    )
    author = AuthorRef(
        username=_schema.required_str(discussion, "author"),
        source_author_id=None,
    )
    comments = [
        normalize_github_discussions_comment(thread_id=thread_id, payload=comment_payload)
        for comment_payload in flattened_comments_payload
    ]
    provenance = ProvenanceMetadata(
        raw_artifact_path=str(raw_artifact_path),
        raw_sha256=calculate_sha256(raw_artifact_path),
        retrieved_at_utc=_schema.required_float(raw_artifact, "fetched_at_utc"),
        normalized_at_utc=time(),
        schema_version=CANONICAL_SCHEMA_VERSION,
        normalization_version=CANONICAL_NORMALIZATION_VERSION,
    )
    comment_count = _schema.required_int(raw_artifact, "total_comment_count")
    discussion_body = required_present_nullable_str(discussion, "body")
    return Thread(
        thread_id=thread_id,
        source=source,
        title=_schema.required_str(discussion, "title"),
        body=discussion_body,
        permalink=_schema.required_str(discussion, "url"),
        author=author,
        comments=comments,
        comment_count=comment_count,
        provenance=provenance,
    )


def normalize_github_discussions_comment(thread_id: str, payload: Mapping[str, Any]) -> Comment:
    node_id = _schema.required_str(payload, "id")
    return Comment(
        thread_id=thread_id,
        comment_id=f"gh:{node_id}",
        parent_comment_id=normalize_github_parent_id(payload.get("parent_node_id")),
        author=AuthorRef(
            username=_schema.required_str(payload, "author"),
            source_author_id=None,
        ),
        body=_schema.required_str(payload, "body"),
        score=_schema.required_int(payload, "score"),
        created_utc=_schema.required_float(payload, "created_utc"),
        depth=_schema.required_int(payload, "depth"),
        permalink=_schema.required_str(payload, "url"),
    )


def normalize_github_gist_artifact(
    raw_artifact: Mapping[str, Any],
    raw_artifact_path: Path,
) -> Thread:
    gist = _schema.nested_object(raw_artifact, "gist")
    comments_payload = _schema.nested_list(raw_artifact, "comments")
    gist_id = _schema.required_str(gist, "gist_id")
    thread_id = f"gist:{gist_id}"
    owner = _schema.required_str(gist, "owner")
    source = SourceRef(
        source_name="github_gist",
        community="gist.github.com",
        source_thread_id=gist_id,
        thread_url=_schema.required_str(raw_artifact, "normalized_url"),
    )
    author = AuthorRef(
        username=owner,
        source_author_id=None,
    )
    comments = [
        normalize_github_gist_comment(thread_id=thread_id, payload=comment_payload)
        for comment_payload in comments_payload
    ]
    provenance = ProvenanceMetadata(
        raw_artifact_path=str(raw_artifact_path),
        raw_sha256=calculate_sha256(raw_artifact_path),
        retrieved_at_utc=_schema.required_float(raw_artifact, "fetched_at_utc"),
        normalized_at_utc=time(),
        schema_version=CANONICAL_SCHEMA_VERSION,
        normalization_version=CANONICAL_NORMALIZATION_VERSION,
    )
    comment_count = _schema.required_int(raw_artifact, "total_comment_count")
    gist_body = _assemble_gist_body(gist)
    description = gist.get("description")
    title = str(description) if description else _gist_title_from_files(gist)
    return Thread(
        thread_id=thread_id,
        source=source,
        title=title,
        body=gist_body,
        permalink=_schema.required_str(gist, "html_url"),
        author=author,
        comments=comments,
        comment_count=comment_count,
        provenance=provenance,
    )


def normalize_github_gist_comment(thread_id: str, payload: Mapping[str, Any]) -> Comment:
    comment_id = payload.get("comment_id")
    if comment_id is None:
        raise SchemaBoundaryError("github gist comment missing comment_id")
    return Comment(
        thread_id=thread_id,
        comment_id=f"gist:{comment_id}",
        parent_comment_id=None,
        author=AuthorRef(
            username=str(payload.get("author", "[deleted]")),
            source_author_id=None,
        ),
        body=str(payload.get("body", "")),
        score=0,
        created_utc=float(payload.get("created_utc", 0.0)),
        depth=0,
        permalink=str(payload.get("url", "")),
    )


def _assemble_gist_body(gist: Mapping[str, Any]) -> str | None:
    files = gist.get("files")
    if not files or not isinstance(files, list):
        return None
    parts: list[str] = []
    for file_entry in files:
        if not isinstance(file_entry, dict):
            continue
        filename = file_entry.get("filename", "untitled")
        content = file_entry.get("content")
        if content is None:
            continue
        parts.append(f"## File: {filename}\n{content}")
    return "\n\n---\n\n".join(parts) if parts else None


def _gist_title_from_files(gist: Mapping[str, Any]) -> str:
    files = gist.get("files")
    if files and isinstance(files, list) and isinstance(files[0], dict):
        return str(files[0].get("filename", "untitled gist"))
    return "untitled gist"


def normalize_github_parent_id(parent_node_id: Any) -> str | None:
    if parent_node_id is None:
        return None
    if not isinstance(parent_node_id, str) or not parent_node_id:
        raise SchemaBoundaryError("github discussions parent id is invalid")
    return f"gh:{parent_node_id}"


def flatten_reddit_comments(comments: list[RedditComment]) -> list[RedditComment]:
    flattened: list[RedditComment] = []
    queue = list(comments)
    while queue:
        comment = queue.pop(0)
        flattened.append(comment)
        queue = list(comment.replies) + queue
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


def required_present_str(payload: Mapping[str, Any], key: str) -> str:
    if key not in payload:
        raise SchemaBoundaryError("raw artifact string field is missing", details={"key": key})
    value = payload[key]
    if not isinstance(value, str):
        raise SchemaBoundaryError("raw artifact string field is invalid", details={"key": key})
    return value


def required_present_nullable_str(payload: Mapping[str, Any], key: str) -> str | None:
    if key not in payload:
        raise SchemaBoundaryError("raw artifact string field is missing", details={"key": key})
    value = payload[key]
    if value is None:
        return None
    if not isinstance(value, str):
        raise SchemaBoundaryError("raw artifact string field is invalid", details={"key": key})
    return value
