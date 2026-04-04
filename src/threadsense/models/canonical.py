from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from threadsense.errors import SchemaBoundaryError

CANONICAL_SCHEMA_VERSION = 1
CANONICAL_NORMALIZATION_VERSION = "reddit-to-canonical-v1"
CANONICAL_ARTIFACT_KIND = "canonical_thread"


@dataclass(frozen=True)
class AuthorRef:
    username: str
    source_author_id: str | None


@dataclass(frozen=True)
class SourceRef:
    source_name: str
    community: str
    source_thread_id: str
    thread_url: str


@dataclass(frozen=True)
class ProvenanceMetadata:
    raw_artifact_path: str
    raw_sha256: str
    retrieved_at_utc: float
    normalized_at_utc: float
    schema_version: int
    normalization_version: str


@dataclass(frozen=True)
class Comment:
    thread_id: str
    comment_id: str
    parent_comment_id: str | None
    author: AuthorRef
    body: str
    score: int
    created_utc: float
    depth: int
    permalink: str


@dataclass(frozen=True)
class Thread:
    thread_id: str
    source: SourceRef
    title: str
    permalink: str
    author: AuthorRef
    comments: list[Comment]
    comment_count: int
    provenance: ProvenanceMetadata

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_kind": CANONICAL_ARTIFACT_KIND,
            "schema_version": CANONICAL_SCHEMA_VERSION,
            "normalization_version": CANONICAL_NORMALIZATION_VERSION,
            "thread": asdict(self),
        }


def load_canonical_thread(path: Path) -> Thread:
    payload = migrate_canonical_payload(read_json_file(path))
    thread_data = nested_object(payload, "thread")
    source_data = nested_object(thread_data, "source")
    author_data = nested_object(thread_data, "author")
    provenance_data = nested_object(thread_data, "provenance")
    comments_data = nested_list(thread_data, "comments")
    comments = [comment_from_dict(thread_data["thread_id"], comment) for comment in comments_data]
    return Thread(
        thread_id=required_str(thread_data, "thread_id"),
        source=SourceRef(
            source_name=required_str(source_data, "source_name"),
            community=required_str(source_data, "community"),
            source_thread_id=required_str(source_data, "source_thread_id"),
            thread_url=required_str(source_data, "thread_url"),
        ),
        title=required_str(thread_data, "title"),
        permalink=required_str(thread_data, "permalink"),
        author=AuthorRef(
            username=required_str(author_data, "username"),
            source_author_id=optional_nullable_str(author_data, "source_author_id"),
        ),
        comments=comments,
        comment_count=required_int(thread_data, "comment_count"),
        provenance=ProvenanceMetadata(
            raw_artifact_path=required_str(provenance_data, "raw_artifact_path"),
            raw_sha256=required_str(provenance_data, "raw_sha256"),
            retrieved_at_utc=required_float(provenance_data, "retrieved_at_utc"),
            normalized_at_utc=required_float(provenance_data, "normalized_at_utc"),
            schema_version=required_int(provenance_data, "schema_version"),
            normalization_version=required_str(provenance_data, "normalization_version"),
        ),
    )


def migrate_canonical_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    artifact_kind = payload.get("artifact_kind")
    schema_version = payload.get("schema_version")
    if artifact_kind != CANONICAL_ARTIFACT_KIND:
        raise SchemaBoundaryError(
            "canonical artifact kind is invalid",
            details={"artifact_kind": artifact_kind},
        )
    if schema_version == CANONICAL_SCHEMA_VERSION:
        return payload
    raise SchemaBoundaryError(
        "canonical schema version is unsupported",
        details={"schema_version": schema_version, "supported": [CANONICAL_SCHEMA_VERSION]},
    )


def comment_from_dict(thread_id: str, payload: Mapping[str, Any]) -> Comment:
    author_data = nested_object(payload, "author")
    return Comment(
        thread_id=thread_id,
        comment_id=required_str(payload, "comment_id"),
        parent_comment_id=optional_nullable_str(payload, "parent_comment_id"),
        author=AuthorRef(
            username=required_str(author_data, "username"),
            source_author_id=optional_nullable_str(author_data, "source_author_id"),
        ),
        body=required_str(payload, "body"),
        score=required_int(payload, "score"),
        created_utc=required_float(payload, "created_utc"),
        depth=required_int(payload, "depth"),
        permalink=required_str(payload, "permalink"),
    )


def read_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise SchemaBoundaryError(
            "canonical artifact path does not exist",
            details={"path": str(path)},
        ) from error
    if not isinstance(payload, dict):
        raise SchemaBoundaryError("canonical artifact must decode to an object")
    return payload


def nested_object(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise SchemaBoundaryError(
            "canonical object field is invalid",
            details={"key": key},
        )
    return value


def nested_list(payload: Mapping[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise SchemaBoundaryError(
            "canonical list field is invalid",
            details={"key": key},
        )
    return value


def required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise SchemaBoundaryError(
            "canonical string field is invalid",
            details={"key": key},
        )
    return value


def optional_nullable_str(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise SchemaBoundaryError(
            "canonical optional string field is invalid",
            details={"key": key},
        )
    return value


def required_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise SchemaBoundaryError(
            "canonical integer field is invalid",
            details={"key": key},
        )
    return value


def required_float(payload: Mapping[str, Any], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise SchemaBoundaryError(
            "canonical float field is invalid",
            details={"key": key},
        )
    return value
