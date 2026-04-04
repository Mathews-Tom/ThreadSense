from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from threadsense.errors import SchemaBoundaryError
from threadsense.schema_utils import SchemaReader

CANONICAL_SCHEMA_VERSION = 1
CANONICAL_NORMALIZATION_VERSION = "reddit-to-canonical-v1"
CANONICAL_ARTIFACT_KIND = "canonical_thread"

_schema = SchemaReader(SchemaBoundaryError, "canonical")


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
    thread_data = _schema.nested_object(payload, "thread")
    source_data = _schema.nested_object(thread_data, "source")
    author_data = _schema.nested_object(thread_data, "author")
    provenance_data = _schema.nested_object(thread_data, "provenance")
    comments_data = _schema.nested_list(thread_data, "comments")
    comments = [comment_from_dict(thread_data["thread_id"], comment) for comment in comments_data]
    return Thread(
        thread_id=_schema.required_str(thread_data, "thread_id"),
        source=SourceRef(
            source_name=_schema.required_str(source_data, "source_name"),
            community=_schema.required_str(source_data, "community"),
            source_thread_id=_schema.required_str(source_data, "source_thread_id"),
            thread_url=_schema.required_str(source_data, "thread_url"),
        ),
        title=_schema.required_str(thread_data, "title"),
        permalink=_schema.required_str(thread_data, "permalink"),
        author=AuthorRef(
            username=_schema.required_str(author_data, "username"),
            source_author_id=_schema.optional_nullable_str(author_data, "source_author_id"),
        ),
        comments=comments,
        comment_count=_schema.required_int(thread_data, "comment_count"),
        provenance=ProvenanceMetadata(
            raw_artifact_path=_schema.required_str(provenance_data, "raw_artifact_path"),
            raw_sha256=_schema.required_str(provenance_data, "raw_sha256"),
            retrieved_at_utc=_schema.required_float(provenance_data, "retrieved_at_utc"),
            normalized_at_utc=_schema.required_float(provenance_data, "normalized_at_utc"),
            schema_version=_schema.required_int(provenance_data, "schema_version"),
            normalization_version=_schema.required_str(provenance_data, "normalization_version"),
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
    author_data = _schema.nested_object(payload, "author")
    return Comment(
        thread_id=thread_id,
        comment_id=_schema.required_str(payload, "comment_id"),
        parent_comment_id=_schema.optional_nullable_str(payload, "parent_comment_id"),
        author=AuthorRef(
            username=_schema.required_str(author_data, "username"),
            source_author_id=_schema.optional_nullable_str(author_data, "source_author_id"),
        ),
        body=_schema.required_str(payload, "body"),
        score=_schema.required_int(payload, "score"),
        created_utc=_schema.required_float(payload, "created_utc"),
        depth=_schema.required_int(payload, "depth"),
        permalink=_schema.required_str(payload, "permalink"),
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
