"""Decision document, run record and immutable publication for typed decision annotations.

The decision document holds deterministic content only. Anything that varies between runs
over identical inputs (timestamps, wall times, run ids, endpoint) lives in the run record,
so re-annotating an unchanged snapshot reproduces the same decision bytes and hash.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.decisions.contracts import (
    CALIBRATION,
    DECISION_ARTIFACT_KIND,
    DECISION_ARTIFACT_SCHEMA_VERSION,
    DECISION_RUN_ARTIFACT_KIND,
    MODE,
    POLICY,
    BatchPlan,
    CheckpointVerification,
    CommentState,
    ContextPolicy,
    CoverageSummary,
    HttpExchange,
    InferenceSettings,
    ItemFailure,
    ItemStatus,
    QuestionSet,
    Requirement,
    RoutingRecord,
    RuntimeManifest,
    UsageCoverage,
    ValidatedAnswer,
    encode_request_body,
)
from threadsense.errors import (
    DecisionArtifactError,
    DecisionInputError,
    ThreadSenseError,
)
from threadsense.monitoring.sweep import _sync_to_storage

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_RUN_ID_PATTERN = re.compile(r"[0-9A-Za-z][0-9A-Za-z._-]{0,79}")
_OBSERVATION_SCOPE = (
    "client-side HTTP wall time only; model-server memory is not observed by this client"
)


_VOLATILE_DETAIL_KEYS = frozenset({"wall_seconds", "endpoint"})


def strip_volatile_details(details: dict[str, Any]) -> dict[str, Any]:
    """Drop timing and endpoint so error content stays deterministic across runs."""
    return {key: value for key, value in details.items() if key not in _VOLATILE_DETAIL_KEYS}


@dataclass(frozen=True)
class SnapshotBinding:
    sha256: str
    bytes: int
    artifact_kind: str
    schema_version: int
    normalization_version: str
    thread_id: str
    source: dict[str, str]
    thread_text_sha256: str
    comment_count: int
    comments_present: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "sha256": self.sha256,
            "bytes": self.bytes,
            "artifact_kind": self.artifact_kind,
            "schema_version": self.schema_version,
            "normalization_version": self.normalization_version,
            "thread_id": self.thread_id,
            "source": dict(self.source),
            "thread_text_sha256": self.thread_text_sha256,
            "comment_count": self.comment_count,
            "comments_present": self.comments_present,
        }


@dataclass(frozen=True)
class AnnotationItem:
    state: CommentState
    status: ItemStatus
    reasons: tuple[str, ...]
    failure: ItemFailure | None
    batch_index: int | None
    usage: UsageCoverage | None
    routing: RoutingRecord | None
    answers: tuple[ValidatedAnswer, ...]

    def __post_init__(self) -> None:
        is_ok = self.status is ItemStatus.OK
        if is_ok != bool(self.answers):
            raise ValueError("an annotation item has answers exactly when its status is ok")
        if is_ok and (self.reasons or self.failure is not None):
            raise ValueError("an ok annotation item carries no reasons or failure")
        if not is_ok and not self.reasons:
            raise ValueError("a non-ok annotation item must state its reasons")

    def to_dict(self) -> dict[str, Any]:
        state = self.state
        return {
            "index": state.index,
            "comment_id": state.comment_id,
            "parent_comment_id": state.parent_comment_id,
            "comment_text_sha256": state.comment_text_sha256,
            "parent_text_sha256": state.parent_text_sha256,
            "parent_context": state.parent_context,
            "status": self.status.value,
            "reasons": list(self.reasons),
            "failure": None if self.failure is None else self.failure.to_dict(),
            "batch_index": self.batch_index,
            "state_sha256": state.state_sha256(),
            "state_chars": state.state_chars,
            "context_windows": [window.to_dict() for window in state.windows],
            "usage": None if self.usage is None else self.usage.to_dict(),
            "routing": None if self.routing is None else self.routing.to_dict(),
            "answers": [answer.to_dict() for answer in self.answers],
        }


@dataclass(frozen=True)
class ExchangeRecord:
    """One batch request as sent and the exact response text, kept apart from answers."""

    batch_index: int
    item_indexes: tuple[int, ...]
    request_body: str
    request_sha256: str
    outcome: str
    status_code: int | None
    response_body: str | None
    response_sha256: str | None
    error: dict[str, Any] | None

    @classmethod
    def from_exchange(cls, plan: BatchPlan, exchange: HttpExchange) -> ExchangeRecord:
        request_sha256 = exchange.request_sha256()
        if exchange.request_body is None or request_sha256 is None:
            raise DecisionArtifactError(
                "a batch exchange must carry the request body that was sent",
                details={"reason": "exchange_request_missing", "batch_index": plan.batch_index},
            )
        return cls(
            batch_index=plan.batch_index,
            item_indexes=plan.item_indexes,
            request_body=exchange.request_body,
            request_sha256=request_sha256,
            outcome="response",
            status_code=exchange.status_code,
            response_body=exchange.response_body,
            response_sha256=exchange.response_sha256(),
            error=None,
        )

    @classmethod
    def from_error(cls, plan: BatchPlan, error: ThreadSenseError) -> ExchangeRecord:
        request_bytes = encode_request_body(plan.body)
        error_dict = error.to_dict()
        error_dict["details"] = strip_volatile_details(error.details)
        return cls(
            batch_index=plan.batch_index,
            item_indexes=plan.item_indexes,
            request_body=request_bytes.decode("utf-8"),
            request_sha256=hashlib.sha256(request_bytes).hexdigest(),
            outcome="error",
            status_code=None,
            response_body=None,
            response_sha256=None,
            error=error_dict,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_index": self.batch_index,
            "item_indexes": list(self.item_indexes),
            "request_body": self.request_body,
            "request_sha256": self.request_sha256,
            "outcome": self.outcome,
            "status_code": self.status_code,
            "response_body": self.response_body,
            "response_sha256": self.response_sha256,
            "error": self.error,
        }


def build_decision_document(
    *,
    snapshot: SnapshotBinding,
    manifest: RuntimeManifest,
    verification: CheckpointVerification | None,
    settings: InferenceSettings,
    context_policy: ContextPolicy,
    question_set: QuestionSet,
    requirement: Requirement,
    items: Sequence[AnnotationItem],
    exchanges: Sequence[ExchangeRecord],
) -> dict[str, Any]:
    ordered_items = sorted(items, key=lambda item: item.state.index)
    ordered_exchanges = sorted(exchanges, key=lambda exchange: exchange.batch_index)
    coverage = CoverageSummary.from_statuses(item.status for item in ordered_items)
    return {
        "artifact_kind": DECISION_ARTIFACT_KIND,
        "schema_version": DECISION_ARTIFACT_SCHEMA_VERSION,
        "mode": MODE,
        "policy": POLICY,
        "calibration": CALIBRATION,
        "requirement": requirement.value,
        "input": snapshot.to_dict(),
        "backend": {
            "manifest_sha256": manifest.sha256(),
            "manifest": manifest.to_dict(),
            "code_identity_basis": "declared_by_manifest_attested_by_reuse_procedure",
            "checkpoint_identity_basis": "client_file_sha256",
            "checkpoint_verification": None if verification is None else verification.to_dict(),
        },
        "route": {
            "model": settings.model,
            "language": {"value": settings.lang, "basis": "declared"},
            "detection": None,
        },
        "questions": {"sha256": question_set.sha256(), **question_set.to_dict()},
        "settings": {"sha256": settings.sha256(), **settings.to_dict()},
        "context_policy": {"sha256": context_policy.sha256(), **context_policy.to_dict()},
        "coverage": coverage.to_dict(),
        "items": [item.to_dict() for item in ordered_items],
        "exchanges": [exchange.to_dict() for exchange in ordered_exchanges],
    }


def decision_bytes(document: dict[str, Any]) -> bytes:
    return (json.dumps(document, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode(
        "utf-8"
    )


@dataclass(frozen=True)
class RequestTiming:
    """Client-observed outcome of one HTTP call; ``batch_index`` is None for the health call."""

    batch_index: int | None
    outcome: str
    status_code: int | None
    wall_seconds: float | None


def build_run_record(
    *,
    run_id: str,
    decision_sha256: str,
    decision_relpath: str,
    endpoint: str,
    started_at_utc: float,
    completed_at_utc: float,
    health: dict[str, Any],
    requests: Sequence[RequestTiming],
    coverage: CoverageSummary,
    requirement: Requirement,
    satisfied: bool,
) -> dict[str, Any]:
    total_wall = sum(timing.wall_seconds for timing in requests if timing.wall_seconds is not None)
    return {
        "artifact_kind": DECISION_RUN_ARTIFACT_KIND,
        "schema_version": DECISION_ARTIFACT_SCHEMA_VERSION,
        "run_id": run_id,
        "decision_sha256": decision_sha256,
        "decision_path": decision_relpath,
        "endpoint": endpoint,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "health": health,
        "requests": [
            {
                "batch_index": timing.batch_index,
                "outcome": timing.outcome,
                "status_code": timing.status_code,
                "wall_seconds": timing.wall_seconds,
            }
            for timing in requests
        ],
        "total_http_wall_seconds": total_wall,
        "coverage": coverage.to_dict(),
        "requirement": requirement.value,
        "satisfied": satisfied,
        "observation_scope": _OBSERVATION_SCOPE,
    }


def require_run_id(run_id: str) -> str:
    if _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise DecisionArtifactError(
            "run id must match [0-9A-Za-z][0-9A-Za-z._-]{0,79}",
            details={"reason": "artifact_name_invalid", "field": "run_id"},
        )
    return run_id


@dataclass(frozen=True)
class AnnotationPaths:
    decision_path: Path
    run_path: Path


def annotation_paths(
    output_root: Path, snapshot_sha256: str, decision_sha256: str, run_id: str
) -> AnnotationPaths:
    named_hashes = (("snapshot_sha256", snapshot_sha256), ("decision_sha256", decision_sha256))
    for label, value in named_hashes:
        if _SHA256_PATTERN.fullmatch(value) is None:
            raise DecisionArtifactError(
                f"{label} must be 64 lowercase hex characters",
                details={"reason": "artifact_name_invalid", "field": label},
            )
    require_run_id(run_id)
    snapshot_root = output_root / snapshot_sha256
    return AnnotationPaths(
        decision_path=snapshot_root / "decisions" / f"{decision_sha256}.json",
        run_path=snapshot_root / "runs" / f"{run_id}.json",
    )


def check_output_location(
    output_root: Path, input_path: Path, protected_roots: Sequence[Path]
) -> Path:
    """Refuse output locations that could hold or overwrite baseline artifacts or the input."""
    resolved_root = output_root.resolve(strict=False)
    for protected in protected_roots:
        resolved_protected = protected.resolve(strict=False)
        if resolved_root == resolved_protected or resolved_protected in resolved_root.parents:
            raise DecisionInputError(
                "annotation output must not be placed inside baseline or monitoring storage",
                details={
                    "reason": "output_inside_protected_storage",
                    "output_root": str(resolved_root),
                    "protected_root": str(resolved_protected),
                },
            )
        if resolved_root in resolved_protected.parents:
            raise DecisionInputError(
                "annotation output must not contain baseline or monitoring storage",
                details={
                    "reason": "output_contains_protected_storage",
                    "output_root": str(resolved_root),
                    "protected_root": str(resolved_protected),
                },
            )
    if input_path.resolve(strict=False) == resolved_root:
        raise DecisionInputError(
            "annotation output root must not be the input path",
            details={"reason": "output_aliases_input", "output_root": str(resolved_root)},
        )
    if resolved_root.exists() and not resolved_root.is_dir():
        raise DecisionInputError(
            "annotation output root exists and is not a directory",
            details={"reason": "output_root_not_directory", "output_root": str(resolved_root)},
        )
    return resolved_root


def refuse_alias(path: Path, input_path: Path) -> None:
    aliased = path.resolve(strict=False) == input_path.resolve(strict=False)
    if not aliased and path.exists() and input_path.exists():
        aliased = os.path.samefile(path, input_path)
    if aliased:
        raise DecisionInputError(
            "annotation output must not alias the input artifact",
            details={"reason": "output_aliases_input", "path": str(path)},
        )


def publish_immutable(path: Path, content: bytes, *, reuse_identical: bool) -> None:
    """Publish ``content`` at ``path`` atomically without ever replacing an existing file."""
    try:
        _publish_without_clobber(path, content, reuse_identical=reuse_identical)
    except OSError as error:
        raise DecisionArtifactError(
            "annotation artifact could not be written",
            details={
                "reason": "artifact_write_failed",
                "path": str(path),
                "error_type": type(error).__name__,
                "error": str(error),
            },
        ) from error


def _publish_without_clobber(path: Path, content: bytes, *, reuse_identical: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.parent / f".{path.name}.{secrets.token_hex(8)}.tmp"
    try:
        with open(temp_path, "xb") as handle:
            handle.write(content)
            handle.flush()
            _sync_to_storage(handle.fileno())
        try:
            # A hard link never replaces an existing name, unlike rename/replace.
            os.link(temp_path, path)
        except FileExistsError:
            if reuse_identical and path.is_file() and path.read_bytes() == content:
                return
            raise DecisionArtifactError(
                "an artifact with different content already exists at the target path",
                details={"reason": "artifact_conflict", "path": str(path)},
            ) from None
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            _sync_to_storage(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temp_path.unlink(missing_ok=True)
