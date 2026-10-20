"""Shadow annotation of one canonical thread with typed Laya decisions.

This module only orchestrates: it reads the canonical artifact once, plans batches, calls the
typed decision service sequentially and records what came back. It never runs analysis, never
touches the generative inference stack and never mutates comments or the input file.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.contracts.analysis_contract import ObjectiveType
from threadsense.decisions.artifact import (
    AnnotationItem,
    ExchangeRecord,
    RequestTiming,
    SnapshotBinding,
    annotation_paths,
    build_decision_document,
    build_run_record,
    check_output_location,
    decision_bytes,
    publish_immutable,
    refuse_alias,
    require_run_id,
    strip_volatile_details,
)
from threadsense.decisions.client import TypedDecisionClient
from threadsense.decisions.context import build_comment_states, plan_batches
from threadsense.decisions.contracts import (
    PINNED_CONTEXT_POLICY,
    PINNED_INFERENCE_SETTINGS,
    AggregateStatus,
    BatchPlan,
    CheckpointVerification,
    CommentState,
    ContextPolicy,
    CoverageSummary,
    InferenceSettings,
    ItemFailure,
    ItemStatus,
    QuestionSet,
    Requirement,
    RuntimeManifest,
    decode_strict_json_object,
)
from threadsense.decisions.questions import build_question_set
from threadsense.decisions.runtime import CheckpointVerifier
from threadsense.decisions.validation import (
    split_batch_results,
    validate_answers,
    validate_envelope,
    validate_health,
)
from threadsense.domains.loader import load_domain_vocabulary
from threadsense.errors import (
    DecisionIdentityError,
    DecisionInputError,
    DecisionProtocolError,
    DecisionRuntimeUnavailableError,
    ThreadSenseError,
)
from threadsense.models.canonical import Thread, canonical_thread_from_payload
from threadsense.monitoring.observations import thread_text_sha256


@dataclass(frozen=True)
class AnnotationJob:
    input_path: Path
    output_root: Path
    protected_roots: tuple[Path, ...]
    requirement: Requirement
    objective: ObjectiveType
    domain: str


@dataclass(frozen=True)
class AnnotationOutcome:
    decision_path: Path
    run_path: Path
    decision_sha256: str
    run_id: str
    snapshot_sha256: str
    coverage: CoverageSummary
    requirement: Requirement
    satisfied: bool
    question_set_sha256: str
    manifest_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_path": str(self.decision_path),
            "run_path": str(self.run_path),
            "decision_sha256": self.decision_sha256,
            "run_id": self.run_id,
            "snapshot_sha256": self.snapshot_sha256,
            "coverage": self.coverage.to_dict(),
            "requirement": self.requirement.value,
            "satisfied": self.satisfied,
            "question_set_sha256": self.question_set_sha256,
            "manifest_sha256": self.manifest_sha256,
        }


@dataclass
class _CheckpointPin:
    """The first verified checkpoint repo; a different repo later in the run is refused."""

    verification: CheckpointVerification | None = None


@dataclass(frozen=True)
class _Session:
    client: TypedDecisionClient
    verifier: CheckpointVerifier
    question_set: QuestionSet
    settings: InferenceSettings
    pin: _CheckpointPin


def _failure_from_error(error: ThreadSenseError) -> ItemFailure:
    return ItemFailure(
        reason=error.details["reason"],
        message=error.message,
        details=strip_volatile_details(error.details),
    )


def _refused_before_sending(state: CommentState) -> AnnotationItem:
    return AnnotationItem(
        state=state,
        status=ItemStatus.REFUSED,
        reasons=state.refusal_reasons,
        failure=None,
        batch_index=None,
        usage=None,
        routing=None,
        answers=(),
    )


def _unavailable(
    state: CommentState, *, reason: str, failure: ItemFailure, batch_index: int | None
) -> AnnotationItem:
    return AnnotationItem(
        state=state,
        status=ItemStatus.UNAVAILABLE,
        reasons=(reason,),
        failure=failure,
        batch_index=batch_index,
        usage=None,
        routing=None,
        answers=(),
    )


def _wall_seconds(details: dict[str, Any]) -> float | None:
    value = details.get("wall_seconds")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _payload_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise DecisionInputError(
            f"canonical input field {key} must be a string",
            details={"reason": "input_field_invalid", "field": key},
        )
    return value


def _payload_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise DecisionInputError(
            f"canonical input field {key} must be an integer",
            details={"reason": "input_field_invalid", "field": key},
        )
    return value


def _snapshot_binding(raw: bytes, payload: dict[str, Any], thread: Thread) -> SnapshotBinding:
    return SnapshotBinding(
        sha256=hashlib.sha256(raw).hexdigest(),
        bytes=len(raw),
        artifact_kind=_payload_str(payload, "artifact_kind"),
        schema_version=_payload_int(payload, "schema_version"),
        normalization_version=_payload_str(payload, "normalization_version"),
        thread_id=thread.thread_id,
        source={
            "source_name": thread.source.source_name,
            "community": thread.source.community,
            "source_thread_id": thread.source.source_thread_id,
            "thread_url": thread.source.thread_url,
        },
        thread_text_sha256=thread_text_sha256(thread.title, thread.body),
        comment_count=thread.comment_count,
        comments_present=len(thread.comments),
    )


def _read_input(input_path: Path) -> bytes:
    try:
        return input_path.read_bytes()
    except OSError as error:
        raise DecisionInputError(
            "canonical input could not be read",
            details={
                "reason": "input_unreadable",
                "path": str(input_path),
                "error_type": type(error).__name__,
            },
        ) from error


def _check_health(
    client: TypedDecisionClient, manifest: RuntimeManifest
) -> tuple[dict[str, Any], RequestTiming, ThreadSenseError | None]:
    exchange = None
    try:
        exchange = client.health()
        observation = validate_health(exchange.payload, manifest.wire)
    except (DecisionRuntimeUnavailableError, DecisionProtocolError) as error:
        wall = exchange.wall_seconds if exchange is not None else _wall_seconds(error.details)
        status = exchange.status_code if exchange is not None else None
        return error.to_dict(), RequestTiming(None, "error", status, wall), error
    timing = RequestTiming(None, "response", exchange.status_code, exchange.wall_seconds)
    return observation.to_dict(), timing, None


def _fail_batch(
    plan: BatchPlan,
    states: dict[int, CommentState],
    *,
    reason: str,
    error: ThreadSenseError,
) -> list[AnnotationItem]:
    failure = _failure_from_error(error)
    return [
        _unavailable(states[index], reason=reason, failure=failure, batch_index=plan.batch_index)
        for index in plan.item_indexes
    ]


def _coverage_item(
    state: CommentState, result_batch: int, session: _Session, raw_result: Any
) -> AnnotationItem:
    try:
        envelope = validate_envelope(raw_result, session.question_set, session.settings)
    except DecisionProtocolError as error:
        return _unavailable(
            state,
            reason="invalid_typed_answer",
            failure=_failure_from_error(error),
            batch_index=result_batch,
        )
    # Identity is checked before any answer is used, so unverified output is never promoted.
    try:
        verification = session.verifier.verify(envelope.routing.repo)
    except DecisionIdentityError as error:
        return _unavailable(
            state,
            reason="checkpoint_identity_unverified",
            failure=_failure_from_error(error),
            batch_index=result_batch,
        )
    pinned = session.pin.verification
    if pinned is None:
        session.pin.verification = verification
    elif pinned.repo != verification.repo:
        failure = ItemFailure(
            "checkpoint_repo_changed",
            "the service answered from a different checkpoint repo than earlier in this run",
            {"first_repo": pinned.repo, "repo": verification.repo},
        )
        return _unavailable(
            state, reason="checkpoint_repo_changed", failure=failure, batch_index=result_batch
        )
    reasons = envelope.usage.refusal_reasons()
    if reasons:
        # Coverage refusal wins over answer content: the raw answers stay only in the exchange.
        return AnnotationItem(
            state=state,
            status=ItemStatus.REFUSED,
            reasons=reasons,
            failure=None,
            batch_index=result_batch,
            usage=envelope.usage,
            routing=envelope.routing,
            answers=(),
        )
    try:
        answers = validate_answers(envelope.raw_answers, session.question_set)
    except DecisionProtocolError as error:
        return _unavailable(
            state,
            reason="invalid_typed_answer",
            failure=_failure_from_error(error),
            batch_index=result_batch,
        )
    return AnnotationItem(
        state=state,
        status=ItemStatus.OK,
        reasons=(),
        failure=None,
        batch_index=result_batch,
        usage=envelope.usage,
        routing=envelope.routing,
        answers=answers,
    )


def _send_batch(
    plan: BatchPlan, states: dict[int, CommentState], session: _Session
) -> tuple[list[AnnotationItem], ExchangeRecord, RequestTiming]:
    try:
        exchange = session.client.post_batch(plan.body)
    except (DecisionRuntimeUnavailableError, DecisionProtocolError) as error:
        reason = (
            "runtime_request_failed"
            if isinstance(error, DecisionRuntimeUnavailableError)
            else "invalid_response"
        )
        timing = RequestTiming(plan.batch_index, "error", None, _wall_seconds(error.details))
        return (
            _fail_batch(plan, states, reason=reason, error=error),
            ExchangeRecord.from_error(plan, error),
            timing,
        )
    record = ExchangeRecord.from_exchange(plan, exchange)
    timing = RequestTiming(
        plan.batch_index, "response", exchange.status_code, exchange.wall_seconds
    )
    try:
        results = split_batch_results(exchange.payload, len(plan.item_indexes))
    except DecisionProtocolError as error:
        return _fail_batch(plan, states, reason="invalid_response", error=error), record, timing
    # Results are matched to items strictly by position, never by content.
    items = [
        _coverage_item(states[index], plan.batch_index, session, result)
        for index, result in zip(plan.item_indexes, results, strict=True)
    ]
    return items, record, timing


def _unavailable_runtime_items(
    plans: tuple[BatchPlan, ...], states: dict[int, CommentState], error: ThreadSenseError
) -> list[AnnotationItem]:
    items: list[AnnotationItem] = []
    for plan in plans:
        items.extend(_fail_batch(plan, states, reason="runtime_health_unavailable", error=error))
    return items


def _ordered_items(
    states: tuple[CommentState, ...], produced: list[AnnotationItem]
) -> list[AnnotationItem]:
    by_index = {item.state.index: item for item in produced}
    for state in states:
        if state.state is None:
            by_index[state.index] = _refused_before_sending(state)
    missing = [state.index for state in states if state.index not in by_index]
    if missing:
        raise DecisionInputError(
            "batch plan did not cover every sendable comment",
            details={"reason": "plan_incomplete", "missing_indexes": missing},
        )
    return [by_index[state.index] for state in states]


def _require_settings_match(settings: InferenceSettings, manifest: RuntimeManifest) -> None:
    if settings.model != manifest.wire.model or settings.lang != manifest.wire.language:
        raise DecisionInputError(
            "inference settings do not match the runtime manifest wire contract",
            details={
                "reason": "settings_manifest_mismatch",
                "settings_model": settings.model,
                "settings_lang": settings.lang,
                "manifest_model": manifest.wire.model,
                "manifest_lang": manifest.wire.language,
            },
        )


def _default_run_id(started_at_utc: float) -> str:
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(started_at_utc))
    return f"{stamp}-{secrets.token_hex(6)}"


def _run_record_bytes(record: dict[str, Any]) -> bytes:
    return (json.dumps(record, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode(
        "utf-8"
    )


def annotate_canonical_thread(
    job: AnnotationJob,
    *,
    client: TypedDecisionClient,
    manifest: RuntimeManifest,
    verifier: CheckpointVerifier,
    settings: InferenceSettings = PINNED_INFERENCE_SETTINGS,
    context_policy: ContextPolicy = PINNED_CONTEXT_POLICY,
    wall_clock: Callable[[], float] = time.time,
    run_id: str | None = None,
) -> AnnotationOutcome:
    _require_settings_match(settings, manifest)
    if run_id is not None:
        require_run_id(run_id)
    output_root = check_output_location(job.output_root, job.input_path, job.protected_roots)
    # The input is read exactly once; every later step works from these bytes.
    raw = _read_input(job.input_path)
    payload = decode_strict_json_object(raw, DecisionInputError, "canonical input")
    thread = canonical_thread_from_payload(payload)
    snapshot = _snapshot_binding(raw, payload, thread)
    question_set = build_question_set(job.objective, load_domain_vocabulary(job.domain))
    comment_states = build_comment_states(thread, context_policy)
    plans = plan_batches(comment_states, question_set, settings)
    states = {state.index: state for state in comment_states}

    started_at_utc = wall_clock()
    health, health_timing, health_error = _check_health(client, manifest)
    timings = [health_timing]
    exchanges: list[ExchangeRecord] = []
    produced: list[AnnotationItem] = []
    session = _Session(client, verifier, question_set, settings, _CheckpointPin())
    if health_error is not None:
        produced = _unavailable_runtime_items(plans, states, health_error)
    else:
        for plan in plans:
            batch_items, record, timing = _send_batch(plan, states, session)
            produced.extend(batch_items)
            exchanges.append(record)
            timings.append(timing)
    items = _ordered_items(comment_states, produced)
    completed_at_utc = wall_clock()

    coverage = CoverageSummary.from_statuses(item.status for item in items)
    # A required run needs a healthy runtime even when no comment was sendable.
    satisfied = job.requirement is Requirement.OPTIONAL or (
        health_error is None and coverage.status is AggregateStatus.COMPLETE
    )
    document = build_decision_document(
        snapshot=snapshot,
        manifest=manifest,
        verification=session.pin.verification,
        settings=settings,
        context_policy=context_policy,
        question_set=question_set,
        requirement=job.requirement,
        items=items,
        exchanges=exchanges,
    )
    content = decision_bytes(document)
    decision_sha256 = hashlib.sha256(content).hexdigest()
    resolved_run_id = run_id if run_id is not None else _default_run_id(started_at_utc)
    paths = annotation_paths(output_root, snapshot.sha256, decision_sha256, resolved_run_id)
    refuse_alias(paths.decision_path, job.input_path)
    refuse_alias(paths.run_path, job.input_path)
    run_record = build_run_record(
        run_id=resolved_run_id,
        decision_sha256=decision_sha256,
        decision_relpath=paths.decision_path.relative_to(output_root).as_posix(),
        endpoint=client.endpoint,
        started_at_utc=started_at_utc,
        completed_at_utc=completed_at_utc,
        health=health,
        requests=timings,
        coverage=coverage,
        requirement=job.requirement,
        satisfied=satisfied,
    )
    publish_immutable(paths.decision_path, content, reuse_identical=True)
    publish_immutable(paths.run_path, _run_record_bytes(run_record), reuse_identical=False)
    return AnnotationOutcome(
        decision_path=paths.decision_path,
        run_path=paths.run_path,
        decision_sha256=decision_sha256,
        run_id=resolved_run_id,
        snapshot_sha256=snapshot.sha256,
        coverage=coverage,
        requirement=job.requirement,
        satisfied=satisfied,
        question_set_sha256=question_set.sha256(),
        manifest_sha256=manifest.sha256(),
    )
