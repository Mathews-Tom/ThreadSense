from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
import secrets
import sys
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.config import AppConfig
from threadsense.connectors.reddit import (
    RedditConnector,
    RedditSearchMatch,
    RedditSearchRequest,
    RedditThreadRequest,
)
from threadsense.contracts import AnalysisContract, contract_from_config
from threadsense.errors import (
    AnalysisBoundaryError,
    MonitoringStateError,
    NetworkBoundaryError,
    RedditInputError,
    RedditRequestError,
    RedditResponseError,
    RunClaimError,
    SchemaBoundaryError,
    ThreadSenseError,
)
from threadsense.models.canonical import Thread, load_canonical_thread
from threadsense.monitoring.observations import (
    ObservationDiff,
    acquisition_completeness,
    build_revision_snapshot,
    comment_text_sha256,
    diff_observation,
    revision_id_for,
    substantive_sha256,
)
from threadsense.monitoring.profiles import WatchProfile
from threadsense.monitoring.state import (
    MonitoringStore,
    RunClaim,
)
from threadsense.monitoring.sweep_records import (
    AcquisitionRecord,
    Completeness,
    DiscoveredThread,
    FailureStage,
    RevisionRecord,
    RevisionRef,
    SweepOutcome,
    ThreadHistory,
    TrackedThread,
)
from threadsense.observability import (
    DEFAULT_METRICS,
    MetricsRegistry,
    TraceContext,
    emit_log,
    observe_stage,
)
from threadsense.workflows import (
    RedditConnectorFactory,
    analyze_normalized_thread,
    is_query_match,
    normalize_reddit_thread,
    research_match_sort_key,
)

WATCH_SEARCH_SORT = "new"
WATCH_SEARCH_TIME_WINDOW = "30d"
MAX_SEARCH_LIMIT_PER_REQUEST = 100
SOURCE_THREAD_ID_PATTERN = re.compile(r"[a-z0-9]{1,16}")

_CHANGE_KINDS = ("initial_observation", "new_source_event", "newly_observed_historical", "edited")
# Faults of the Reddit source itself; only these, and analyzer faults, are per-item failures.
# Store, artifact and invariant errors are never per-item: they fail the whole sweep.
_SOURCE_ERRORS: tuple[type[ThreadSenseError], ...] = (
    RedditRequestError,
    RedditResponseError,
    NetworkBoundaryError,
    RedditInputError,
)
_ANALYZER_ERRORS: tuple[type[ThreadSenseError], ...] = (AnalysisBoundaryError, SchemaBoundaryError)


class _ItemFailure(Exception):
    """A source or analyzer fault confined to one sweep item; the sweep records it and continues."""

    def __init__(self, error: ThreadSenseError) -> None:
        super().__init__(str(error))
        self.error = error


@dataclass(frozen=True)
class SweepResult:
    run_id: str
    watch_id: str
    profile_revision: int
    recovered_run_id: str | None
    outcome: SweepOutcome
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "watch_id": self.watch_id,
            "profile_revision": self.profile_revision,
            "recovered_run_id": self.recovered_run_id,
            "outcome": self.outcome,
            "summary": self.summary,
        }


@dataclass(frozen=True)
class _SweepContext:
    config: AppConfig
    store: MonitoringStore
    claim: RunClaim
    profile: WatchProfile
    root: Path
    connector: RedditConnector
    contract: AnalysisContract
    logger: logging.Logger
    trace: TraceContext
    registry: MetricsRegistry
    failures: list[dict[str, Any]]


@dataclass(frozen=True)
class _Acquisition:
    source_thread_id: str
    thread_dir: Path
    raw_path: Path
    raw_sha256: str
    acquired: Thread
    observed_at_utc: float


@dataclass(frozen=True)
class _RefreshedThread:
    diff: ObservationDiff
    completeness: Completeness
    revision_created: bool


def run_watch_sweep(
    *,
    config: AppConfig,
    store: MonitoringStore,
    watch_id: str,
    lease_seconds: int,
    connector_factory: RedditConnectorFactory,
    logger: logging.Logger,
    trace: TraceContext,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> SweepResult:
    """Run one bounded discover, refresh and analyze pass under an exclusive run claim."""
    claim = store.claim_run(watch_id, lease_seconds=lease_seconds)
    failures: list[dict[str, Any]] = []
    try:
        profile = store.load_profile_revision(watch_id, claim.profile_revision)
        sweep_config = _sweep_config(config, profile)
        ctx = _SweepContext(
            config=sweep_config,
            store=store,
            claim=claim,
            profile=profile,
            root=store.monitoring_root().absolute(),
            connector=connector_factory(sweep_config),
            contract=contract_from_config(
                sweep_config.analysis, created_at_utc=claim.claimed_at_utc
            ),
            logger=logger,
            trace=trace,
            registry=registry,
            failures=failures,
        )
        return _execute_sweep(ctx)
    except RunClaimError:
        raise
    except Exception as error:
        error_payload = _error_payload(error)
        claim_error = _fail_sweep(store, claim, failures, error_payload)
        if claim_error is not None:
            # The claim is no longer live, so nothing may be recorded for this run; the
            # original error still propagates to the caller.
            emit_log(
                logger,
                "watch_sweep_unrecorded",
                trace,
                level=logging.ERROR,
                watch_run_id=claim.run_id,
                error=error_payload,
                claim_error=claim_error.to_dict(),
            )
        raise


def _execute_sweep(ctx: _SweepContext) -> SweepResult:
    profile = ctx.profile
    discovery, searches_succeeded = _discover(ctx)
    refresh, changes = _refresh(ctx)
    analysis = _analyze(ctx)
    succeeded = searches_succeeded + refresh["fetched"] + analysis["analyzed"]
    outcome: SweepOutcome
    if ctx.failures:
        outcome = "partial" if succeeded > 0 else "failed"
    elif changes["revisions_created"] + analysis["analyzed"] > 0:
        outcome = "changes"
    else:
        outcome = "no_changes"
    summary: dict[str, Any] = {
        "profile_revision": ctx.claim.profile_revision,
        "profile_content_sha256": profile.content_sha256(),
        "discovery": discovery,
        "refresh": refresh,
        "analysis": analysis,
        "changes": changes,
        "failures": list(ctx.failures),
    }
    ctx.store.complete_sweep(ctx.claim, outcome, summary)
    emit_log(
        ctx.logger,
        "watch_sweep_completed",
        ctx.trace,
        watch_id=profile.watch_id,
        watch_run_id=ctx.claim.run_id,
        outcome=outcome,
    )
    return SweepResult(
        run_id=ctx.claim.run_id,
        watch_id=profile.watch_id,
        profile_revision=ctx.claim.profile_revision,
        recovered_run_id=ctx.claim.recovered_run_id,
        outcome=outcome,
        summary=summary,
    )


def _sweep_config(config: AppConfig, profile: WatchProfile) -> AppConfig:
    # Sweeps must observe Reddit fresh, keep artifacts at their content-addressed paths
    # and stay deterministic, so cache, versioned storage and model runtime are disabled.
    return config.model_copy(
        update={
            "cache": config.cache.model_copy(update={"enabled": False}),
            "storage": config.storage.model_copy(update={"versioning_enabled": False}),
            "runtime": config.runtime.model_copy(update={"enabled": False}),
            "analysis": config.analysis.model_copy(
                update={
                    "domain": profile.domain,
                    "objective": profile.objective,
                    "abstraction_level": profile.abstraction_level,
                }
            ),
        }
    )


def _error_payload(error: Exception) -> dict[str, Any]:
    if isinstance(error, ThreadSenseError):
        return error.to_dict()
    return {"type": type(error).__name__, "message": str(error)}


def _fail_sweep(
    store: MonitoringStore,
    claim: RunClaim,
    failures: list[dict[str, Any]],
    error_payload: dict[str, Any],
) -> RunClaimError | None:
    summary: dict[str, Any] = {
        "profile_revision": claim.profile_revision,
        "error": error_payload,
        "failures": list(failures),
    }
    try:
        store.complete_sweep(claim, "failed", summary)
    except RunClaimError as claim_error:
        return claim_error
    return None


def _record_item_failure(
    ctx: _SweepContext, stage: FailureStage, target: str, error: ThreadSenseError
) -> None:
    payload = error.to_dict()
    ctx.store.record_failure(ctx.claim, stage=stage, target=target, error=payload)
    ctx.failures.append({"stage": stage, "target": target, "error": payload})


def _discover(ctx: _SweepContext) -> tuple[dict[str, Any], int]:
    profile = ctx.profile
    limit = profile.limits.discovered_candidates
    request_count = len(profile.queries) * len(profile.subreddits)
    per_request_limit = max(1, min(MAX_SEARCH_LIMIT_PER_REQUEST, limit // request_count))
    best: dict[str, tuple[tuple[int, int, int, int, int, int, float], str, RedditSearchMatch]] = {}
    failed_queries: list[str] = []
    invalid_source_ids: set[str] = set()
    searches_succeeded = 0
    for query in profile.queries:
        request = RedditSearchRequest(
            query=query,
            subreddits=list(profile.subreddits),
            limit=per_request_limit,
            sort=WATCH_SEARCH_SORT,
            time_window=WATCH_SEARCH_TIME_WINDOW,
        )
        try:
            with _item_fault(_SOURCE_ERRORS), _stage(ctx, "watch_discover"):
                matches = ctx.connector.search_threads(request).matches
        except _ItemFailure as failure:
            failed_queries.append(query)
            _record_item_failure(ctx, "search", query, failure.error)
            continue
        searches_succeeded += 1
        for match in matches:
            if SOURCE_THREAD_ID_PATTERN.fullmatch(match.post_id) is None:
                # An id unsafe for artifact paths is never tracked.
                invalid_source_ids.add(match.post_id)
                continue
            key = research_match_sort_key(match, query)
            current = best.get(match.post_id)
            # Strictly better only: ties keep the earlier query in profile order.
            if current is None or key > current[0]:
                best[match.post_id] = (key, query, match)
    by_post_id = sorted(best.items())
    ranked = sorted(by_post_id, key=lambda item: item[1][0], reverse=True)[:limit]
    discoveries = [
        _discovered_thread(profile, rank, query, match)
        for rank, (_post_id, (_key, query, match)) in enumerate(ranked, start=1)
    ]
    newly_tracked = ctx.store.record_discoveries(ctx.claim, discoveries) if discoveries else 0
    summary: dict[str, Any] = {
        "queries": list(profile.queries),
        "subreddits": list(profile.subreddits),
        "sort": WATCH_SEARCH_SORT,
        "time_window": WATCH_SEARCH_TIME_WINDOW,
        "per_request_limit": per_request_limit,
        "limit": limit,
        "matched_count": len(best),
        "persisted_count": len(discoveries),
        "truncated": len(best) > limit,
        "selected_count": sum(1 for item in discoveries if item.selected),
        "newly_tracked_count": newly_tracked,
        "failed_queries": failed_queries,
        "invalid_source_ids": len(invalid_source_ids),
        "search_completeness": "unknown",
    }
    return summary, searches_succeeded


def _discovered_thread(
    profile: WatchProfile, rank: int, query: str, match: RedditSearchMatch
) -> DiscoveredThread:
    rejection_reason: str | None = None
    if not is_query_match(match, query):
        rejection_reason = "query_mismatch"
    else:
        haystacks = (match.title.casefold(), match.selftext.casefold())
        if any(
            term.casefold() in haystack for term in profile.exclude_terms for haystack in haystacks
        ):
            rejection_reason = "excluded_term"
    return DiscoveredThread(
        query=query,
        source_thread_id=match.post_id,
        thread_url=match.thread_url,
        subreddit=match.subreddit,
        title=match.title,
        source_created_utc=match.created_utc,
        rank=rank,
        score=match.score,
        num_comments=match.num_comments,
        selected=rejection_reason is None,
        rejection_reason=rejection_reason,
    )


def _refresh(ctx: _SweepContext) -> tuple[dict[str, Any], dict[str, int]]:
    watch_id = ctx.profile.watch_id
    limit = ctx.profile.limits.refreshed_threads
    queue = ctx.store.refresh_queue(watch_id, limit)
    tracked_threads = ctx.store.tracked_thread_count(watch_id)
    kinds: Counter[str] = Counter()
    revisions_created = thread_text_changed = absent_known = truncated = fetched = failed = 0
    for tracked in queue:
        try:
            refreshed = _refresh_thread(ctx, tracked)
        except _ItemFailure as failure:
            failed += 1
            _record_item_failure(ctx, "fetch", tracked.source_thread_id, failure.error)
            continue
        fetched += 1
        kinds.update(change.kind for change in refreshed.diff.changes)
        revisions_created += int(refreshed.revision_created)
        thread_text_changed += int(refreshed.diff.thread_text_changed)
        absent_known += len(refreshed.diff.absent_known_comment_ids)
        truncated += int(refreshed.completeness == "truncated")
    refresh: dict[str, Any] = {
        "limit": limit,
        "tracked_threads": tracked_threads,
        "attempted": len(queue),
        "fetched": fetched,
        "failed": failed,
        "pending": max(0, tracked_threads - len(queue)),
        "truncated_acquisitions": truncated,
    }
    changes = {
        "revisions_created": revisions_created,
        **{kind: kinds[kind] for kind in _CHANGE_KINDS},
        "thread_text_changed": thread_text_changed,
        "absent_known_comments": absent_known,
    }
    return refresh, changes


def _refresh_thread(ctx: _SweepContext, tracked: TrackedThread) -> _RefreshedThread:
    watch_id = ctx.profile.watch_id
    source_thread_id = _require_source_thread_id(tracked.source_thread_id)
    # One observation time per acquisition, sampled before the fetch: a comment posted while
    # the fetch is in flight is newer than this observation and so a new source event later.
    observed_at_utc = ctx.store.now()
    with _item_fault(_SOURCE_ERRORS), _stage(ctx, "watch_refresh"):
        result = ctx.connector.fetch_thread(
            RedditThreadRequest(post_url=tracked.thread_url, expand_more=True)
        )
        if result.post.id != source_thread_id:
            raise RedditResponseError(
                "fetched thread does not match the tracked thread",
                details={
                    "reason": "source_mismatch",
                    "source_thread_id": source_thread_id,
                    "fetched_source_thread_id": result.post.id,
                },
            )
    raw_artifact = result.to_dict()
    raw_bytes = json.dumps(raw_artifact, indent=2, ensure_ascii=False).encode("utf-8")
    raw_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    acquisition_id = "acq-" + raw_sha256[:32]
    thread_dir = ctx.root / "watches" / watch_id / "threads" / source_thread_id
    raw_path = thread_dir / "acquisitions" / f"{acquisition_id}.raw.json"
    normalized_path = thread_dir / "acquisitions" / f"{acquisition_id}.normalized.json"
    _publish_bytes(raw_path, raw_bytes)
    acquired = _normalize_acquisition(ctx, raw_path, raw_sha256, normalized_path)
    acquisition = _Acquisition(
        source_thread_id=source_thread_id,
        thread_dir=thread_dir,
        raw_path=raw_path,
        raw_sha256=raw_sha256,
        acquired=acquired,
        observed_at_utc=observed_at_utc,
    )
    history = ctx.store.thread_history(watch_id, source_thread_id)
    diff = diff_observation(history, acquired)
    revision = _write_revision(ctx, history, diff, acquisition) if diff.substantive else None
    completeness = acquisition_completeness(raw_artifact)
    record = AcquisitionRecord(
        acquisition_id=acquisition_id,
        source_thread_id=source_thread_id,
        observed_at_utc=observed_at_utc,
        raw_path=raw_path,
        raw_sha256=raw_sha256,
        normalized_path=normalized_path,
        normalized_sha256=_file_sha256(normalized_path),
        completeness=completeness,
        cache_status=result.cache_status,
        parsed_comment_count=result.total_comment_count,
        reported_comment_count=result.post.num_comments,
        observed_comments=tuple(
            (comment.comment_id, comment_text_sha256(comment.body), comment.created_utc)
            for comment in sorted(acquired.comments, key=lambda item: item.comment_id)
        ),
        absent_known_comment_count=len(diff.absent_known_comment_ids),
    )
    ctx.store.record_acquisition(ctx.claim, record, revision)
    return _RefreshedThread(
        diff=diff, completeness=completeness, revision_created=revision is not None
    )


def _normalize_acquisition(
    ctx: _SweepContext, raw_path: Path, raw_sha256: str, normalized_path: Path
) -> Thread:
    # Normalization stamps wall-clock time, so a retried acquisition reuses the earlier
    # normalized file when it was derived from these exact raw bytes.
    if normalized_path.exists():
        existing = load_canonical_thread(normalized_path)
        if (
            existing.provenance.raw_sha256 != raw_sha256
            or existing.provenance.raw_artifact_path != str(raw_path)
        ):
            raise _artifact_conflict(normalized_path)
        return existing
    temp_path = _temp_path(normalized_path)
    with _item_fault((SchemaBoundaryError,)):
        normalize_reddit_thread(
            config=ctx.config,
            logger=ctx.logger,
            trace=ctx.trace,
            input_path=raw_path,
            output_path=temp_path,
            registry=ctx.registry,
        )
    _publish_temp(temp_path, normalized_path)
    return load_canonical_thread(normalized_path)


def _write_revision(
    ctx: _SweepContext, history: ThreadHistory, diff: ObservationDiff, acquisition: _Acquisition
) -> RevisionRecord:
    previous = history.current_revision
    previous_snapshot: Thread | None = None
    if previous is not None:
        _verify_stored_artifact(previous.snapshot_path, previous.snapshot_sha256)
        previous_snapshot = load_canonical_thread(previous.snapshot_path)
    snapshot = build_revision_snapshot(
        previous_snapshot=previous_snapshot,
        acquired=acquisition.acquired,
        raw_artifact_path=acquisition.raw_path,
        raw_sha256=acquisition.raw_sha256,
        observed_at_utc=acquisition.observed_at_utc,
    )
    substantive = substantive_sha256(
        acquisition.source_thread_id, diff.thread_text_sha256, snapshot.comments
    )
    previous_revision_id = previous.revision_id if previous is not None else None
    revision_id = revision_id_for(
        ctx.profile.watch_id, acquisition.source_thread_id, previous_revision_id, substantive
    )
    snapshot_bytes = json.dumps(snapshot.to_dict(), indent=2, ensure_ascii=False).encode("utf-8")
    snapshot_sha256 = hashlib.sha256(snapshot_bytes).hexdigest()
    snapshot_path = acquisition.thread_dir / "revisions" / revision_id / f"{snapshot_sha256}.json"
    _publish_bytes(snapshot_path, snapshot_bytes)
    return RevisionRecord(
        revision_id=revision_id,
        previous_revision_id=previous_revision_id,
        substantive_sha256=substantive,
        thread_text_sha256=diff.thread_text_sha256,
        snapshot_path=snapshot_path,
        snapshot_sha256=snapshot_sha256,
        changes=diff.changes,
        thread_text_changed=diff.thread_text_changed,
    )


def _analyze(ctx: _SweepContext) -> dict[str, Any]:
    watch_id = ctx.profile.watch_id
    limit = ctx.profile.limits.analyzed_revisions
    analyzed = failed = 0
    for revision in ctx.store.pending_analyses(watch_id, limit):
        try:
            _analyze_revision(ctx, revision)
        except _ItemFailure as failure:
            failed += 1
            _record_item_failure(ctx, "analyze", revision.revision_id, failure.error)
            continue
        analyzed += 1
    return {
        "limit": limit,
        "analyzed": analyzed,
        "failed": failed,
        "pending": ctx.store.pending_analysis_count(watch_id),
    }


def _analyze_revision(ctx: _SweepContext, revision: RevisionRef) -> None:
    source_thread_id = _require_source_thread_id(revision.source_thread_id)
    _verify_stored_artifact(revision.snapshot_path, revision.snapshot_sha256)
    analysis_path = (
        ctx.root
        / "watches"
        / ctx.profile.watch_id
        / "threads"
        / source_thread_id
        / "analyses"
        / revision.revision_id
        / f"{ctx.claim.run_id}.json"
    )
    temp_path = _temp_path(analysis_path)
    with _item_fault(_ANALYZER_ERRORS), _stage(ctx, "watch_analyze"):
        analyze_normalized_thread(
            config=ctx.config,
            logger=ctx.logger,
            trace=ctx.trace,
            input_path=revision.snapshot_path,
            output_path=temp_path,
            contract=ctx.contract,
            registry=ctx.registry,
        )
    analysis_sha256 = _publish_temp(temp_path, analysis_path)
    ctx.store.record_analysis(ctx.claim, revision.revision_id, analysis_path, analysis_sha256)


@contextmanager
def _stage(ctx: _SweepContext, stage: str) -> Iterator[None]:
    with observe_stage(registry=ctx.registry, logger=ctx.logger, trace=ctx.trace, stage=stage):
        yield


@contextmanager
def _item_fault(error_types: tuple[type[ThreadSenseError], ...]) -> Iterator[None]:
    """Mark the listed errors raised in this block as failures of the current item only."""
    try:
        yield
    except error_types as error:
        raise _ItemFailure(error) from error


def _require_source_thread_id(source_thread_id: str) -> str:
    if SOURCE_THREAD_ID_PATTERN.fullmatch(source_thread_id) is None:
        raise MonitoringStateError(
            "source thread id is not safe to use in an artifact path",
            details={"reason": "invalid_source_id", "source_thread_id": source_thread_id},
        )
    return source_thread_id


def _temp_path(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")


def _publish_bytes(path: Path, content: bytes) -> str:
    """Write content to a content-addressed path once; an identical existing file is reused."""
    if path.exists():
        return _require_file_sha256(path, hashlib.sha256(content).hexdigest())
    temp_path = _temp_path(path)
    with temp_path.open("xb") as handle:
        handle.write(content)
    return _publish_temp(temp_path, path)


def _publish_temp(temp_path: Path, path: Path) -> str:
    with temp_path.open("rb+") as handle:
        handle.flush()
        _sync_to_storage(handle.fileno())
    sha256 = _file_sha256(temp_path)
    if path.exists():
        _require_file_sha256(path, sha256)
        temp_path.unlink()
        return sha256
    os.replace(temp_path, path)
    # The rename is durable only once the directory entry itself reaches storage.
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        _sync_to_storage(directory_fd)
    finally:
        os.close(directory_fd)
    return sha256


def _sync_to_storage(fd: int) -> None:
    # On macOS fsync only reaches the drive's volatile cache; F_FULLFSYNC flushes it.
    if sys.platform == "darwin" and hasattr(fcntl, "F_FULLFSYNC"):
        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
    else:
        os.fsync(fd)


def _require_file_sha256(path: Path, expected_sha256: str) -> str:
    if _file_sha256(path) != expected_sha256:
        raise _artifact_conflict(path)
    return expected_sha256


def _verify_stored_artifact(path: Path, expected_sha256: str) -> None:
    if not path.is_file():
        raise MonitoringStateError(
            "recorded monitoring artifact is missing",
            details={"reason": "artifact_missing", "path": str(path)},
        )
    if _file_sha256(path) != expected_sha256:
        raise MonitoringStateError(
            "recorded monitoring artifact does not match its recorded hash",
            details={"reason": "artifact_hash_mismatch", "path": str(path)},
        )


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact_conflict(path: Path) -> MonitoringStateError:
    return MonitoringStateError(
        "existing monitoring artifact does not match the content being written",
        details={"reason": "artifact_conflict", "path": str(path)},
    )
