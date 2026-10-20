"""Materialize analyzed evidence into digest entries and present it as durable digests."""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from threadsense.errors import MonitoringStateError, RunClaimError
from threadsense.models.analysis import AnalysisFinding, load_analysis_artifact_file
from threadsense.models.canonical import Comment, load_canonical_thread
from threadsense.monitoring.digest_records import (
    DigestEntryRecord,
    MaterializationInput,
    PendingDigest,
    finding_entry_id,
    thread_update_entry_id,
)
from threadsense.monitoring.explain import rank_digest
from threadsense.monitoring.issue_records import IssuePolicy
from threadsense.monitoring.issues import ingest_issue_evidence
from threadsense.monitoring.state import MonitoringStore, RunClaim
from threadsense.monitoring.sweep import _error_payload, _publish_bytes, _verify_stored_artifact
from threadsense.observability import TraceContext, emit_log

DigestStatus = Literal["presented", "no_entries"]
DIGEST_ARTIFACT_TYPE = "watch_digest"
# Digests recorded with a ranking basis render schema 3. Digests recorded before rankings
# existed (state schema < 7) keep rendering schema 2, and before issue views existed (state
# schema < 5) schema 1, so a resume reproduces their bytes.
DIGEST_ARTIFACT_SCHEMA_VERSION = 3
DIGEST_ARTIFACT_PRE_RANKING_SCHEMA_VERSION = 2
DIGEST_ARTIFACT_PRE_ISSUE_SCHEMA_VERSION = 1
EXCERPT_MAX_CHARS = 280
_CHANGE_KINDS = ("initial_observation", "new_source_event", "newly_observed_historical", "edited")
_MARKDOWN_SPECIAL = frozenset("\\`*_[]<>#|")


@dataclass(frozen=True)
class DigestResult:
    run_id: str
    watch_id: str
    status: DigestStatus
    digest_id: str | None
    sequence: int | None
    resumed: bool
    json_path: Path | None
    markdown_path: Path | None
    entry_ids: tuple[str, ...]
    materialized_entries: int
    backlog: dict[str, Any] | None
    issues: dict[str, Any]
    ranking: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "watch_id": self.watch_id,
            "status": self.status,
            "digest_id": self.digest_id,
            "sequence": self.sequence,
            "resumed": self.resumed,
            "json_path": None if self.json_path is None else str(self.json_path),
            "markdown_path": None if self.markdown_path is None else str(self.markdown_path),
            "entry_ids": list(self.entry_ids),
            "materialized_entries": self.materialized_entries,
            "backlog": self.backlog,
            "issues": self.issues,
            "ranking": self.ranking,
        }


def build_digest_entries(watch_id: str, item: MaterializationInput) -> list[DigestEntryRecord]:
    """Derive the thread update and novel-evidence finding entries of one analyzed revision."""
    revision = item.revision
    _verify_stored_artifact(revision.snapshot_path, revision.snapshot_sha256)
    _verify_stored_artifact(item.analysis_path, item.analysis_sha256)
    snapshot = load_canonical_thread(revision.snapshot_path)
    analysis = load_analysis_artifact_file(item.analysis_path)
    comments = {comment.comment_id: comment for comment in snapshot.comments}
    novel = [
        _novel_comment(revision.revision_id, change, comments)
        for change in sorted(item.novel_comments, key=lambda change: str(change["comment_id"]))
    ]
    thread_fields: dict[str, Any] = {
        "watch_id": watch_id,
        "source_thread_id": revision.source_thread_id,
        "thread_url": item.thread_url,
        "subreddit": item.subreddit,
        "title": item.title,
        "revision_id": revision.revision_id,
        "revision_sequence": revision.sequence,
        "snapshot_path": revision.snapshot_path.relative_to(item.monitoring_root).as_posix(),
        "snapshot_sha256": revision.snapshot_sha256,
        "analysis_path": item.analysis_path.relative_to(item.monitoring_root).as_posix(),
        "analysis_sha256": item.analysis_sha256,
        "thread_text_changed": item.thread_text_changed,
    }
    kinds = Counter(str(comment["kind"]) for comment in novel)
    update_id = thread_update_entry_id(watch_id, revision.revision_id)
    entries = [
        DigestEntryRecord(
            entry_id=update_id,
            source_thread_id=revision.source_thread_id,
            revision_id=revision.revision_id,
            kind="thread_update",
            theme_key=None,
            payload={
                "kind": "thread_update",
                "entry_id": update_id,
                **thread_fields,
                "novel_comments": novel,
                "counts": {kind: kinds[kind] for kind in _CHANGE_KINDS},
            },
        )
    ]
    novel_by_id = {str(comment["comment_id"]): comment for comment in novel}
    for finding in analysis.findings:
        entry = _finding_entry(watch_id, finding, thread_fields, novel_by_id)
        if entry is not None:
            entries.append(entry)
    return entries


def _novel_comment(
    revision_id: str, change: dict[str, Any], comments: dict[str, Comment]
) -> dict[str, Any]:
    comment = comments.get(str(change["comment_id"]))
    if comment is None:
        raise MonitoringStateError(
            "revision snapshot lacks a comment its changes cite",
            details={
                "reason": "snapshot_comment_missing",
                "revision_id": revision_id,
                "comment_id": change["comment_id"],
            },
        )
    excerpt = " ".join(comment.body.split())[:EXCERPT_MAX_CHARS]
    return {**change, "permalink": comment.permalink, "excerpt": excerpt}


def _finding_entry(
    watch_id: str,
    finding: AnalysisFinding,
    thread_fields: dict[str, Any],
    novel_by_id: dict[str, dict[str, Any]],
) -> DigestEntryRecord | None:
    evidence_ids = sorted(set(finding.evidence_comment_ids) & novel_by_id.keys())
    if not evidence_ids:
        return None
    revision_id = str(thread_fields["revision_id"])
    entry_id = finding_entry_id(watch_id, revision_id, finding.theme_key)
    return DigestEntryRecord(
        entry_id=entry_id,
        source_thread_id=str(thread_fields["source_thread_id"]),
        revision_id=revision_id,
        kind="finding",
        theme_key=finding.theme_key,
        payload={
            "kind": "finding",
            "entry_id": entry_id,
            **thread_fields,
            "theme_key": finding.theme_key,
            "theme_label": finding.theme_label,
            "severity": finding.severity,
            "finding_comment_count": finding.comment_count,
            "novel_evidence_comment_ids": evidence_ids,
            "novel_evidence": [novel_by_id[comment_id] for comment_id in evidence_ids],
        },
    )


def rank_pending_digest(
    store: MonitoringStore, watch_id: str, digest: PendingDigest
) -> PendingDigest:
    """Materialize the digest's order from its recorded basis; older digests stay unranked."""
    if digest.ranking_basis is None:
        return digest
    ranking = rank_digest(store, watch_id, digest.entries, digest.issue_view, digest.ranking_basis)
    return replace(digest, ranking=ranking)


def _required_ranking(digest: PendingDigest) -> dict[str, Any] | None:
    if digest.ranking_basis is not None and digest.ranking is None:
        raise MonitoringStateError(
            "digest ranking was not materialized from its recorded basis",
            details={"reason": "ranking_not_materialized", "digest_id": digest.digest_id},
        )
    return digest.ranking


def render_digest_json(watch_id: str, digest: PendingDigest) -> bytes:
    """Render the digest artifact from the recorded digest only; no clock is read."""
    issue_view = digest.issue_view
    ranking = _required_ranking(digest)
    schema_version = DIGEST_ARTIFACT_SCHEMA_VERSION
    if ranking is None:
        schema_version = DIGEST_ARTIFACT_PRE_RANKING_SCHEMA_VERSION
    if issue_view is None:
        schema_version = DIGEST_ARTIFACT_PRE_ISSUE_SCHEMA_VERSION
    payload: dict[str, Any] = {
        "artifact_type": DIGEST_ARTIFACT_TYPE,
        "schema_version": schema_version,
        "watch_id": watch_id,
        "digest_id": digest.digest_id,
        "sequence": digest.sequence,
        "created_at_utc": digest.created_at_utc,
        "entries": list(digest.entries),
        "backlog": digest.backlog,
    }
    if issue_view is not None:
        payload["issues"] = issue_view
    if ranking is not None:
        payload["ranking"] = ranking
    text = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
    return (text + "\n").encode("utf-8")


def render_digest_markdown(watch_id: str, digest: PendingDigest) -> bytes:
    """Render the human digest from the recorded digest only; source text is escaped."""
    backlog = digest.backlog
    lines = [
        f"# Watch digest {_md(watch_id)} #{digest.sequence}",
        "",
        f"- Digest: {_md(digest.digest_id)}",
        f"- Created: {_utc(digest.created_at_utc)}",
        f"- Entries: {len(digest.entries)}",
        "",
        "## Backlog",
        "",
        f"- Presentation limit: {backlog['presentation_limit']}",
        f"- Presented in this digest: {backlog['presented_in_digest']}",
        f"- Unpresented entries after this digest: {backlog['unpresented_entries_after']}",
        f"- Pending analyses: {backlog['pending_analyses']}",
        f"- Tracked threads: {backlog['tracked_threads']}",
        f"- Threads pending refresh: {_or_unknown(backlog['refresh_pending'])}",
        f"- Latest sweep outcome: {_or_unknown(backlog['latest_sweep_outcome'])}",
        f"- Search completeness: {_md(str(backlog['search_completeness']))}",
    ]
    issue_view = digest.issue_view
    ranking = _required_ranking(digest)
    refs: dict[str, dict[str, Any]] = {}
    if issue_view is not None:
        lines.extend(["", *_issues_lines(issue_view)])
        refs = {str(ref["entry_id"]): ref for ref in issue_view["entry_refs"]}
    entries = list(digest.entries)
    ranked: dict[str, dict[str, Any]] = {}
    if ranking is not None:
        lines.extend(["", *_ranking_lines(ranking)])
        ranked = {str(item["entry_id"]): item for item in ranking["order"]}
        by_id = {str(entry["entry_id"]): entry for entry in entries}
        # Presentation follows the ranked order; the entries themselves are unchanged.
        entries = [by_id[str(item["entry_id"])] for item in ranking["order"]]
    for index, entry in enumerate(entries, start=1):
        entry_id = str(entry["entry_id"])
        block = _entry_lines(index, entry, refs.get(entry_id))
        if entry_id in ranked:
            # Rank lines follow the heading, before the entry's facts and quoted evidence.
            block[2:2] = _rank_lines(ranked[entry_id])
        lines.extend(["", *block])
    return ("\n".join(lines) + "\n").encode("utf-8")


def _entry_lines(index: int, entry: dict[str, Any], ref: dict[str, Any] | None) -> list[str]:
    if entry["kind"] == "finding":
        heading = (
            f"## {index}. Finding: {_md(str(entry['theme_label']))} ({_md(str(entry['severity']))})"
        )
        comments = entry["novel_evidence"]
    else:
        heading = f"## {index}. Thread update: {_md(str(entry['title']))}"
        comments = entry["novel_comments"]
    lines = [
        heading,
        "",
        f"- Entry: {_md(str(entry['entry_id']))}",
        f"- Thread: {_md(str(entry['thread_url']))} (r/{_md(str(entry['subreddit']))})",
        f"- Revision: {_md(str(entry['revision_id']))} (sequence {entry['revision_sequence']})",
        *_entry_issue_lines(ref),
        f"- Thread text changed: {'yes' if entry['thread_text_changed'] else 'no'}",
    ]
    if entry["kind"] == "finding":
        lines.append(f"- Comments in finding: {entry['finding_comment_count']}")
    lines.append(f"- Novel comments: {len(comments)}")
    for comment in comments:
        lines.extend(_comment_lines(comment))
    return lines


def _ranking_lines(ranking: dict[str, Any]) -> list[str]:
    basis = ranking["basis"]
    return [
        "## Ranking",
        "",
        f"- Policy: {_md(str(basis['policy_name']))} ({_md(str(basis['policy_id']))})",
        f"- Objective: {_words(basis['objective'])}; profile revision {basis['profile_revision']}",
        f"- Preferences applied up to sequence {basis['preference_sequence']}",
        f"- Semantic component: {_words(ranking['semantic']['status'])}; contributes no points",
        "- Ranking orders presentation only; evidence, counts and corrections are unchanged.",
    ]


def _rank_lines(item: dict[str, Any]) -> list[str]:
    scored = [component for component in item["components"] if component["points"] is not None]
    parts = "; ".join(f"{component['component']} {component['points']:+d}" for component in scored)
    lines = [f"- Rank {item['rank']}: score {item['score']} ({parts})"]
    for component in scored:
        if component["component"] == "preference" and component["matched"]:
            lines.append(f"- Preference: {_md(str(component['reason']))}")
    return lines


def _issues_lines(issue_view: dict[str, Any]) -> list[str]:
    grouping = _words(issue_view["semantic_grouping"])
    lines = [
        "## Issues",
        "",
        f"- Semantic grouping: {grouping}; suggestions are unconfirmed and never merge "
        "memberships or counts.",
    ]
    for issue in issue_view["issues"]:
        lines.extend(["", *_issue_lines(issue)])
    return lines


def _issue_lines(issue: dict[str, Any]) -> list[str]:
    counts = issue["counts"]
    updates = issue["update_counts"]
    members = ", ".join(_md(str(thread_id)) for thread_id in issue["member_thread_ids"])
    lines = [
        f"### Issue {_md(str(issue['issue_id']))} ({_words(issue['state'])}, open)",
        "",
        f"- Member threads: {members}",
        f"- Firsthand reports: {counts['firsthand_reports']}; "
        f"exact copies: {counts['exact_copies']}",
        f"- Observed threads: {counts['observed_threads']}; "
        f"accounts: {counts['observed_accounts']}; "
        f"communities: {counts['observed_communities']}; "
        f"independence: {_words(counts['independence']['status'])}",
        f"- Updates: reproduction {updates['reproduction']}, "
        f"contradiction {updates['contradiction']}, workaround {updates['workaround']}, "
        f"resolution claims {updates['resolution_claim']} (unverified; issue not resolved)",
    ]
    lines.extend(_suggestion_lines(issue["suggestions"]))
    return lines


def _suggestion_lines(suggestions: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for suggestion in suggestions:
        pair = (
            f"{_md(str(suggestion['source_thread_id']))} and "
            f"{_md(str(suggestion['candidate_thread_id']))}"
        )
        if suggestion["status"] == "refused":
            lines.append(f"- Refused grouping: {pair} ({_words(suggestion['refusal_reason'])})")
        elif not suggestion["currently_grouped"]:
            signals = suggestion["signals"]
            lines.append(
                f"- Unconfirmed suggestion: {pair} (themes: {_md_list(signals['themes'])}; "
                f"key phrases: {_md_list(signals['key_phrases'])})"
            )
    return lines


def _entry_issue_lines(ref: dict[str, Any] | None) -> list[str]:
    if ref is None:
        return []
    lines = [f"- Issue: {_md(str(ref['issue_id']))}"]
    for update in ref["updates"]:
        lines.append(
            f"- Update: {_md(str(update['kind']))} in {_md(str(update['comment_id']))} "
            f'(cue "{_md(str(update["cue"]))}", unverified)'
        )
    for report in ref["reports"]:
        if report["lineage"] == "exact_copy":
            lines.append(
                f"- Exact copy: {_md(str(report['comment_id']))} "
                f"copies {_md(str(report['copy_of_report_id']))}"
            )
    return lines


def _words(value: object) -> str:
    """Render a stored snake_case token as escaped words."""
    return _md(str(value).replace("_", " "))


def _md_list(values: list[Any]) -> str:
    return ", ".join(_md(str(value)) for value in values) if values else "none"


def _comment_lines(comment: dict[str, Any]) -> list[str]:
    if comment["kind"] == "edited":
        observed = f"edit observed {_utc(float(comment['observed_at_utc']))}"
    else:
        observed = f"first observed {_utc(float(comment['first_observed_at_utc']))}"
    return [
        "",
        f"### {_md(str(comment['comment_id']))}: {_md(str(comment['kind']))}, {observed}",
        "",
        f"- Permalink: {_md(str(comment['permalink']))}",
        f"- Source created: {_utc(float(comment['source_created_utc']))}",
        "",
        f"> {_md(str(comment['excerpt']))}",
    ]


def _md(text: str) -> str:
    """Quote one line of source text, escaping structural Markdown delimiters."""
    collapsed = " ".join(text.split())
    return "".join(f"\\{char}" if char in _MARKDOWN_SPECIAL else char for char in collapsed)


def _or_unknown(value: object) -> str:
    return "unknown" if value is None else _md(str(value))


def _utc(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def present_watch_digest(
    *,
    store: MonitoringStore,
    watch_id: str,
    lease_seconds: int,
    issue_policy: IssuePolicy,
    logger: logging.Logger,
    trace: TraceContext,
) -> DigestResult:
    """Materialize new entries and present the next digest under an exclusive run claim."""
    claim = store.claim_run(watch_id, lease_seconds=lease_seconds)
    try:
        result = _present(store, claim, issue_policy)
    except RunClaimError:
        raise
    except Exception as error:
        error_payload = _error_payload(error)
        claim_error = _fail_digest_run(store, claim)
        emit_log(
            logger,
            "watch_digest_failed" if claim_error is None else "watch_digest_unrecorded",
            trace,
            level=logging.ERROR,
            watch_run_id=claim.run_id,
            error=error_payload,
            claim_error=None if claim_error is None else claim_error.to_dict(),
        )
        raise
    store.finalize_run(claim.run_id, claim.owner_token, "succeeded")
    emit_log(
        logger,
        "watch_digest_completed",
        trace,
        watch_id=watch_id,
        watch_run_id=claim.run_id,
        status=result.status,
        digest_id=result.digest_id,
        resumed=result.resumed,
    )
    return result


def _present(store: MonitoringStore, claim: RunClaim, policy: IssuePolicy) -> DigestResult:
    watch_id = claim.watch_id
    profile = store.load_profile_revision(watch_id, claim.profile_revision)
    materialized = 0
    for item in store.unmaterialized_revisions(watch_id):
        entries = build_digest_entries(watch_id, item)
        materialized += store.materialize_digest_entries(claim, entries)
    ingestion = ingest_issue_evidence(store, claim, policy)
    pending = store.begin_digest(claim, profile.limits.presented_findings)
    if pending is None:
        return DigestResult(
            run_id=claim.run_id,
            watch_id=watch_id,
            status="no_entries",
            digest_id=None,
            sequence=None,
            resumed=False,
            json_path=None,
            markdown_path=None,
            entry_ids=(),
            materialized_entries=materialized,
            backlog=None,
            issues=ingestion.to_dict(),
            ranking=None,
        )
    digest = rank_pending_digest(store, watch_id, pending)
    json_bytes = render_digest_json(watch_id, digest)
    with store.fenced_artifact_write(claim):
        json_sha256 = _publish_bytes(digest.json_path, json_bytes)
    markdown_bytes = render_digest_markdown(watch_id, digest)
    with store.fenced_artifact_write(claim):
        markdown_sha256 = _publish_bytes(digest.markdown_path, markdown_bytes)
    store.complete_digest(claim, digest.digest_id, json_sha256, markdown_sha256)
    return DigestResult(
        run_id=claim.run_id,
        watch_id=watch_id,
        status="presented",
        digest_id=digest.digest_id,
        sequence=digest.sequence,
        resumed=digest.resumed,
        json_path=digest.json_path,
        markdown_path=digest.markdown_path,
        entry_ids=tuple(str(entry["entry_id"]) for entry in digest.entries),
        materialized_entries=materialized,
        backlog=digest.backlog,
        issues=ingestion.to_dict(),
        ranking=_ranking_summary(digest.ranking),
    )


def _ranking_summary(ranking: dict[str, Any] | None) -> dict[str, Any] | None:
    if ranking is None:
        return None
    return {
        **ranking["basis"],
        "ranked_entry_ids": [item["entry_id"] for item in ranking["order"]],
    }


def _fail_digest_run(store: MonitoringStore, claim: RunClaim) -> RunClaimError | None:
    try:
        store.finalize_run(claim.run_id, claim.owner_token, "failed")
    except RunClaimError as claim_error:
        return claim_error
    return None
