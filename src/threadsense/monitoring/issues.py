"""Issue evidence ingestion: derive report inputs from verified artifacts and record them.

Each pending finding entry is re-read from its store-recorded snapshot and analysis
artifacts. Both are hash-verified before use, and every cited comment must still match the
text hash the digest recorded. Links and update cues are extracted lexically; cues are
recorded as unverified claims.
"""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlsplit, urlunsplit

from threadsense.config import IssuesConfig
from threadsense.errors import MonitoringStateError
from threadsense.models.analysis import AnalysisFinding, load_analysis_artifact_file
from threadsense.models.canonical import Comment, Thread, load_canonical_thread
from threadsense.monitoring.issue_records import (
    UPDATE_CUES,
    UPDATE_KINDS,
    IssueEntryInput,
    IssueIngestion,
    IssuePolicy,
    IssueReportInput,
    PendingIssueEntry,
    UpdateKind,
)
from threadsense.monitoring.observations import comment_text_sha256
from threadsense.monitoring.state import MonitoringStore, RunClaim
from threadsense.monitoring.sweep import _verify_stored_artifact
from threadsense.pipeline.strategies.keyword_heuristic import _URL_PATTERN

_LINK_TRAILING_CHARS = ".,;:!?)]}>'\""
_UNATTRIBUTED_USERNAMES = frozenset({"[deleted]", "[removed]"})
_CUE_PATTERNS: tuple[tuple[UpdateKind, tuple[tuple[str, re.Pattern[str]], ...]], ...] = tuple(
    (
        kind,
        tuple(
            (cue, re.compile(r"(?<![a-z0-9])" + re.escape(cue) + r"(?![a-z0-9])"))
            for cue in UPDATE_CUES[kind]
        ),
    )
    for kind in UPDATE_KINDS
)


def issue_policy(config: IssuesConfig) -> IssuePolicy:
    """Build the versioned grouping policy from the configured candidate bounds."""
    return IssuePolicy(
        candidate_window_days=config.candidate_window_days,
        max_candidate_comparisons=config.max_candidate_comparisons,
        max_suggestions_per_thread=config.max_suggestions_per_thread,
    )


def extract_links(body: str) -> tuple[str, ...]:
    """Return the sorted unique normalized http(s) links cited in a comment body."""
    links: set[str] = set()
    for match in _URL_PATTERN.finditer(body):
        candidate = match.group(0).rstrip(_LINK_TRAILING_CHARS)
        if candidate.startswith("www."):
            candidate = "https://" + candidate
        try:
            parts = urlsplit(candidate)
        except ValueError:
            # Malformed authority (for example an unbalanced IPv6 bracket): not a usable link.
            continue
        if not parts.netloc:
            continue
        path = parts.path
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        links.add(urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, parts.query, "")))
    return tuple(sorted(links))


def detect_update_cues(body: str) -> tuple[tuple[UpdateKind, str], ...]:
    """Return at most one `(kind, cue)` per update kind, in `UPDATE_KINDS` order."""
    text = " ".join(body.replace("’", "'").casefold().split())
    cues: list[tuple[UpdateKind, str]] = []
    for kind, patterns in _CUE_PATTERNS:
        for cue, pattern in patterns:
            if pattern.search(text):
                cues.append((kind, cue))
                break
    return tuple(cues)


def account_sha256(source_name: str, username: str) -> str | None:
    """Hash a source account; `None` when the comment has no attributable account."""
    if not username.strip() or username in _UNATTRIBUTED_USERNAMES:
        return None
    return hashlib.sha256(f"{source_name}\n{username.casefold()}".encode()).hexdigest()


def build_issue_entry_input(pending: PendingIssueEntry) -> IssueEntryInput:
    """Read one pending finding entry's verified artifacts into an issue entry input."""
    _verify_stored_artifact(pending.snapshot_path, pending.snapshot_sha256)
    _verify_stored_artifact(pending.analysis_path, pending.analysis_sha256)
    snapshot = load_canonical_thread(pending.snapshot_path)
    analysis = load_analysis_artifact_file(pending.analysis_path)
    if snapshot.source.source_thread_id != pending.source_thread_id:
        raise MonitoringStateError(
            "issue entry snapshot belongs to a different thread",
            details={
                "reason": "issue_evidence_mismatch",
                "entry_id": pending.entry_id,
                "source_thread_id": pending.source_thread_id,
                "snapshot_source_thread_id": snapshot.source.source_thread_id,
            },
        )
    finding = _entry_finding(pending, analysis.findings)
    comments = {comment.comment_id: comment for comment in snapshot.comments}
    reports = [
        _report_input(
            pending,
            snapshot,
            comments,
            (str(evidence["comment_id"]), str(evidence["text_sha256"])),
        )
        for evidence in pending.novel_evidence
    ]
    reports.sort(key=lambda report: (report.source_created_utc, report.comment_id))
    return IssueEntryInput(
        entry_id=pending.entry_id,
        source_thread_id=pending.source_thread_id,
        revision_id=pending.revision_id,
        snapshot_sha256=pending.snapshot_sha256,
        theme_key=pending.theme_key,
        key_phrases=tuple(finding.key_phrases),
        reports=tuple(reports),
    )


def ingest_issue_evidence(
    store: MonitoringStore, claim: RunClaim, policy: IssuePolicy
) -> IssueIngestion:
    """Record every pending finding entry of the claimed watch into issue history."""
    pending = store.pending_issue_entries(claim.watch_id)
    inputs = [build_issue_entry_input(entry) for entry in pending]
    return store.record_issue_evidence(claim, inputs, policy)


def _entry_finding(pending: PendingIssueEntry, findings: list[AnalysisFinding]) -> AnalysisFinding:
    matches = [finding for finding in findings if finding.theme_key == pending.theme_key]
    if len(matches) != 1:
        raise MonitoringStateError(
            "issue entry theme does not resolve to exactly one analysis finding",
            details={
                "reason": "issue_finding_missing",
                "entry_id": pending.entry_id,
                "source_thread_id": pending.source_thread_id,
                "theme_key": pending.theme_key,
                "matching_findings": len(matches),
            },
        )
    return matches[0]


def _report_input(
    pending: PendingIssueEntry,
    snapshot: Thread,
    comments: dict[str, Comment],
    cited: tuple[str, str],
) -> IssueReportInput:
    comment_id, text_sha256 = cited
    comment = comments.get(comment_id)
    if comment is None:
        raise MonitoringStateError(
            "issue evidence comment is missing from the recorded snapshot",
            details={
                "reason": "issue_evidence_missing",
                "entry_id": pending.entry_id,
                "source_thread_id": pending.source_thread_id,
                "comment_id": comment_id,
            },
        )
    if comment_text_sha256(comment.body) != text_sha256:
        raise MonitoringStateError(
            "issue evidence comment text does not match its recorded hash",
            details={
                "reason": "issue_evidence_mismatch",
                "entry_id": pending.entry_id,
                "source_thread_id": pending.source_thread_id,
                "comment_id": comment_id,
            },
        )
    return IssueReportInput(
        comment_id=comment_id,
        text_sha256=text_sha256,
        content_chars=len(" ".join(comment.body.split())),
        author_sha256=account_sha256(snapshot.source.source_name, comment.author.username),
        community=snapshot.source.community,
        permalink=comment.permalink,
        source_created_utc=comment.created_utc,
        links=extract_links(comment.body),
        cues=detect_update_cues(comment.body),
    )
