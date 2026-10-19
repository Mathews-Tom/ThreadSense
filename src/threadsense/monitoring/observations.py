from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from threadsense.errors import MonitoringStateError
from threadsense.models.canonical import (
    CANONICAL_NORMALIZATION_VERSION,
    CANONICAL_SCHEMA_VERSION,
    Comment,
    ProvenanceMetadata,
    Thread,
)
from threadsense.monitoring.sweep_records import (
    ChangeKind,
    CommentChange,
    Completeness,
    ThreadHistory,
)


@dataclass(frozen=True)
class ObservationDiff:
    """Changes of one acquisition measured against durable per-comment history.

    Known comments missing from the acquisition are reported as absent, never as
    deletions: Reddit listings omit comments for many reasons besides removal.
    """

    changes: tuple[CommentChange, ...]
    absent_known_comment_ids: tuple[str, ...]
    thread_text_sha256: str
    thread_text_changed: bool
    first_acquisition: bool

    @property
    def substantive(self) -> bool:
        # The first acquisition is the baseline revision even when the thread has no comments.
        return bool(self.changes) or self.thread_text_changed or self.first_acquisition


def comment_text_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def thread_text_sha256(title: str, body: str | None) -> str:
    encoded = json.dumps([title, body], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def diff_observation(history: ThreadHistory, acquired: Thread) -> ObservationDiff:
    first_acquisition = history.prior_observed_at_utc is None
    if first_acquisition != (history.current_revision is None):
        raise MonitoringStateError(
            "thread history has an acquisition without a revision or vice versa",
            details={
                "reason": "inconsistent_history",
                "source_thread_id": history.source_thread_id,
            },
        )
    acquired_by_id = _comments_by_id(acquired)
    changes: list[CommentChange] = []
    for comment_id in sorted(acquired_by_id):
        change = _classify_comment(history, acquired_by_id[comment_id])
        if change is not None:
            changes.append(change)
    current_text_sha256 = thread_text_sha256(acquired.title, acquired.body)
    thread_text_changed = (
        history.current_revision is not None
        and history.current_revision.thread_text_sha256 != current_text_sha256
    )
    return ObservationDiff(
        changes=tuple(changes),
        absent_known_comment_ids=tuple(sorted(set(history.known_comments) - set(acquired_by_id))),
        thread_text_sha256=current_text_sha256,
        thread_text_changed=thread_text_changed,
        first_acquisition=first_acquisition,
    )


def substantive_sha256(
    source_thread_id: str, thread_text_sha256: str, comments: Sequence[Comment]
) -> str:
    payload = {
        "source_thread_id": source_thread_id,
        "thread_text_sha256": thread_text_sha256,
        "comments": [
            [comment.comment_id, comment.parent_comment_id, comment_text_sha256(comment.body)]
            for comment in sorted(comments, key=lambda item: item.comment_id)
        ],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def revision_id_for(
    watch_id: str,
    source_thread_id: str,
    previous_revision_id: str | None,
    substantive_sha256: str,
) -> str:
    chain = f"{watch_id}\n{source_thread_id}\n{previous_revision_id or ''}\n{substantive_sha256}"
    return "rev-" + hashlib.sha256(chain.encode("utf-8")).hexdigest()[:32]


def build_revision_snapshot(
    *,
    previous_snapshot: Thread | None,
    acquired: Thread,
    raw_artifact_path: Path,
    raw_sha256: str,
    observed_at_utc: float,
) -> Thread:
    """Union of every known comment: current text where acquired, last snapshot otherwise."""
    comments = dict(_comments_by_id(acquired))
    if previous_snapshot is not None:
        for comment in previous_snapshot.comments:
            comments.setdefault(comment.comment_id, comment)
    ordered = sorted(comments.values(), key=lambda item: (item.created_utc, item.comment_id))
    return replace(
        acquired,
        comments=ordered,
        comment_count=len(ordered),
        provenance=ProvenanceMetadata(
            raw_artifact_path=str(raw_artifact_path),
            raw_sha256=raw_sha256,
            retrieved_at_utc=acquired.provenance.retrieved_at_utc,
            normalized_at_utc=observed_at_utc,
            schema_version=CANONICAL_SCHEMA_VERSION,
            normalization_version=CANONICAL_NORMALIZATION_VERSION,
        ),
    )


def acquisition_completeness(raw_artifact: Mapping[str, Any]) -> Completeness:
    """`truncated` when any `more` placeholder names a comment the payloads never returned.

    Absence of placeholders does not prove completeness, so the other answer is `unknown`.
    """
    payloads = [raw_artifact["raw_thread_payload"], raw_artifact["raw_morechildren_payloads"]]
    returned_ids: set[str] = set()
    placeholder_ids: set[str] = set()
    for kind, data in _things(payloads):
        if kind == "t1" and isinstance(data.get("id"), str):
            returned_ids.add(data["id"])
        elif kind == "more" and isinstance(data.get("children"), list):
            placeholder_ids.update(child for child in data["children"] if isinstance(child, str))
    return "truncated" if placeholder_ids - returned_ids else "unknown"


def _things(node: Any) -> Iterator[tuple[str, Mapping[str, Any]]]:
    stack: list[Any] = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, list):
            stack.extend(current)
            continue
        if not isinstance(current, dict):
            continue
        kind = current.get("kind")
        data = current.get("data")
        if isinstance(kind, str) and isinstance(data, dict):
            yield kind, data
        stack.extend(current.values())


def _comments_by_id(thread: Thread) -> dict[str, Comment]:
    comments: dict[str, Comment] = {}
    for comment in thread.comments:
        if comment.comment_id in comments:
            raise MonitoringStateError(
                "acquired thread contains a duplicate comment id",
                details={
                    "reason": "duplicate_comment_id",
                    "source_thread_id": thread.source.source_thread_id,
                    "comment_id": comment.comment_id,
                },
            )
        comments[comment.comment_id] = comment
    return comments


def _classify_comment(history: ThreadHistory, comment: Comment) -> CommentChange | None:
    text_sha256 = comment_text_sha256(comment.body)
    known = history.known_comments.get(comment.comment_id)
    if known is not None:
        if known.text_sha256 == text_sha256:
            return None
        return CommentChange(
            comment_id=comment.comment_id,
            kind="edited",
            text_sha256=text_sha256,
            previous_text_sha256=known.text_sha256,
            source_created_utc=known.source_created_utc,
        )
    kind: ChangeKind
    if history.prior_observed_at_utc is None:
        kind = "initial_observation"
    elif comment.created_utc > history.prior_observed_at_utc:
        kind = "new_source_event"
    else:
        kind = "newly_observed_historical"
    return CommentChange(
        comment_id=comment.comment_id,
        kind=kind,
        text_sha256=text_sha256,
        previous_text_sha256=None,
        source_created_utc=comment.created_utc,
    )
