"""Bounded comment-local states and request batches for typed decisions.

Context fields are prefix windows; the comment text itself is never windowed or trimmed,
so an oversized comment is refused before sending instead of being silently shortened.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from threadsense.decisions import contracts
from threadsense.decisions.contracts import (
    BatchPlan,
    CommentState,
    ContextPolicy,
    ContextWindow,
    InferenceSettings,
    QuestionSet,
)
from threadsense.errors import DecisionInputError
from threadsense.models.canonical import Comment, Thread
from threadsense.monitoring.observations import comment_text_sha256

REFUSAL_COMMENT_BUDGET = "comment_exceeds_client_char_budget"
REFUSAL_STATE_LIMIT = "state_exceeds_server_char_limit"


def _window(field: str, text: str, limit: int) -> tuple[str, ContextWindow]:
    included = text[:limit]
    return included, ContextWindow(
        field=field, source_chars=len(text), included_chars=len(included)
    )


def _index_comments(thread: Thread) -> dict[str, Comment]:
    by_id: dict[str, Comment] = {}
    for comment in thread.comments:
        if comment.comment_id in by_id:
            raise DecisionInputError(
                f"duplicate comment id {comment.comment_id}",
                {
                    "reason": "duplicate_comment_id",
                    "thread_id": thread.thread_id,
                    "comment_id": comment.comment_id,
                },
            )
        by_id[comment.comment_id] = comment
    return by_id


def _build_state(
    thread: Thread,
    comment: Comment,
    parent: Comment | None,
    policy: ContextPolicy,
) -> tuple[dict[str, str], tuple[ContextWindow, ...]]:
    title, title_window = _window("thread_title", thread.title, policy.title_window_chars)
    state: dict[str, str] = {"comment": comment.body, "thread_title": title}
    windows = [title_window]
    if isinstance(thread.body, str) and thread.body:
        body, body_window = _window("thread_body", thread.body, policy.body_window_chars)
        state["thread_body"] = body
        windows.append(body_window)
    if parent is not None:
        text, parent_window = _window("parent_comment", parent.body, policy.parent_window_chars)
        state["parent_comment"] = text
        windows.append(parent_window)
    return state, tuple(windows)


def _comment_state(
    index: int,
    thread: Thread,
    comment: Comment,
    by_id: dict[str, Comment],
    policy: ContextPolicy,
) -> CommentState:
    parent: Comment | None = None
    parent_context = "none"
    if comment.parent_comment_id is not None:
        parent = by_id.get(comment.parent_comment_id)
        parent_context = "included" if parent is not None else "missing_from_snapshot"

    def make(
        state: dict[str, str] | None,
        state_chars: int,
        windows: tuple[ContextWindow, ...],
        refusal_reasons: tuple[str, ...],
    ) -> CommentState:
        return CommentState(
            index=index,
            comment_id=comment.comment_id,
            parent_comment_id=comment.parent_comment_id,
            comment_text_sha256=comment_text_sha256(comment.body),
            parent_text_sha256=None if parent is None else comment_text_sha256(parent.body),
            parent_context=parent_context,
            state=state,
            state_chars=state_chars,
            windows=windows,
            refusal_reasons=refusal_reasons,
        )

    if len(comment.body) > policy.comment_max_chars:
        return make(None, 0, (), (REFUSAL_COMMENT_BUDGET,))
    state, windows = _build_state(thread, comment, parent, policy)
    # Same measure as the server: default json.dumps separators, non-ASCII kept.
    state_chars = len(json.dumps(state, ensure_ascii=False))
    if state_chars > contracts.MAX_STATE_CHARS:
        return make(None, state_chars, windows, (REFUSAL_STATE_LIMIT,))
    return make(state, state_chars, windows, ())


def build_comment_states(thread: Thread, policy: ContextPolicy) -> tuple[CommentState, ...]:
    by_id = _index_comments(thread)
    return tuple(
        _comment_state(index, thread, comment, by_id, policy)
        for index, comment in enumerate(thread.comments)
    )


def _request_body(
    settings: InferenceSettings,
    wire_questions: dict[str, Any],
    states: Sequence[dict[str, str]],
) -> dict[str, Any]:
    # Key order is fixed; sort_by_length, batch_size and min_confidence are never sent.
    return {**settings.to_wire(), "questions": wire_questions, "states": list(states)}


def plan_batches(
    states: Sequence[CommentState],
    question_set: QuestionSet,
    settings: InferenceSettings,
) -> tuple[BatchPlan, ...]:
    wire_questions: dict[str, Any] = dict(question_set.wire_questions())

    def fits(chunk: Sequence[CommentState]) -> bool:
        payload = [item.state for item in chunk if item.state is not None]
        body = _request_body(settings, wire_questions, payload)
        return len(contracts.encode_request_body(body)) <= contracts.MAX_BODY_BYTES

    plans: list[BatchPlan] = []
    chunk: list[CommentState] = []

    def flush() -> None:
        payload = [item.state for item in chunk if item.state is not None]
        plans.append(
            BatchPlan(
                batch_index=len(plans),
                item_indexes=tuple(item.index for item in chunk),
                body=_request_body(settings, wire_questions, payload),
            )
        )
        chunk.clear()

    for item in (item for item in states if item.state is not None):
        if not fits([item]):
            raise DecisionInputError(
                f"state for comment {item.comment_id} exceeds the request body limit",
                {
                    "reason": "state_exceeds_request_body_limit",
                    "comment_id": item.comment_id,
                    "max_body_bytes": contracts.MAX_BODY_BYTES,
                },
            )
        if chunk and (len(chunk) >= contracts.MAX_BATCH_STATES or not fits([*chunk, item])):
            flush()
        chunk.append(item)
    if chunk:
        flush()
    return tuple(plans)
