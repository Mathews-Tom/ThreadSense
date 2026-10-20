"""Comparators placed beside typed decisions during an offline assessment.

The keyword heuristic maps its existing rules onto typed theme options. The existing
local-generative reclassification task supports the bounded theme cohort only; its
acquisition and frozen offline replay live in generative.py.
"""

from __future__ import annotations

from typing import Any

import httpx

from threadsense.config import RuntimeConfig
from threadsense.decisions.client import validate_loopback_endpoint
from threadsense.decisions.contracts import (
    OTHER_OR_UNCERTAIN,
    ChoiceQuestion,
    QuestionSet,
    decode_strict_json_object,
)
from threadsense.decisions.evaluation_contracts import HeuristicDecision
from threadsense.domains.vocabulary import DomainVocabulary
from threadsense.errors import (
    DecisionEvaluationError,
    DecisionInputError,
    DecisionProtocolError,
)
from threadsense.models.canonical import Comment, Thread
from threadsense.pipeline.strategies.keyword_heuristic import (
    CommentSignal,
    build_comment_signal,
    classify_theme,
    is_noise_signal,
)

HEURISTIC_COMPARATOR_NAME = "keyword_heuristic"
HEURISTIC_QUESTION_ID = "theme"
HEURISTIC_BASES = (
    "keyword_hits",
    "issue_marker_fallback",
    "request_marker_fallback",
    "default_theme",
    "noise_filtered",
    "empty_text",
)
GENERATIVE_COMPARATOR_NAME = "local_openai_compatible"

_THEME_SHAPE_REASON = "supported_theme_intersection"
_THEME_SHAPE_DETAIL = (
    "existing catch_all_reclassification with thread title and complete comment bodies "
    "of at most 300 characters; default and new themes abstain"
)
_NO_TASK_REASON = "no_existing_task"
_NO_TASK_DETAIL = "no existing local-generative inference task answers this question"


def _rule_basis(signal: CommentSignal) -> str:
    # Mirrors the precedence of classify_theme: keyword hits, issue, request, default.
    if max(signal.theme_hits.values()) > 0:
        return "keyword_hits"
    if signal.issue_marker_count > 0:
        return "issue_marker_fallback"
    if signal.request_marker_count > 0:
        return "request_marker_fallback"
    return "default_theme"


def _map_theme(theme: str, vocabulary: DomainVocabulary, question: ChoiceQuestion) -> str:
    if theme == vocabulary.default_theme:
        return OTHER_OR_UNCERTAIN
    if theme in question.labels:
        return theme
    raise DecisionEvaluationError(
        "keyword heuristic produced a theme that is not a typed option",
        {
            "reason": "heuristic_theme_outside_options",
            "theme": theme,
            "options": list(question.labels),
        },
    )


def _comment_decision(
    comment: Comment, vocabulary: DomainVocabulary, question: ChoiceQuestion
) -> HeuristicDecision:
    signal = build_comment_signal(
        comment,
        theme_rules=vocabulary.theme_rules,
        issue_markers=vocabulary.issue_markers,
        request_markers=vocabulary.request_markers,
    )
    if signal is None:
        return HeuristicDecision(comment.comment_id, None, "empty_text", None)
    if is_noise_signal(signal):
        return HeuristicDecision(comment.comment_id, None, "noise_filtered", None)
    theme = classify_theme(
        signal,
        issue_fallback_theme=vocabulary.issue_fallback_theme,
        request_fallback_theme=vocabulary.request_fallback_theme,
        default_theme=vocabulary.default_theme,
    )
    return HeuristicDecision(
        comment_id=comment.comment_id,
        label=_map_theme(theme, vocabulary, question),
        basis=_rule_basis(signal),
        heuristic_theme=theme,
    )


def heuristic_theme_decisions(
    thread: Thread, vocabulary: DomainVocabulary, question: ChoiceQuestion
) -> tuple[HeuristicDecision, ...]:
    """One heuristic decision per comment, in thread order."""
    return tuple(_comment_decision(comment, vocabulary, question) for comment in thread.comments)


def generative_shape_support(question_set: QuestionSet) -> list[dict[str, Any]]:
    support: list[dict[str, Any]] = []
    for question in question_set.questions:
        is_theme = question.question_id == HEURISTIC_QUESTION_ID
        support.append(
            {
                "question_id": question.question_id,
                "supported": is_theme,
                "reason": _THEME_SHAPE_REASON if is_theme else _NO_TASK_REASON,
                "detail": _THEME_SHAPE_DETAIL if is_theme else _NO_TASK_DETAIL,
            }
        )
    return support


def _listing_ids(raw: bytes) -> list[str] | None:
    """Model ids of a strict `{"data": [{"id": str, ...}, ...]}` listing, else None."""
    payload = decode_strict_json_object(raw, DecisionProtocolError, "model listing")
    entries = payload.get("data")
    if not isinstance(entries, list):
        return None
    ids: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("id"), str):
            return None
        ids.append(entry["id"])
    return ids


def _probe_listing(
    url: str, transport: httpx.BaseTransport | None, timeout: float
) -> httpx.Response:
    with httpx.Client(
        transport=transport, trust_env=False, timeout=timeout, follow_redirects=False
    ) as client:
        return client.get(url)


def _configured_generative_availability(runtime: RuntimeConfig) -> dict[str, Any]:
    """Build the configured record and apply no-HTTP configuration guards."""
    observation: dict[str, Any] = {
        "provider": GENERATIVE_COMPARATOR_NAME,
        "enabled": runtime.enabled,
        "base_url": runtime.base_url,
        "model": runtime.model,
        "method": None,
        "status": "unavailable",
        "reason": None,
        "served_models": None,
    }
    if not runtime.enabled:
        return {
            **observation,
            "status": "not_observed",
            "method": None,
            "reason": "disabled_by_configuration",
        }
    try:
        validate_loopback_endpoint(runtime.base_url)
    except DecisionInputError as error:
        return {
            **observation,
            "status": "not_observed",
            "method": None,
            "reason": error.details["reason"],
        }
    return observation


def observe_generative_availability(
    runtime: RuntimeConfig,
    *,
    transport: httpx.BaseTransport | None = None,
    timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    """Observe whether the configured local generative model is served; never generates."""
    observation = _configured_generative_availability(runtime)
    if observation["status"] == "not_observed":
        return observation
    base = runtime.base_url.rstrip("/")
    observation = {**observation, "method": f"GET {base}/v1/models"}
    try:
        response = _probe_listing(f"{base}/v1/models", transport, timeout_seconds)
    except httpx.HTTPError as error:
        return {
            **observation,
            "reason": "runtime_unreachable",
            "error_type": type(error).__name__,
        }
    if response.status_code != 200:
        return {
            **observation,
            "reason": "listing_http_status",
            "status_code": response.status_code,
        }
    try:
        ids = _listing_ids(response.content)
    except (DecisionProtocolError, ValueError, OverflowError, RecursionError):
        return {**observation, "reason": "listing_invalid"}
    if ids is None:
        return {**observation, "reason": "listing_invalid"}
    served = {**observation, "served_models": sorted(ids)}
    if runtime.model not in ids:
        return {**served, "reason": "configured_model_not_served"}
    return {**served, "status": "available"}
