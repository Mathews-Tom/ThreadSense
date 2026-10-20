"""Pure validation of decoded typed-decision responses.

The server's own choice, score and confidence fields are checked for internal consistency
within the upstream four-decimal rounding and are never recomputed, coerced or dropped.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from threadsense.decisions.contracts import (
    ROUNDING_QUANTUM,
    TOLERANCE_EPSILON,
    ChoiceQuestion,
    ConfidenceQuantity,
    HealthObservation,
    InferenceSettings,
    NoulQuestion,
    Question,
    QuestionSet,
    QuestionType,
    RoutingRecord,
    ScoreQuestion,
    UsageCoverage,
    ValidatedAnswer,
    ValidatedResult,
    WireContract,
)
from threadsense.errors import DecisionProtocolError

_CONFIDENCE_TOLERANCE = ROUNDING_QUANTUM + TOLERANCE_EPSILON
_RESULT_KEYS = frozenset({"model", "answers", "usage", "routing"})
_ROUTING_KEYS = frozenset({"model", "repo", "reason", "detection", "workflow"})
_USAGE_COUNT_KEYS = ("input_tokens", "output_tokens", "state_tokens", "state_tokens_dropped")
_USAGE_KEYS = frozenset({*_USAGE_COUNT_KEYS, "truncated", "truncated_questions", "options"})
_USAGE_REQUIRED = _USAGE_KEYS - {"options"}
_OPTIONAL_RAW_EVIDENCE = frozenset({"action", "x_jev_confidence"})
_CHOICE_REQUIRED = frozenset({"type", "choice", "probabilities", "confidence", "answer_confidence"})
_SCORE_REQUIRED = frozenset(
    {"type", "score", "legend", "probabilities", "confidence", "answer_confidence"}
)
_NOUL_REQUIRED = frozenset({"type", "noul", "confidence", "answer_confidence"})
_ENTROPY_DEFINITION = (
    "normalized-entropy concentration of the option distribution; uncalibrated and not on "
    "the answer_confidence scale"
)


def _fail(reason: str, message: str, **details: Any) -> DecisionProtocolError:
    return DecisionProtocolError(message, {"reason": reason, **details})


def _is_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    return isinstance(value, int) or (isinstance(value, float) and math.isfinite(value))


def _is_probability(value: Any) -> bool:
    return _is_number(value) and 0 <= value <= 1


def _is_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _require_keys(
    value: Mapping[str, Any],
    required: frozenset[str],
    optional: frozenset[str],
    *,
    reason: str,
    unknown_reason: str | None = None,
    **context: Any,
) -> None:
    missing = sorted(required - set(value))
    unexpected = sorted(set(value) - required - optional)
    if missing:
        raise _fail(
            reason, "response object is missing required fields", missing=missing, **context
        )
    if unexpected:
        raise _fail(
            unknown_reason or reason,
            "response object carries unknown fields",
            unexpected=unexpected,
            **context,
        )


def validate_health(payload: dict[str, Any], wire: WireContract) -> HealthObservation:
    status = payload.get("status")
    if status != "ok":
        raise _fail("health_not_ready", "service health is not ok", observed=status)
    loaded = payload.get("loaded")
    if not isinstance(loaded, list) or not all(isinstance(name, str) for name in loaded):
        raise _fail("health_invalid", "health loaded must be a list of strings", field="loaded")
    if wire.model not in loaded:
        raise _fail(
            "health_model_not_loaded",
            "pinned model is not loaded",
            expected=wire.model,
            observed=loaded,
        )
    revisions = payload.get("revisions")
    if not isinstance(revisions, dict) or wire.model not in revisions:
        raise _fail("health_invalid", "health revisions lack the pinned model", field="revisions")
    revision = revisions[wire.model]
    if revision is not None and not isinstance(revision, str):
        raise _fail("health_invalid", "health revision must be a string or null", field="revisions")
    if "device" not in payload or not isinstance(payload["device"], str | None):
        raise _fail("health_invalid", "health device must be a string or null", field="device")
    return HealthObservation(
        status="ok",
        loaded=tuple(loaded),
        revision=revision,
        device=payload["device"],
        payload=payload,
    )


def _usage_sums(results: list[Any]) -> tuple[int, int] | None:
    input_tokens = 0
    output_tokens = 0
    for result in results:
        usage = result.get("usage") if isinstance(result, dict) else None
        if not isinstance(usage, dict):
            return None
        if not _is_count(usage.get("input_tokens")) or not _is_count(usage.get("output_tokens")):
            return None
        input_tokens += usage["input_tokens"]
        output_tokens += usage["output_tokens"]
    return input_tokens, output_tokens


def split_batch_results(payload: dict[str, Any], expected_count: int) -> list[Any]:
    """Return results in request order; association with items is strictly by index."""
    _require_keys(
        payload, frozenset({"results", "total_usage"}), frozenset(), reason="batch_shape_invalid"
    )
    results = payload["results"]
    if not isinstance(results, list):
        raise _fail("batch_shape_invalid", "batch results must be a list", field="results")
    if len(results) != expected_count:
        raise _fail(
            "result_count_mismatch",
            "batch result count differs from the request",
            expected=expected_count,
            observed=len(results),
        )
    total = payload["total_usage"]
    if not isinstance(total, dict):
        raise _fail("batch_shape_invalid", "total_usage must be an object", field="total_usage")
    _require_keys(
        total,
        frozenset({"input_tokens", "output_tokens"}),
        frozenset(),
        reason="batch_shape_invalid",
        field="total_usage",
    )
    if not _is_count(total["input_tokens"]) or not _is_count(total["output_tokens"]):
        raise _fail(
            "batch_shape_invalid",
            "total_usage counts must be non-negative integers",
            field="total_usage",
        )
    sums = _usage_sums(results)
    if sums is not None and sums != (total["input_tokens"], total["output_tokens"]):
        raise _fail(
            "total_usage_mismatch",
            "total_usage differs from the per-result sums",
            expected=list(sums),
            observed=[total["input_tokens"], total["output_tokens"]],
        )
    return results


def _validate_routing(
    value: Any, response_model: str, settings: InferenceSettings
) -> RoutingRecord:
    if not isinstance(value, dict):
        raise _fail("result_shape_invalid", "routing must be an object", field="routing")
    _require_keys(value, _ROUTING_KEYS, frozenset(), reason="result_shape_invalid", field="routing")
    if value["model"] != settings.model:
        raise _fail(
            "routing_model_mismatch",
            "routed model differs from the explicit request",
            expected=settings.model,
            observed=value["model"],
        )
    expected_reason = f"explicit model={settings.model!r}"
    if value["reason"] != expected_reason:
        raise _fail(
            "routing_not_explicit",
            "routing was not an explicit model selection",
            expected=expected_reason,
            observed=value["reason"],
        )
    if value["detection"] is not None:
        raise _fail("routing_language_detected", "service detected a language", field="detection")
    if value["workflow"] is not None:
        raise _fail("routing_workflow_present", "service applied a workflow", field="workflow")
    repo = value["repo"]
    if not isinstance(repo, str) or not PurePosixPath(repo).is_absolute():
        raise _fail("routing_repo_invalid", "routing repo must be an absolute path", observed=repo)
    return RoutingRecord(
        response_model=response_model, model=settings.model, repo=repo, reason=expected_reason
    )


def _validate_usage(value: Any, question_ids: tuple[str, ...]) -> UsageCoverage:
    if not isinstance(value, dict):
        raise _fail("result_shape_invalid", "usage must be an object", field="usage")
    unknown = sorted(set(value) - _USAGE_KEYS)
    if unknown:
        raise _fail("usage_unknown_field", "usage carries unknown fields", unexpected=unknown)
    missing = sorted(_USAGE_REQUIRED - set(value))
    if missing:
        raise _fail(
            "result_shape_invalid", "usage is missing fields", field="usage", missing=missing
        )
    for key in _USAGE_COUNT_KEYS:
        if not _is_count(value[key]):
            raise _fail(
                "result_shape_invalid", "usage count must be a non-negative integer", field=key
            )
    truncated = value["truncated"]
    truncated_questions = value["truncated_questions"]
    if not isinstance(truncated, bool):
        raise _fail("result_shape_invalid", "usage.truncated must be a bool", field="truncated")
    if not isinstance(truncated_questions, list) or any(
        not isinstance(item, str) or item not in question_ids for item in truncated_questions
    ):
        raise _fail(
            "result_shape_invalid",
            "usage.truncated_questions must list requested question ids",
            field="truncated_questions",
        )
    return UsageCoverage(
        input_tokens=value["input_tokens"],
        output_tokens=value["output_tokens"],
        state_tokens=value["state_tokens"],
        state_tokens_dropped=value["state_tokens_dropped"],
        truncated=truncated,
        truncated_questions=tuple(truncated_questions),
        options_reported="options" in value,
    )


def _probability_field(value: Any, question_id: str, field: str) -> float:
    if not _is_probability(value):
        raise _fail(
            "probability_invalid",
            "value must be a finite number within [0, 1]",
            question_id=question_id,
            field=field,
            observed=value,
        )
    return float(value)


def _distribution(
    raw: Any, labels: tuple[str, ...], question_id: str, order_reason: str
) -> tuple[tuple[str, float], ...]:
    if not isinstance(raw, dict) or list(raw) != list(labels):
        raise _fail(
            order_reason,
            "probability keys differ from the requested order",
            question_id=question_id,
            expected=list(labels),
            observed=list(raw) if isinstance(raw, dict) else raw,
        )
    pairs = tuple(
        (label, _probability_field(raw[label], question_id, f"probabilities.{label}"))
        for label in labels
    )
    mass = sum(probability for _label, probability in pairs)
    if abs(mass - 1) > len(labels) * ROUNDING_QUANTUM + TOLERANCE_EPSILON:
        raise _fail(
            "probability_mass_out_of_tolerance",
            "probabilities do not sum to one within rounding",
            question_id=question_id,
            observed=mass,
        )
    return pairs


def _check_close(
    value: float, target: float, reason: str, question_id: str, field: str, tolerance: float
) -> None:
    if abs(value - target) > tolerance:
        raise _fail(
            reason,
            "reported confidence is inconsistent with the distribution",
            question_id=question_id,
            field=field,
            expected=target,
            observed=value,
        )


def _entropy_quantity(value: float) -> ConfidenceQuantity:
    return ConfidenceQuantity("confidence", value, _ENTROPY_DEFINITION)


def _validate_choice(answer: dict[str, Any], question: ChoiceQuestion) -> ValidatedAnswer:
    qid = question.question_id
    _require_keys(
        answer,
        _CHOICE_REQUIRED,
        _OPTIONAL_RAW_EVIDENCE,
        reason="answer_shape_invalid",
        unknown_reason="answer_unknown_field",
        question_id=qid,
    )
    pairs = _distribution(
        answer["probabilities"], question.labels, qid, "choice_option_order_mismatch"
    )
    choice = answer["choice"]
    if not isinstance(choice, str) or choice not in question.labels:
        raise _fail(
            "choice_unknown", "choice is not a requested option", question_id=qid, observed=choice
        )
    probabilities = dict(pairs)
    if probabilities[choice] != max(probabilities.values()):
        raise _fail(
            "choice_not_maximal",
            "reported choice is not a most probable option",
            question_id=qid,
            observed=choice,
        )
    confidence = _probability_field(answer["confidence"], qid, "confidence")
    answer_confidence = _probability_field(answer["answer_confidence"], qid, "answer_confidence")
    _check_close(
        answer_confidence,
        probabilities[choice],
        "answer_confidence_inconsistent",
        qid,
        "answer_confidence",
        _CONFIDENCE_TOLERANCE,
    )
    return ValidatedAnswer(
        question_id=qid,
        question_type=QuestionType.CHOICE,
        choice=choice,
        score=None,
        noul=None,
        probabilities=pairs,
        confidence_quantities=(
            ConfidenceQuantity(
                "answer_confidence",
                answer_confidence,
                "reported probability of the selected option (max p); uncalibrated",
            ),
            _entropy_quantity(confidence),
        ),
    )


def _validate_legend(legend: Any, question: ScoreQuestion) -> tuple[str, ...]:
    keys = tuple(str(index) for index in range(len(question.levels)))
    if (
        not isinstance(legend, dict)
        or list(legend) != list(keys)
        or [legend[key] for key in keys] != list(question.levels)
    ):
        raise _fail(
            "score_legend_mismatch",
            "score legend differs from the requested levels",
            question_id=question.question_id,
        )
    return keys


def _validate_score(answer: dict[str, Any], question: ScoreQuestion) -> ValidatedAnswer:
    qid = question.question_id
    _require_keys(
        answer,
        _SCORE_REQUIRED,
        _OPTIONAL_RAW_EVIDENCE,
        reason="answer_shape_invalid",
        unknown_reason="answer_unknown_field",
        question_id=qid,
    )
    keys = _validate_legend(answer["legend"], question)
    pairs = _distribution(answer["probabilities"], keys, qid, "score_level_order_mismatch")
    top = len(keys) - 1
    score = answer["score"]
    if not _is_number(score) or not 0 <= score <= top:
        raise _fail(
            "score_out_of_range",
            "score is outside the level range",
            question_id=qid,
            observed=score,
        )
    expected = sum(index * probability for index, (_key, probability) in enumerate(pairs))
    tolerance = (top * (top + 1) / 2 + 1) * ROUNDING_QUANTUM + TOLERANCE_EPSILON
    if abs(score - expected) > tolerance:
        raise _fail(
            "score_inconsistent_with_distribution",
            "score differs from the expected level of the distribution",
            question_id=qid,
            expected=expected,
            observed=score,
        )
    confidence = _probability_field(answer["confidence"], qid, "confidence")
    answer_confidence = _probability_field(answer["answer_confidence"], qid, "answer_confidence")
    _check_close(
        answer_confidence,
        max(probability for _key, probability in pairs),
        "answer_confidence_inconsistent",
        qid,
        "answer_confidence",
        _CONFIDENCE_TOLERANCE,
    )
    return ValidatedAnswer(
        question_id=qid,
        question_type=QuestionType.SCORE,
        choice=None,
        score=float(score),
        noul=None,
        probabilities=pairs,
        confidence_quantities=(
            ConfidenceQuantity(
                "answer_confidence",
                answer_confidence,
                "probability of the most likely level (max p); uncalibrated; the score itself "
                "is an expected level, not a probability",
            ),
            _entropy_quantity(confidence),
        ),
    )


def _validate_noul(answer: dict[str, Any], question: NoulQuestion) -> ValidatedAnswer:
    qid = question.question_id
    _require_keys(
        answer,
        _NOUL_REQUIRED,
        frozenset({"action"}),
        reason="answer_shape_invalid",
        unknown_reason="answer_unknown_field",
        question_id=qid,
    )
    noul = _probability_field(answer["noul"], qid, "noul")
    confidence = _probability_field(answer["confidence"], qid, "confidence")
    answer_confidence = _probability_field(answer["answer_confidence"], qid, "answer_confidence")
    expected = max(noul, 1 - noul)
    for field, value in (("confidence", confidence), ("answer_confidence", answer_confidence)):
        _check_close(
            value, expected, "noul_confidence_inconsistent", qid, field, _CONFIDENCE_TOLERANCE
        )
    definition = "max(P(true), 1 - P(true)); uncalibrated"
    return ValidatedAnswer(
        question_id=qid,
        question_type=QuestionType.NOUL,
        choice=None,
        score=None,
        noul=noul,
        probabilities=(),
        confidence_quantities=(
            ConfidenceQuantity("answer_confidence", answer_confidence, definition),
            ConfidenceQuantity("confidence", confidence, definition),
        ),
    )


def _validate_answer(raw: Any, question: Question) -> ValidatedAnswer:
    qid = question.question_id
    if not isinstance(raw, dict):
        raise _fail("answer_shape_invalid", "answer must be an object", question_id=qid)
    if raw.get("type") != question.question_type.value:
        raise _fail(
            "answer_type_mismatch",
            "answer type differs from the question type",
            question_id=qid,
            expected=question.question_type.value,
            observed=raw.get("type"),
        )
    if isinstance(question, ChoiceQuestion):
        return _validate_choice(raw, question)
    if isinstance(question, ScoreQuestion):
        return _validate_score(raw, question)
    return _validate_noul(raw, question)


def _validate_answers(raw: Any, question_set: QuestionSet) -> tuple[ValidatedAnswer, ...]:
    if not isinstance(raw, dict):
        raise _fail("result_shape_invalid", "answers must be an object", field="answers")
    requested = question_set.question_ids()
    if set(raw) != set(requested):
        raise _fail(
            "answer_ids_mismatch",
            "answer ids differ from the requested question ids",
            missing=sorted(set(requested) - set(raw)),
            unexpected=sorted(set(raw) - set(requested)),
        )
    return tuple(
        _validate_answer(raw[question.question_id], question) for question in question_set.questions
    )


@dataclass(frozen=True)
class ResultEnvelope:
    """Routing and coverage of one result, validated before any answer is examined."""

    routing: RoutingRecord
    usage: UsageCoverage
    raw_answers: Any


def validate_envelope(
    result: Any, question_set: QuestionSet, settings: InferenceSettings
) -> ResultEnvelope:
    if not isinstance(result, dict):
        raise _fail(
            "result_shape_invalid", "result must be an object", observed=type(result).__name__
        )
    _require_keys(result, _RESULT_KEYS, frozenset(), reason="result_shape_invalid")
    response_model = result["model"]
    if not isinstance(response_model, str) or not response_model:
        raise _fail(
            "result_shape_invalid", "result model must be a non-empty string", field="model"
        )
    routing = _validate_routing(result["routing"], response_model, settings)
    usage = _validate_usage(result["usage"], question_set.question_ids())
    return ResultEnvelope(routing=routing, usage=usage, raw_answers=result["answers"])


def validate_answers(raw: Any, question_set: QuestionSet) -> tuple[ValidatedAnswer, ...]:
    return _validate_answers(raw, question_set)


def validate_result(
    result: Any, question_set: QuestionSet, settings: InferenceSettings
) -> ValidatedResult:
    envelope = validate_envelope(result, question_set, settings)
    answers = validate_answers(envelope.raw_answers, question_set)
    return ValidatedResult(answers=answers, usage=envelope.usage, routing=envelope.routing)
