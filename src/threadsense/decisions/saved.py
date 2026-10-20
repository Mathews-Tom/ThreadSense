"""Strict loading and offline replay of saved typed decision documents.

A saved document is accepted only when its file name is the sha256 of its exact bytes and
those bytes are the canonical serialization. Replay re-derives every sent item from the
recorded response bodies through the validation the annotator used. Checkpoint files are
not re-hashed offline: the saved verification's repo stands in for the client-side check.
"""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.decisions.artifact import decision_bytes, strip_volatile_details
from threadsense.decisions.contracts import (
    CALIBRATION,
    DECISION_ARTIFACT_KIND,
    DECISION_ARTIFACT_SCHEMA_VERSION,
    MODE,
    POLICY,
    CoverageSummary,
    InferenceSettings,
    ItemFailure,
    ItemStatus,
    QuestionSet,
    QuestionType,
    Requirement,
    canonical_json,
    canonical_sha256,
    decode_strict_json_object,
)
from threadsense.decisions.evaluation_contracts import (
    ReplayReport,
    SavedAnswer,
    SavedDecision,
    SavedItem,
)
from threadsense.decisions.validation import (
    split_batch_results,
    validate_answers,
    validate_envelope,
)
from threadsense.errors import DecisionEvaluationError, DecisionProtocolError

_TOP_LEVEL_KEYS = frozenset(
    {
        "artifact_kind",
        "schema_version",
        "mode",
        "policy",
        "calibration",
        "requirement",
        "input",
        "backend",
        "route",
        "questions",
        "settings",
        "context_policy",
        "coverage",
        "items",
        "exchanges",
    }
)
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_STATUS_VALUES = tuple(status.value for status in ItemStatus)
_QUESTION_TYPE_VALUES = tuple(question_type.value for question_type in QuestionType)
_REQUIREMENT_VALUES = tuple(requirement.value for requirement in Requirement)
_CHECKPOINT_REASONS = frozenset({"checkpoint_identity_unverified", "checkpoint_repo_changed"})
_COMPARED_FIELDS = ("status", "reasons", "failure", "usage", "routing", "answers")
_ERROR_ITEM_REASONS = {
    "decision_runtime_unavailable": "runtime_request_failed",
    "decision_protocol_error": "invalid_response",
}


def _invalid(field: str, message: str) -> DecisionEvaluationError:
    return DecisionEvaluationError(message, {"reason": "annotation_invalid", "field": field})


def _value(container: dict[str, Any], key: str, field: str) -> Any:
    if key not in container:
        raise _invalid(field, "a saved decision field is missing")
    return container[key]


def _object(container: dict[str, Any], key: str, field: str) -> dict[str, Any]:
    value = _value(container, key, field)
    if not isinstance(value, dict):
        raise _invalid(field, "a saved decision field must be an object")
    return value


def _list(container: dict[str, Any], key: str, field: str) -> list[Any]:
    value = _value(container, key, field)
    if not isinstance(value, list):
        raise _invalid(field, "a saved decision field must be a list")
    return value


def _string(container: dict[str, Any], key: str, field: str) -> str:
    value = _value(container, key, field)
    if not isinstance(value, str) or not value:
        raise _invalid(field, "a saved decision field must be a non-empty string")
    return value


def _sha256_field(container: dict[str, Any], key: str, field: str) -> str:
    value = _string(container, key, field)
    if not _SHA256_PATTERN.fullmatch(value):
        raise _invalid(field, "a saved decision digest must be 64 lowercase hex characters")
    return value


def _is_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _integer(container: dict[str, Any], key: str, field: str) -> int:
    value = _value(container, key, field)
    if not _is_integer(value):
        raise _invalid(field, "a saved decision field must be an integer")
    result: int = value
    return result


def _optional_integer(container: dict[str, Any], key: str, field: str) -> int | None:
    value = _value(container, key, field)
    if value is not None and not _is_integer(value):
        raise _invalid(field, "a saved decision field must be an integer or null")
    result: int | None = value
    return result


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise _invalid(field, "a saved decision quantity must be a number")
    try:
        number = float(value)
    except OverflowError as error:
        raise _invalid(field, "a saved decision quantity must be finite") from error
    if not math.isfinite(number):
        raise _invalid(field, "a saved decision quantity must be finite")
    return number


def _read_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as error:
        raise DecisionEvaluationError(
            "saved decision could not be read",
            {
                "reason": "annotation_unreadable",
                "path": str(path),
                "error_type": type(error).__name__,
            },
        ) from error


def _check_envelope(document: dict[str, Any]) -> None:
    keys = set(document)
    if keys != _TOP_LEVEL_KEYS:
        raise DecisionEvaluationError(
            "saved decision top-level keys differ from the decision schema",
            {
                "reason": "annotation_invalid",
                "field": "document",
                "missing": sorted(_TOP_LEVEL_KEYS - keys),
                "unexpected": sorted(keys - _TOP_LEVEL_KEYS),
            },
        )
    if document["artifact_kind"] != DECISION_ARTIFACT_KIND:
        raise _invalid("artifact_kind", "saved document is not a typed decision annotation")
    version = document["schema_version"]
    if not _is_integer(version) or version != DECISION_ARTIFACT_SCHEMA_VERSION:
        raise DecisionEvaluationError(
            "saved decision schema version is not supported",
            {"reason": "annotation_version_unsupported", "schema_version": version},
        )
    for key, expected in (("mode", MODE), ("policy", POLICY), ("calibration", CALIBRATION)):
        if document[key] != expected:
            raise _invalid(key, f"saved decision {key} must be {expected!r}")
    if document["requirement"] not in _REQUIREMENT_VALUES:
        raise _invalid("requirement", "saved decision requirement is not a known requirement")


def _probabilities(raw: dict[str, Any], field: str) -> tuple[tuple[str, float], ...]:
    pairs: list[tuple[str, float]] = []
    for position, pair in enumerate(_list(raw, "probabilities", f"{field}.probabilities")):
        pair_field = f"{field}.probabilities[{position}]"
        if not (isinstance(pair, list) and len(pair) == 2 and isinstance(pair[0], str)):
            raise _invalid(pair_field, "a probability entry must be a [label, number] pair")
        pairs.append((pair[0], _number(pair[1], pair_field)))
    return tuple(pairs)


def _answer_confidence(raw: dict[str, Any], field: str) -> float:
    quantities_field = f"{field}.confidence_quantities"
    found: list[float] = []
    for position, quantity in enumerate(_list(raw, "confidence_quantities", quantities_field)):
        quantity_field = f"{quantities_field}[{position}]"
        if not isinstance(quantity, dict):
            raise _invalid(quantity_field, "a confidence quantity must be an object")
        value = _number(_value(quantity, "value", f"{quantity_field}.value"), quantity_field)
        if _string(quantity, "name", f"{quantity_field}.name") == "answer_confidence":
            found.append(value)
    if len(found) != 1:
        raise _invalid(quantities_field, "an answer must carry exactly one answer_confidence")
    return found[0]


def _typed_number(raw: dict[str, Any], key: str, field: str) -> float:
    return _number(_value(raw, key, f"{field}.{key}"), f"{field}.{key}")


def _parse_answer(raw: Any, field: str) -> SavedAnswer:
    if not isinstance(raw, dict):
        raise _invalid(field, "a saved answer must be an object")
    type_value = _string(raw, "type", f"{field}.type")
    if type_value not in _QUESTION_TYPE_VALUES:
        raise _invalid(f"{field}.type", "a saved answer has an unknown question type")
    question_type = QuestionType(type_value)
    if _value(raw, "calibration", f"{field}.calibration") != CALIBRATION:
        raise _invalid(f"{field}.calibration", f"a saved answer must be {CALIBRATION!r}")
    is_choice = question_type is QuestionType.CHOICE
    is_score = question_type is QuestionType.SCORE
    is_noul = question_type is QuestionType.NOUL
    return SavedAnswer(
        question_id=_string(raw, "question_id", f"{field}.question_id"),
        question_type=question_type,
        choice=_string(raw, "choice", f"{field}.choice") if is_choice else None,
        score=_typed_number(raw, "score", field) if is_score else None,
        noul=_typed_number(raw, "noul", field) if is_noul else None,
        probabilities=_probabilities(raw, field),
        answer_confidence=_answer_confidence(raw, field),
        raw=raw,
    )


def _parse_reasons(raw: dict[str, Any], field: str) -> tuple[str, ...]:
    reasons = _list(raw, "reasons", field)
    if not all(isinstance(reason, str) and reason for reason in reasons):
        raise _invalid(field, "saved item reasons must be non-empty strings")
    return tuple(reasons)


def _parse_item(raw: Any, position: int) -> SavedItem:
    field = f"items[{position}]"
    if not isinstance(raw, dict):
        raise _invalid(field, "a saved item must be an object")
    if _integer(raw, "index", f"{field}.index") != position:
        raise _invalid(f"{field}.index", "saved items must be indexed 0..n-1 in order")
    status_value = _string(raw, "status", f"{field}.status")
    if status_value not in _STATUS_VALUES:
        raise _invalid(f"{field}.status", "a saved item has an unknown status")
    status = ItemStatus(status_value)
    reasons = _parse_reasons(raw, f"{field}.reasons")
    answers = _list(raw, "answers", f"{field}.answers")
    if (status is ItemStatus.OK) != bool(answers):
        raise _invalid(f"{field}.answers", "a saved item has answers exactly when it is ok")
    if (status is ItemStatus.OK) == bool(reasons):
        raise _invalid(f"{field}.reasons", "a saved item states reasons exactly when not ok")
    return SavedItem(
        index=position,
        comment_id=_string(raw, "comment_id", f"{field}.comment_id"),
        comment_text_sha256=_sha256_field(
            raw, "comment_text_sha256", f"{field}.comment_text_sha256"
        ),
        status=status,
        reasons=reasons,
        batch_index=_optional_integer(raw, "batch_index", f"{field}.batch_index"),
        answers=tuple(
            _parse_answer(answer, f"{field}.answers[{number}]")
            for number, answer in enumerate(answers)
        ),
        raw=raw,
    )


def _check_coverage(document: dict[str, Any], items: tuple[SavedItem, ...]) -> CoverageSummary:
    derived = CoverageSummary.from_statuses(item.status for item in items)
    if canonical_json(document["coverage"]) != canonical_json(derived.to_dict()):
        raise DecisionEvaluationError(
            "saved coverage does not match the saved item statuses",
            {
                "reason": "annotation_coverage_mismatch",
                "saved": document["coverage"],
                "derived": derived.to_dict(),
            },
        )
    return derived


def _backend_identity(document: dict[str, Any]) -> tuple[str, str, bool]:
    backend = _object(document, "backend", "backend")
    manifest = _object(backend, "manifest", "backend.manifest")
    manifest_sha256 = _sha256_field(backend, "manifest_sha256", "backend.manifest_sha256")
    if canonical_sha256(manifest) != manifest_sha256:
        raise _invalid("backend.manifest_sha256", "the runtime manifest digest does not match")
    checkpoint = _object(manifest, "checkpoint", "backend.manifest.checkpoint")
    revision = _string(checkpoint, "revision", "backend.manifest.checkpoint.revision")
    pinned_files = _list(checkpoint, "files", "backend.manifest.checkpoint.files")
    verification = _value(backend, "checkpoint_verification", "backend.checkpoint_verification")
    if verification is None:
        return manifest_sha256, revision, False
    if not isinstance(verification, dict):
        raise _invalid(
            "backend.checkpoint_verification", "checkpoint verification must be an object"
        )
    _string(verification, "repo", "backend.checkpoint_verification.repo")
    observed = _list(verification, "files", "backend.checkpoint_verification.files")
    return manifest_sha256, revision, canonical_json(observed) == canonical_json(pinned_files)


def _route(document: dict[str, Any]) -> tuple[str, str]:
    route = _object(document, "route", "route")
    model = _string(route, "model", "route.model")
    language = _object(route, "language", "route.language")
    if _value(language, "basis", "route.language.basis") != "declared":
        raise _invalid("route.language.basis", "the saved language must be declared")
    if _value(route, "detection", "route.detection") is not None:
        raise _invalid("route.detection", "a saved decision must not carry language detection")
    return model, _string(language, "value", "route.language.value")


def _identity_digest(document: dict[str, Any], key: str) -> str:
    """Recompute an identity digest from its saved key/value pairs, in their saved order."""
    identity = _object(document, key, key)
    digest = _sha256_field(identity, "sha256", f"{key}.sha256")
    pairs = [[name, value] for name, value in identity.items() if name != "sha256"]
    if canonical_sha256(pairs) != digest:
        raise _invalid(f"{key}.sha256", f"the saved {key} digest does not match its values")
    return digest


def _saved_decision(
    document: dict[str, Any], digest: str, items: tuple[SavedItem, ...], coverage: CoverageSummary
) -> SavedDecision:
    snapshot = _object(document, "input", "input")
    comment_count = _integer(snapshot, "comment_count", "input.comment_count")
    if comment_count != len(items):
        raise _invalid("input.comment_count", "the saved comment count differs from the items")
    runtime_sha256, revision, verified = _backend_identity(document)
    route_model, language = _route(document)
    questions = _object(document, "questions", "questions")
    objective = _object(questions, "objective", "questions.objective")
    vocabulary = _object(questions, "vocabulary", "questions.vocabulary")
    return SavedDecision(
        decision_sha256=digest,
        snapshot_sha256=_sha256_field(snapshot, "sha256", "input.sha256"),
        thread_id=_string(snapshot, "thread_id", "input.thread_id"),
        comment_count=comment_count,
        requirement=document["requirement"],
        runtime_manifest_sha256=runtime_sha256,
        checkpoint_revision=revision,
        checkpoint_verified=verified,
        question_set_sha256=_sha256_field(questions, "sha256", "questions.sha256"),
        settings_sha256=_identity_digest(document, "settings"),
        context_policy_sha256=_identity_digest(document, "context_policy"),
        language=language,
        route_model=route_model,
        objective=_string(objective, "objective", "questions.objective.objective"),
        domain=_string(vocabulary, "domain", "questions.vocabulary.domain"),
        coverage=coverage,
        items=items,
        document=document,
    )


def load_saved_decision(path: Path) -> SavedDecision:
    """Load one saved decision document, refusing anything but its exact published form."""
    raw = _read_bytes(path)
    document = decode_strict_json_object(raw, DecisionEvaluationError, "saved decision")
    digest = hashlib.sha256(raw).hexdigest()
    if path.name != f"{digest}.json":
        raise DecisionEvaluationError(
            "saved decision file name is not the sha256 of its bytes",
            {"reason": "annotation_digest_mismatch", "path": str(path), "sha256": digest},
        )
    if decision_bytes(document) != raw:
        raise DecisionEvaluationError(
            "saved decision bytes are not the canonical serialization",
            {"reason": "annotation_not_canonical", "path": str(path)},
        )
    _check_envelope(document)
    items = tuple(
        _parse_item(item, position)
        for position, item in enumerate(_list(document, "items", "items"))
    )
    coverage = _check_coverage(document, items)
    return _saved_decision(document, digest, items, coverage)


@dataclass(frozen=True)
class _Exchange:
    batch_index: int
    item_indexes: tuple[int, ...]
    status_code: int | None
    response_body: str | None
    # For an "error" exchange: the fields every item of the batch must carry.
    failed_items: dict[str, Any] | None

    def replayable(self) -> bool:
        return self.response_body is not None and self.status_code == 200


def _digest_mismatch(batch_index: int, field: str) -> DecisionEvaluationError:
    return DecisionEvaluationError(
        "a recorded exchange body does not match its saved digest",
        {"reason": "replay_exchange_digest_mismatch", "batch_index": batch_index, "field": field},
    )


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _failed_items(error: dict[str, Any], field: str) -> dict[str, Any]:
    """Mirror how the annotator fails every item of a batch whose request raised."""
    code = _string(error, "code", f"{field}.code")
    if code not in _ERROR_ITEM_REASONS:
        raise _invalid(f"{field}.code", "a recorded exchange error has an unknown code")
    details = _object(error, "details", f"{field}.details")
    failure = ItemFailure(
        reason=_string(details, "reason", f"{field}.details.reason"),
        message=_string(error, "message", f"{field}.message"),
        details=strip_volatile_details(details),
    )
    return {
        "status": ItemStatus.UNAVAILABLE.value,
        "reasons": [_ERROR_ITEM_REASONS[code]],
        "failure": failure.to_dict(),
    }


def _error_exchange(
    raw: dict[str, Any], field: str, batch_index: int, item_indexes: tuple[int, ...]
) -> _Exchange:
    for key in ("status_code", "response_body", "response_sha256"):
        if _value(raw, key, f"{field}.{key}") is not None:
            raise _invalid(f"{field}.{key}", "an error exchange must not record a response")
    error = _object(raw, "error", f"{field}.error")
    return _Exchange(
        batch_index=batch_index,
        item_indexes=item_indexes,
        status_code=None,
        response_body=None,
        failed_items=_failed_items(error, f"{field}.error"),
    )


def _response_exchange(
    raw: dict[str, Any], field: str, batch_index: int, item_indexes: tuple[int, ...]
) -> _Exchange:
    if _value(raw, "error", f"{field}.error") is not None:
        raise _invalid(f"{field}.error", "a response exchange must not record an error")
    status_code = _integer(raw, "status_code", f"{field}.status_code")
    response_body = _string(raw, "response_body", f"{field}.response_body")
    response_sha256 = _sha256_field(raw, "response_sha256", f"{field}.response_sha256")
    if _sha256_text(response_body) != response_sha256:
        raise _digest_mismatch(batch_index, "response_body")
    return _Exchange(
        batch_index=batch_index,
        item_indexes=item_indexes,
        status_code=status_code,
        response_body=response_body,
        failed_items=None,
    )


def _parse_exchange(raw: Any, position: int) -> _Exchange:
    field = f"exchanges[{position}]"
    if not isinstance(raw, dict):
        raise _invalid(field, "a recorded exchange must be an object")
    batch_index = _integer(raw, "batch_index", f"{field}.batch_index")
    item_indexes = _list(raw, "item_indexes", f"{field}.item_indexes")
    if not item_indexes or not all(_is_integer(index) for index in item_indexes):
        raise _invalid(f"{field}.item_indexes", "a recorded exchange must list item indexes")
    request_body = _string(raw, "request_body", f"{field}.request_body")
    request_sha256 = _sha256_field(raw, "request_sha256", f"{field}.request_sha256")
    if _sha256_text(request_body) != request_sha256:
        raise _digest_mismatch(batch_index, "request_body")
    outcome = _value(raw, "outcome", f"{field}.outcome")
    if outcome == "error":
        return _error_exchange(raw, field, batch_index, tuple(item_indexes))
    if outcome != "response":
        raise _invalid(f"{field}.outcome", "a recorded exchange outcome must be response or error")
    return _response_exchange(raw, field, batch_index, tuple(item_indexes))


def _mismatch(item: SavedItem, field: str) -> DecisionEvaluationError:
    return DecisionEvaluationError(
        "a saved item differs from its re-derivation from the recorded response",
        {"reason": "replay_mismatch", "comment_id": item.comment_id, "field": field},
    )


def _parse_exchanges(saved: SavedDecision) -> tuple[_Exchange, ...]:
    raw_exchanges = _list(saved.document, "exchanges", "exchanges")
    exchanges = tuple(_parse_exchange(raw, position) for position, raw in enumerate(raw_exchanges))
    batches = {exchange.batch_index for exchange in exchanges}
    for exchange in exchanges:
        for index in exchange.item_indexes:
            if not 0 <= index < len(saved.items):
                raise _invalid("exchanges.item_indexes", "an exchange names an unknown item")
            if saved.items[index].batch_index != exchange.batch_index:
                raise _mismatch(saved.items[index], "batch_index")
    for item in saved.items:
        # Without any exchange the health check failed and no batch was sent.
        if exchanges and item.batch_index is not None and item.batch_index not in batches:
            raise _mismatch(item, "batch_index")
    return exchanges


def _failure_dict(error: DecisionProtocolError) -> dict[str, Any]:
    failure = ItemFailure(
        reason=error.details["reason"],
        message=error.message,
        details=strip_volatile_details(error.details),
    )
    return failure.to_dict()


def _require_fields(item: SavedItem, expected: dict[str, Any]) -> None:
    # Canonical text comparison is type- and order-exact (1 != 1.0, true != 1).
    for field, value in expected.items():
        saved_value = _value(item.raw, field, f"items[{item.index}].{field}")
        if canonical_json(saved_value) != canonical_json(value):
            raise _mismatch(item, field)


def _require_failed_batch(
    saved: SavedDecision, exchange: _Exchange, expected: dict[str, Any]
) -> None:
    for index in exchange.item_indexes:
        _require_fields(saved.items[index], expected)


def _invalid_batch_fields(error: DecisionProtocolError) -> dict[str, Any]:
    return {
        "status": ItemStatus.UNAVAILABLE.value,
        "reasons": ["invalid_response"],
        "failure": _failure_dict(error),
    }


def _recorded_results(saved: SavedDecision, exchanges: tuple[_Exchange, ...]) -> dict[int, Any]:
    results: dict[int, Any] = {}
    for exchange in exchanges:
        if exchange.failed_items is not None:
            # The request itself failed; its items must record exactly that failure.
            _require_failed_batch(saved, exchange, exchange.failed_items)
            continue
        if exchange.response_body is None or not exchange.replayable():
            continue
        payload = decode_strict_json_object(
            exchange.response_body.encode("utf-8"), DecisionEvaluationError, "recorded response"
        )
        try:
            batch = split_batch_results(payload, len(exchange.item_indexes))
        except DecisionProtocolError as error:
            # The whole batch was unusable; its items must record exactly that refusal.
            _require_failed_batch(saved, exchange, _invalid_batch_fields(error))
            continue
        results.update(zip(exchange.item_indexes, batch, strict=True))
    return results


def _unavailable_answer(error: DecisionProtocolError) -> dict[str, Any]:
    return {
        "status": ItemStatus.UNAVAILABLE.value,
        "reasons": ["invalid_typed_answer"],
        "failure": _failure_dict(error),
        "usage": None,
        "routing": None,
        "answers": [],
    }


def _derive_fields(
    result: Any, question_set: QuestionSet, settings: InferenceSettings, repo: str | None
) -> dict[str, Any] | None:
    """Mirror the annotator's per-result derivation; None means a checkpoint refusal."""
    try:
        envelope = validate_envelope(result, question_set, settings)
    except DecisionProtocolError as error:
        return _unavailable_answer(error)
    if repo is None or envelope.routing.repo != repo:
        return None
    usage = envelope.usage.to_dict()
    routing = envelope.routing.to_dict()
    reasons = envelope.usage.refusal_reasons()
    if reasons:
        # Coverage refusal wins over answer content, exactly as in the annotator.
        return {
            "status": ItemStatus.REFUSED.value,
            "reasons": list(reasons),
            "failure": None,
            "usage": usage,
            "routing": routing,
            "answers": [],
        }
    try:
        answers = validate_answers(envelope.raw_answers, question_set)
    except DecisionProtocolError as error:
        return _unavailable_answer(error)
    return {
        "status": ItemStatus.OK.value,
        "reasons": [],
        "failure": None,
        "usage": usage,
        "routing": routing,
        "answers": [answer.to_dict() for answer in answers],
    }


def _require_checkpoint_refusal(item: SavedItem) -> None:
    if item.status is not ItemStatus.UNAVAILABLE:
        raise _mismatch(item, "status")
    if len(item.reasons) != 1 or item.reasons[0] not in _CHECKPOINT_REASONS:
        raise _mismatch(item, "reasons")
    _require_fields(item, {"usage": None, "routing": None})


def _checkpoint_repo(saved: SavedDecision) -> str | None:
    verification = saved.document["backend"]["checkpoint_verification"]
    if verification is None:
        return None
    repo: str = verification["repo"]
    return repo


def _require_replay_identity(
    saved: SavedDecision, question_set: QuestionSet, settings: InferenceSettings
) -> None:
    for field, saved_value, current in (
        ("question_set_sha256", saved.question_set_sha256, question_set.sha256()),
        ("settings_sha256", saved.settings_sha256, settings.sha256()),
    ):
        if saved_value != current:
            raise DecisionEvaluationError(
                "replay identity differs from the saved decision",
                {
                    "reason": "replay_identity_mismatch",
                    "field": field,
                    "saved": saved_value,
                    "current": current,
                },
            )
    questions = {
        key: value for key, value in saved.document["questions"].items() if key != "sha256"
    }
    if canonical_json(questions) != canonical_json(question_set.to_dict()):
        raise DecisionEvaluationError(
            "the saved questions differ from the replay question set",
            {"reason": "replay_identity_mismatch", "field": "questions"},
        )


def _unsent_reason(item: SavedItem) -> str:
    if item.status is ItemStatus.OK:
        raise _mismatch(item, "exchanges")
    return item.reasons[0]


def replay_saved_decision(
    saved: SavedDecision, question_set: QuestionSet, settings: InferenceSettings
) -> ReplayReport:
    """Re-derive every item with a recorded response and require it to equal the saved item."""
    _require_replay_identity(saved, question_set, settings)
    results = _recorded_results(saved, _parse_exchanges(saved))
    repo = _checkpoint_repo(saved)
    replayed = 0
    matched = 0
    not_replayable: list[tuple[str, str]] = []
    for item in saved.items:
        if item.index not in results:
            not_replayable.append((item.comment_id, _unsent_reason(item)))
            continue
        replayed += 1
        expected = _derive_fields(results[item.index], question_set, settings, repo)
        if expected is None:
            _require_checkpoint_refusal(item)
        else:
            _require_fields(item, expected)
        matched += 1
    return ReplayReport(
        decision_sha256=saved.decision_sha256,
        replayed_items=replayed,
        matched_items=matched,
        not_replayable=tuple(not_replayable),
    )
