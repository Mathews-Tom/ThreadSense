"""Shared contracts for typed semantic decisions served by a separate local Laya service.

Typed decisions are not chat completions: a request carries a JSON state plus typed
questions and the answer is a validated distribution per question. Everything here is
shadow evidence. Nothing in this package alters baseline findings, evidence counts or
ranking, and no confidence quantity is calibrated.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from threadsense.errors import ThreadSenseError

DECISION_ARTIFACT_KIND = "typed_decision_annotations"
DECISION_RUN_ARTIFACT_KIND = "typed_decision_annotation_run"
DECISION_ARTIFACT_SCHEMA_VERSION = 1
QUESTION_SCHEMA_ID = "threadsense-comment-decisions"
QUESTION_SCHEMA_VERSION = 1
CALIBRATION = "uncalibrated"
POLICY = "none"
MODE = "shadow"
OTHER_OR_UNCERTAIN = "other_or_uncertain"

# Request bounds of the pinned laya-serve (laya/serve.py at the manifest commit). The
# client refuses anything larger before sending it rather than relying on a 413.
MAX_BATCH_STATES = 64
MAX_QUESTIONS = 64
MAX_STATE_CHARS = 50_000
MAX_BODY_BYTES = 2 * 1024 * 1024
MAX_CHOICE_OPTIONS = 100
MAX_SCORE_LEVELS = 32
MAX_TOTAL_OPTIONS = 512
# Token settings stay within the english checkpoint's own 512-token window.
MAX_TOKEN_SETTING = 512
# Upstream rounds every reported probability and confidence to four decimals.
ROUNDING_QUANTUM = 5e-5
TOLERANCE_EPSILON = 1e-9


class Requirement(StrEnum):
    OPTIONAL = "optional"
    REQUIRED = "required"


class QuestionType(StrEnum):
    CHOICE = "choice"
    SCORE = "score"
    NOUL = "noul"


class ItemStatus(StrEnum):
    OK = "ok"
    REFUSED = "refused"
    UNAVAILABLE = "unavailable"


class AggregateStatus(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


def canonical_json(value: Any) -> str:
    """Serialize without sorting keys: list and mapping order is part of decision identity."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def encode_request_body(body: dict[str, Any]) -> bytes:
    """The exact bytes sent on the wire; the body size bound is measured on these."""
    return canonical_json(body).encode("utf-8")


def _reject_constant(token: str) -> Any:
    raise ValueError(f"non-finite JSON constant {token}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def _finite_float(token: str) -> float:
    # `1e999` is valid JSON number syntax that Python decodes to infinity.
    value = float(token)
    if not math.isfinite(value):
        raise ValueError(f"non-finite JSON number {token}")
    return value


def decode_strict_json_object(
    raw: bytes,
    error_factory: Callable[[str, dict[str, Any]], ThreadSenseError],
    what: str,
) -> dict[str, Any]:
    """Decode UTF-8 JSON bytes refusing non-finite numbers, duplicate keys, unpaired
    surrogate escapes and non-object roots."""
    try:
        payload = json.loads(
            raw.decode("utf-8"),
            parse_constant=_reject_constant,
            parse_float=_finite_float,
            object_pairs_hook=_unique_object,
        )
        # An unpaired `\udXXX` escape decodes to text that cannot be re-encoded as UTF-8.
        canonical_json(payload).encode("utf-8")
    except (UnicodeError, ValueError, RecursionError) as error:
        raise error_factory(
            f"{what} is not strict JSON", {"reason": "invalid_json", "decode_error": str(error)}
        ) from error
    if not isinstance(payload, dict):
        raise error_factory(
            f"{what} must be a JSON object",
            {"reason": "invalid_json", "decode_error": f"root is {type(payload).__name__}"},
        )
    return payload


@dataclass(frozen=True)
class InferenceSettings:
    """Explicit request controls. Sorting, batch_size and min_confidence are never sent."""

    model: str
    lang: str
    max_len: int
    head_max_len: int

    def __post_init__(self) -> None:
        if not self.model or not self.lang:
            raise ValueError("inference settings require a model and a declared language")
        if not 0 < self.head_max_len <= self.max_len <= MAX_TOKEN_SETTING:
            raise ValueError("token settings must satisfy 0 < head_max_len <= max_len <= 512")

    def to_wire(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "lang": self.lang,
            "max_len": self.max_len,
            "head_max_len": self.head_max_len,
        }

    def to_dict(self) -> dict[str, Any]:
        return self.to_wire()

    def sha256(self) -> str:
        return canonical_sha256(
            [
                ["model", self.model],
                ["lang", self.lang],
                ["max_len", self.max_len],
                ["head_max_len", self.head_max_len],
            ]
        )


PINNED_INFERENCE_SETTINGS = InferenceSettings(
    model="english", lang="en", max_len=512, head_max_len=256
)


@dataclass(frozen=True)
class ContextPolicy:
    """Client-side character windows for comment-local context.

    The commented text itself is never windowed: a comment longer than
    ``comment_max_chars`` is refused before sending. Context fields are prefix windows
    whose exact extent is recorded per item, never a silent trim.
    """

    version: int
    comment_max_chars: int
    title_window_chars: int
    body_window_chars: int
    parent_window_chars: int

    def to_dict(self) -> dict[str, int]:
        return {
            "version": self.version,
            "comment_max_chars": self.comment_max_chars,
            "title_window_chars": self.title_window_chars,
            "body_window_chars": self.body_window_chars,
            "parent_window_chars": self.parent_window_chars,
        }

    def sha256(self) -> str:
        return canonical_sha256([[key, value] for key, value in self.to_dict().items()])


PINNED_CONTEXT_POLICY = ContextPolicy(
    version=1,
    comment_max_chars=2000,
    title_window_chars=300,
    body_window_chars=600,
    parent_window_chars=400,
)


@dataclass(frozen=True)
class ChoiceQuestion:
    question_id: str
    dimension: str
    instructions: str
    options: tuple[tuple[str, str], ...]

    @property
    def question_type(self) -> QuestionType:
        return QuestionType.CHOICE

    @property
    def labels(self) -> tuple[str, ...]:
        return tuple(label for label, _description in self.options)

    def to_wire(self) -> dict[str, Any]:
        return {
            "type": self.question_type.value,
            "instructions": self.instructions,
            "criteria": dict(self.options),
        }

    def identity(self) -> list[Any]:
        return [
            self.question_id,
            self.question_type.value,
            self.dimension,
            self.instructions,
            [[label, description] for label, description in self.options],
        ]


@dataclass(frozen=True)
class ScoreQuestion:
    question_id: str
    dimension: str
    instructions: str
    levels: tuple[str, ...]

    @property
    def question_type(self) -> QuestionType:
        return QuestionType.SCORE

    def to_wire(self) -> dict[str, Any]:
        return {
            "type": self.question_type.value,
            "instructions": self.instructions,
            "criteria": list(self.levels),
        }

    def identity(self) -> list[Any]:
        return [
            self.question_id,
            self.question_type.value,
            self.dimension,
            self.instructions,
            list(self.levels),
        ]


@dataclass(frozen=True)
class NoulQuestion:
    question_id: str
    dimension: str
    instructions: str

    @property
    def question_type(self) -> QuestionType:
        return QuestionType.NOUL

    def to_wire(self) -> dict[str, Any]:
        return {"type": self.question_type.value, "instructions": self.instructions}

    def identity(self) -> list[Any]:
        return [self.question_id, self.question_type.value, self.dimension, self.instructions]


Question = ChoiceQuestion | ScoreQuestion | NoulQuestion


@dataclass(frozen=True)
class ObjectiveIdentity:
    objective: str
    text: str

    def to_dict(self) -> dict[str, str]:
        return {"objective": self.objective, "text": self.text}


@dataclass(frozen=True)
class VocabularyIdentity:
    """The bounded domain theme vocabulary the theme question was built from."""

    domain: str
    version: str
    themes: tuple[str, ...]
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "version": self.version,
            "themes": list(self.themes),
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class QuestionSet:
    schema_id: str
    schema_version: int
    objective: ObjectiveIdentity
    vocabulary: VocabularyIdentity
    questions: tuple[Question, ...]

    def __post_init__(self) -> None:
        ids = self.question_ids()
        if not ids or len(set(ids)) != len(ids):
            raise ValueError("question set requires unique question ids")
        if len(ids) > MAX_QUESTIONS:
            raise ValueError("question set exceeds the server question limit")
        total_options = 0
        for question in self.questions:
            if isinstance(question, ChoiceQuestion):
                labels = question.labels
                if len(labels) < 2 or len(set(labels)) != len(labels):
                    raise ValueError(f"choice {question.question_id} needs unique options")
                if len(labels) > MAX_CHOICE_OPTIONS:
                    raise ValueError(f"choice {question.question_id} has too many options")
                total_options += len(labels)
            elif isinstance(question, ScoreQuestion):
                if not 2 <= len(question.levels) <= MAX_SCORE_LEVELS:
                    raise ValueError(f"score {question.question_id} level count is invalid")
                total_options += len(question.levels)
        if total_options > MAX_TOTAL_OPTIONS:
            raise ValueError("question set exceeds the server total option limit")

    def question_ids(self) -> tuple[str, ...]:
        return tuple(question.question_id for question in self.questions)

    def by_id(self) -> dict[str, Question]:
        return {question.question_id: question for question in self.questions}

    def wire_questions(self) -> dict[str, Any]:
        return {question.question_id: question.to_wire() for question in self.questions}

    def identity(self) -> list[Any]:
        return [
            self.schema_id,
            self.schema_version,
            [self.objective.objective, self.objective.text],
            [
                self.vocabulary.domain,
                self.vocabulary.version,
                list(self.vocabulary.themes),
                self.vocabulary.sha256,
            ],
            [question.identity() for question in self.questions],
        ]

    def sha256(self) -> str:
        return canonical_sha256(self.identity())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "objective": self.objective.to_dict(),
            "vocabulary": self.vocabulary.to_dict(),
            "questions": [
                {
                    "question_id": question.question_id,
                    "dimension": question.dimension,
                    **question.to_wire(),
                }
                for question in self.questions
            ],
        }


@dataclass(frozen=True)
class CheckpointFile:
    path: str
    bytes: int
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "bytes": self.bytes, "sha256": self.sha256}


@dataclass(frozen=True)
class CodeIdentity:
    repository: str
    commit: str
    package: str
    version: str
    license: str
    identity_basis: str

    def to_dict(self) -> dict[str, str]:
        return {
            "repository": self.repository,
            "commit": self.commit,
            "package": self.package,
            "version": self.version,
            "license": self.license,
            "identity_basis": self.identity_basis,
        }


@dataclass(frozen=True)
class CheckpointIdentity:
    repository: str
    revision: str
    license: str
    files: tuple[CheckpointFile, ...]
    identity_basis: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "repository": self.repository,
            "revision": self.revision,
            "license": self.license,
            "files": [item.to_dict() for item in self.files],
            "identity_basis": self.identity_basis,
        }


@dataclass(frozen=True)
class WireContract:
    health_path: str
    batch_path: str
    model: str
    language: str

    def to_dict(self) -> dict[str, str]:
        return {
            "health_path": self.health_path,
            "batch_path": self.batch_path,
            "model": self.model,
            "language": self.language,
        }


@dataclass(frozen=True)
class RuntimeManifest:
    """Immutable identity of the pinned typed-decision runtime, free of local paths."""

    manifest_version: int
    backend: str
    code: CodeIdentity
    checkpoint: CheckpointIdentity
    wire: WireContract

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "backend": self.backend,
            "code": self.code.to_dict(),
            "checkpoint": self.checkpoint.to_dict(),
            "wire": self.wire.to_dict(),
        }

    def sha256(self) -> str:
        return canonical_sha256(self.to_dict())


@dataclass(frozen=True)
class CheckpointVerification:
    """Client-observed digests of the checkpoint directory the service reported."""

    repo: str
    files: tuple[CheckpointFile, ...]
    basis: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "repo": self.repo,
            "basis": self.basis,
            "files": [item.to_dict() for item in self.files],
        }


@dataclass(frozen=True)
class ContextWindow:
    """A prefix window ``[0, included_chars)`` of one context field."""

    field: str
    source_chars: int
    included_chars: int

    @property
    def complete(self) -> bool:
        return self.included_chars == self.source_chars

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "source_chars": self.source_chars,
            "included_chars": self.included_chars,
            "complete": self.complete,
        }


@dataclass(frozen=True)
class CommentState:
    """One comment's wire state plus the exact coverage of what it contains.

    ``state`` is ``None`` when the comment was refused before sending; then
    ``refusal_reasons`` is non-empty.
    """

    index: int
    comment_id: str
    parent_comment_id: str | None
    comment_text_sha256: str
    parent_text_sha256: str | None
    parent_context: str
    state: dict[str, str] | None
    state_chars: int
    windows: tuple[ContextWindow, ...]
    refusal_reasons: tuple[str, ...]

    def state_sha256(self) -> str | None:
        return None if self.state is None else canonical_sha256(self.state)


@dataclass(frozen=True)
class BatchPlan:
    batch_index: int
    item_indexes: tuple[int, ...]
    body: dict[str, Any]


@dataclass(frozen=True)
class ConfidenceQuantity:
    name: str
    value: float
    definition: str

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "value": self.value, "definition": self.definition}


@dataclass(frozen=True)
class ValidatedAnswer:
    """A usable typed answer. Exactly one of choice/score/noul is set for its type."""

    question_id: str
    question_type: QuestionType
    choice: str | None
    score: float | None
    noul: float | None
    probabilities: tuple[tuple[str, float], ...]
    confidence_quantities: tuple[ConfidenceQuantity, ...]

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any]
        if self.question_type is QuestionType.CHOICE:
            value = {"choice": self.choice}
        elif self.question_type is QuestionType.SCORE:
            value = {"score": self.score}
        else:
            value = {"noul": self.noul}
        return {
            "question_id": self.question_id,
            "type": self.question_type.value,
            **value,
            "probabilities": [[label, probability] for label, probability in self.probabilities],
            "confidence_quantities": [item.to_dict() for item in self.confidence_quantities],
            "calibration": CALIBRATION,
        }


@dataclass(frozen=True)
class UsageCoverage:
    input_tokens: int
    output_tokens: int
    state_tokens: int
    state_tokens_dropped: int
    truncated: bool
    truncated_questions: tuple[str, ...]
    options_reported: bool

    def refusal_reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if self.state_tokens_dropped > 0:
            reasons.append("server_dropped_state_tokens")
        if self.truncated:
            reasons.append("server_reported_truncation")
        if self.truncated_questions:
            reasons.append("server_truncated_questions")
        if self.options_reported:
            reasons.append("server_reported_option_usage")
        return tuple(reasons)

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "state_tokens": self.state_tokens,
            "state_tokens_dropped": self.state_tokens_dropped,
            "truncated": self.truncated,
            "truncated_questions": list(self.truncated_questions),
            "options_reported": self.options_reported,
        }


@dataclass(frozen=True)
class RoutingRecord:
    """The service's routing block. Language is declared by the request, never detected."""

    response_model: str
    model: str
    repo: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "response_model": self.response_model,
            "model": self.model,
            "repo": self.repo,
            "reason": self.reason,
            "detection": None,
            "workflow": None,
        }


@dataclass(frozen=True)
class ValidatedResult:
    answers: tuple[ValidatedAnswer, ...]
    usage: UsageCoverage
    routing: RoutingRecord


@dataclass(frozen=True)
class HealthObservation:
    """The service health payload as observed; a null revision is kept null."""

    status: str
    loaded: tuple[str, ...]
    revision: str | None
    device: str | None
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "loaded": list(self.loaded),
            "revision": self.revision,
            "device": self.device,
            "payload": self.payload,
        }


@dataclass(frozen=True)
class HttpExchange:
    """One HTTP call as sent and received; bodies are the exact UTF-8 texts."""

    method: str
    path: str
    status_code: int
    request_body: str | None
    response_body: str
    payload: dict[str, Any]
    wall_seconds: float

    def request_sha256(self) -> str | None:
        if self.request_body is None:
            return None
        return hashlib.sha256(self.request_body.encode("utf-8")).hexdigest()

    def response_sha256(self) -> str:
        return hashlib.sha256(self.response_body.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ItemFailure:
    reason: str
    message: str
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"reason": self.reason, "message": self.message, "details": self.details}


@dataclass(frozen=True)
class CoverageSummary:
    status: AggregateStatus
    total: int
    ok: int
    refused: int
    unavailable: int

    @classmethod
    def from_statuses(cls, statuses: Iterable[ItemStatus]) -> CoverageSummary:
        values = list(statuses)
        ok = values.count(ItemStatus.OK)
        refused = values.count(ItemStatus.REFUSED)
        unavailable = values.count(ItemStatus.UNAVAILABLE)
        if ok == len(values):
            status = AggregateStatus.COMPLETE
        elif ok == 0:
            status = AggregateStatus.UNAVAILABLE
        else:
            status = AggregateStatus.PARTIAL
        return cls(status, len(values), ok, refused, unavailable)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "total": self.total,
            "ok": self.ok,
            "refused": self.refused,
            "unavailable": self.unavailable,
        }
