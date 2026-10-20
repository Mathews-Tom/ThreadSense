"""Activation eligibility of a typed decision shape against frozen evaluation evidence.

A policy check reads an operator-authored activation policy and one content-addressed
evaluation document and reports whether one typed decision question is eligible for semantic
activation by a named consumer. It never activates a consumer, never installs a policy and
never leaves shadow mode. Every applicable check runs and records its reasons in a fixed
order; a check whose inputs are absent is recorded as not run, never as passed. Only the
accepted label origin differs by scope, and a test-only result never permits activation.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeGuard, TypeVar

from threadsense.contracts.analysis_contract import DomainType, ObjectiveType
from threadsense.decisions.artifact import (
    check_output_location,
    decision_bytes,
    publish_immutable,
    refuse_alias,
)
from threadsense.decisions.assessment import _rate
from threadsense.decisions.contracts import MODE, RuntimeManifest
from threadsense.decisions.evaluate import _current_bindings
from threadsense.decisions.evaluation_contracts import (
    EVALUATION_ARTIFACT_KIND,
    EVALUATION_SCHEMA_VERSION,
    IdentityBindings,
    LabelOriginKind,
    Partition,
)
from threadsense.decisions.questions import build_question_set
from threadsense.domains.loader import load_domain_vocabulary
from threadsense.errors import DecisionArtifactError

POLICY_KIND = "typed_decision_activation_policy"
POLICY_CHECK_KIND = "typed_decision_policy_check"
POLICY_SCHEMA_VERSION = 1
HELD_OUT_PARTITION = Partition.EVALUATION
# A test_only disclosure must match every pattern (case-insensitive whole words) so that the
# policy itself states that its fixture is constructed for tests: "contest" or "latest" do not.
TEST_ONLY_DISCLOSURE_PATTERNS = (
    re.compile(r"\bconstructed\b", re.IGNORECASE),
    re.compile(r"\btests?\b", re.IGNORECASE),
)
ACTIVATION_NOTE = (
    "a policy check never activates a consumer or installs a policy; "
    "semantic decisions stay in shadow mode"
)
_POLICY_CHECKS_DIRNAME = "policy-checks"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_STALE_CODES = {
    "runtime_manifest_sha256": "stale_checkpoint_binding",
    "checkpoint_revision": "stale_checkpoint_binding",
    "question_set_sha256": "stale_question_binding",
    "settings_sha256": "stale_settings_binding",
    "context_policy_sha256": "stale_context_policy_binding",
    "language": "stale_language_binding",
    "dataset_manifest_sha256": "stale_dataset_binding",
    "labels_sha256": "stale_dataset_binding",
}
_IDENTITY_FIELDS = (
    "runtime_manifest_sha256",
    "checkpoint_revision",
    "question_set_sha256",
    "settings_sha256",
    "context_policy_sha256",
    "language",
)
_POLICY_KEYS = (
    "artifact_kind",
    "schema_version",
    "policy_id",
    "scope",
    "disclosure",
    "decision",
    "bindings",
    "tolerances",
)
_DECISION_KEYS = ("question_id", "consumer")
_TOLERANCE_KEYS = ("max_error_rate", "min_decision_rate", "min_labeled_decided")
_RATE_TOLERANCES = ("max_error_rate", "min_decision_rate")
_EVIDENCE_KEYS = (
    "artifact_kind",
    "schema_version",
    "mode",
    "dataset",
    "identity",
    "partition_integrity",
    "annotations",
    "partitions",
    "audit",
)
_POPULATION_KEYS = ("comments", "ok", "refused", "unavailable", "excluded_language", "eligible")
_AUDIT_KEYS = ("rejected", "excluded", "abstained")
_BUILTIN_DOMAINS = frozenset(item.value for item in DomainType if item is not DomainType.CUSTOM)

_EnumT = TypeVar("_EnumT", bound=StrEnum)


class PolicyScope(StrEnum):
    RUNTIME = "runtime"
    TEST_ONLY = "test_only"


class Eligibility(StrEnum):
    ELIGIBLE = "eligible"
    NOT_ELIGIBLE = "not_eligible"


class ActivationConsumer(StrEnum):
    SEMANTIC_RERANKING = "semantic_reranking"
    SEMANTIC_GROUPING = "semantic_grouping"


class CheckStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    NOT_RUN = "not_run"


@dataclass(frozen=True)
class PolicyReason:
    code: str
    check: str
    field: str | None
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "check": self.check, "field": self.field, "detail": self.detail}


@dataclass(frozen=True)
class PolicyCheckResult:
    eligibility: Eligibility
    scope: PolicyScope | None
    reasons: tuple[PolicyReason, ...]
    # Deterministic check document; it carries no timestamps and no local paths.
    document: dict[str, Any]
    # The evaluation path the check read; used for output-location checks, never published.
    evaluation_path: Path

    @property
    def activation_permitted(self) -> bool:
        return self.eligibility is Eligibility.ELIGIBLE and self.scope is PolicyScope.RUNTIME


@dataclass(frozen=True)
class PolicyCheckOutcome:
    policy_check_path: Path
    policy_check_sha256: str
    eligibility: Eligibility
    scope: PolicyScope | None
    activation_permitted: bool
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_check_path": str(self.policy_check_path),
            "policy_check_sha256": self.policy_check_sha256,
            "eligibility": self.eligibility.value,
            "scope": None if self.scope is None else self.scope.value,
            "activation_permitted": self.activation_permitted,
            "reasons": list(self.reason_codes),
        }


class _Refusal(Exception):
    """One reason that stops loading an input; converted to a PolicyReason in this module."""

    def __init__(self, code: str, field: str | None, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.field = field
        self.detail = detail


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _is_nonfinite(value: Any) -> bool:
    return isinstance(value, float) and not math.isfinite(value)


def _is_text(value: Any) -> TypeGuard[str]:
    return isinstance(value, str) and bool(value.strip())


def _joined(prefix: str, key: str) -> str:
    return f"{prefix}.{key}" if prefix else key


@dataclass(frozen=True)
class _Reader:
    """Typed access to a decoded JSON object; every failure is a refusal with ``code``."""

    code: str

    def fail(self, field: str | None, detail: str) -> _Refusal:
        return _Refusal(self.code, field, detail)

    def member(self, container: Mapping[str, Any], key: str, prefix: str) -> Any:
        if key not in container:
            raise self.fail(_joined(prefix, key), "is missing")
        return container[key]

    def exact_keys(self, container: Mapping[str, Any], keys: Sequence[str], field: str) -> None:
        unknown = sorted(set(container) - set(keys))
        if unknown:
            raise self.fail(_joined(field, unknown[0]), "is not a known key")
        for key in keys:
            self.member(container, key, field)

    def as_object(self, value: Any, field: str) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise self.fail(field, "must be a JSON object")
        return value

    def obj(self, container: Mapping[str, Any], key: str, prefix: str) -> dict[str, Any]:
        return self.as_object(self.member(container, key, prefix), _joined(prefix, key))

    def items(self, container: Mapping[str, Any], key: str, prefix: str) -> list[Any]:
        value = self.member(container, key, prefix)
        if not isinstance(value, list):
            raise self.fail(_joined(prefix, key), "must be a JSON list")
        return value

    def text(self, container: Mapping[str, Any], key: str, prefix: str) -> str:
        value = self.member(container, key, prefix)
        if not isinstance(value, str) or not value.strip():
            raise self.fail(_joined(prefix, key), "must be a non-empty string")
        return value

    def sha256(self, container: Mapping[str, Any], key: str, prefix: str) -> str:
        value = self.text(container, key, prefix)
        if _SHA256_PATTERN.fullmatch(value) is None:
            raise self.fail(_joined(prefix, key), "must be 64 lowercase hex characters")
        return value

    def count(self, container: Mapping[str, Any], key: str, prefix: str) -> int:
        value = self.member(container, key, prefix)
        if not _is_int(value) or value < 0:
            raise self.fail(_joined(prefix, key), "must be a non-negative integer")
        return int(value)

    def rate(self, container: Mapping[str, Any], key: str, prefix: str) -> float | None:
        """A number or null; non-finite values are kept for the finite_values check."""
        value = self.member(container, key, prefix)
        if value is None:
            return None
        if not _is_number(value):
            raise self.fail(_joined(prefix, key), "must be a number or null")
        try:
            return float(value)
        except OverflowError as error:
            raise self.fail(_joined(prefix, key), "must be a number or null") from error

    def choice(
        self, container: Mapping[str, Any], key: str, prefix: str, enum_type: type[_EnumT]
    ) -> _EnumT:
        value = self.member(container, key, prefix)
        if not isinstance(value, str) or value not in {item.value for item in enum_type}:
            raise self.fail(_joined(prefix, key), f"is not a known {enum_type.__name__} value")
        return enum_type(value)


_POLICY_READER = _Reader("policy_invalid")
_EVIDENCE_READER = _Reader("evidence_invalid")


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def _decode_json_object(raw: bytes, reader: _Reader, what: str) -> dict[str, Any]:
    """Decode UTF-8 JSON refusing duplicate keys while keeping non-finite numbers.

    Unlike ``decode_strict_json_object``, the ``NaN``, ``Infinity`` and ``-Infinity`` tokens
    and overflowing literals such as ``1e999`` decode to non-finite floats, so the checker can
    report them as ``nonfinite_value`` instead of refusing the whole input.
    """
    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_pairs)
        # An unpaired surrogate escape decodes to text that cannot be re-encoded as UTF-8.
        json.dumps(payload, ensure_ascii=False).encode("utf-8")
    except (UnicodeError, ValueError, RecursionError) as error:
        raise reader.fail(None, f"{what} is not valid JSON: {error}") from error
    if not isinstance(payload, dict):
        raise reader.fail(None, f"{what} root must be a JSON object")
    return payload


def _read_input(path: Path, *, missing_code: str, reader: _Reader, what: str) -> bytes:
    try:
        return path.read_bytes()
    except FileNotFoundError as error:
        raise _Refusal(missing_code, None, f"the {what} file does not exist") from error
    except OSError as error:
        detail = f"the {what} file could not be read: {type(error).__name__}"
        raise reader.fail(None, detail) from error


@dataclass(frozen=True)
class _Tolerances:
    # Decoded JSON numbers or booleans; the tolerances check decides whether they are valid.
    max_error_rate: int | float
    min_decision_rate: int | float
    min_labeled_decided: int | float

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_error_rate": self.max_error_rate,
            "min_decision_rate": self.min_decision_rate,
            "min_labeled_decided": self.min_labeled_decided,
        }


@dataclass(frozen=True)
class _Policy:
    sha256: str
    policy_id: str
    scope: PolicyScope
    disclosure: str
    question_id: str
    consumer: ActivationConsumer
    # (field, bound value) in binding order; None and "" are unbound.
    bindings: tuple[tuple[str, str | None], ...]
    tolerances: _Tolerances

    def to_dict(self) -> dict[str, Any]:
        return {
            "sha256": self.sha256,
            "policy_id": self.policy_id,
            "scope": self.scope.value,
            "disclosure": self.disclosure,
            "decision": {"question_id": self.question_id, "consumer": self.consumer.value},
            "bindings": dict(self.bindings),
            "tolerances": self.tolerances.to_dict(),
        }


def _policy_version(document: Mapping[str, Any]) -> None:
    reader = _POLICY_READER
    if document.get("artifact_kind") != POLICY_KIND:
        raise reader.fail("artifact_kind", f"must be {POLICY_KIND}")
    version = document.get("schema_version")
    if not _is_int(version):
        raise reader.fail("schema_version", "must be an integer")
    if version != POLICY_SCHEMA_VERSION:
        detail = f"schema_version {version} is unsupported"
        raise _Refusal("policy_version_unsupported", "schema_version", detail)


def _policy_bindings(document: Mapping[str, Any]) -> tuple[tuple[str, str | None], ...]:
    reader = _POLICY_READER
    bindings = reader.obj(document, "bindings", "")
    reader.exact_keys(bindings, tuple(_STALE_CODES), "bindings")
    for field, value in bindings.items():
        if value is not None and not isinstance(value, str):
            raise reader.fail(f"bindings.{field}", "must be a string or null")
    return tuple((field, bindings[field]) for field in _STALE_CODES)


def _policy_tolerances(document: Mapping[str, Any]) -> _Tolerances:
    reader = _POLICY_READER
    tolerances = reader.obj(document, "tolerances", "")
    reader.exact_keys(tolerances, _TOLERANCE_KEYS, "tolerances")
    for key in _TOLERANCE_KEYS:
        # Booleans pass here so that the tolerances check reports them as tolerance_invalid.
        if not isinstance(tolerances[key], int | float):
            raise reader.fail(f"tolerances.{key}", "must be a JSON number")
    return _Tolerances(
        max_error_rate=tolerances["max_error_rate"],
        min_decision_rate=tolerances["min_decision_rate"],
        min_labeled_decided=tolerances["min_labeled_decided"],
    )


def _load_policy(path: Path) -> _Policy:
    reader = _POLICY_READER
    raw = _read_input(path, missing_code="policy_invalid", reader=reader, what="policy")
    document = _decode_json_object(raw, reader, "the policy")
    _policy_version(document)
    reader.exact_keys(document, _POLICY_KEYS, "")
    scope = reader.choice(document, "scope", "", PolicyScope)
    disclosure = reader.text(document, "disclosure", "")
    disclosed = all(pattern.search(disclosure) for pattern in TEST_ONLY_DISCLOSURE_PATTERNS)
    if scope is PolicyScope.TEST_ONLY and not disclosed:
        detail = "a test_only disclosure must state it is constructed for tests"
        raise reader.fail("disclosure", detail)
    decision = reader.obj(document, "decision", "")
    reader.exact_keys(decision, _DECISION_KEYS, "decision")
    return _Policy(
        sha256=hashlib.sha256(raw).hexdigest(),
        policy_id=reader.text(document, "policy_id", ""),
        scope=scope,
        disclosure=disclosure,
        question_id=reader.text(decision, "question_id", "decision"),
        consumer=reader.choice(decision, "consumer", "decision", ActivationConsumer),
        bindings=_policy_bindings(document),
        tolerances=_policy_tolerances(document),
    )


@dataclass(frozen=True)
class _Population:
    comments: int
    ok: int
    refused: int
    unavailable: int
    excluded_language: int
    eligible: int


@dataclass(frozen=True)
class _QuestionMetrics:
    population: int
    eligible: int
    abstained: int
    decided: int
    decision_rate: float | None
    labels_status: str
    labeled_decided: int
    correct: int
    incorrect: int
    error_rate: float | None
    # The number of entries in labels.incorrect_items.
    incorrect_items: int


@dataclass(frozen=True)
class _Replay:
    status: str
    replayed_items: int
    matched_items: int


@dataclass(frozen=True)
class _Audit:
    """Audit entries of the held-out partition only, as recorded in the evidence."""

    rejected: tuple[dict[str, Any], ...]
    excluded: tuple[dict[str, Any], ...]
    abstained: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class _Evidence:
    sha256: str
    dataset_manifest_sha256: str
    labels_sha256: str
    label_origin: LabelOriginKind
    objective: ObjectiveType
    domain: str
    supported_languages: tuple[str, ...]
    identity: IdentityBindings
    integrity_status: str
    replays: tuple[_Replay, ...]
    population: _Population
    questions: Mapping[str, _QuestionMetrics]
    audit: _Audit

    def bound_values(self) -> dict[str, str]:
        return {
            **self.identity.to_dict(),
            "dataset_manifest_sha256": self.dataset_manifest_sha256,
            "labels_sha256": self.labels_sha256,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "evaluation_sha256": self.sha256,
            "dataset_manifest_sha256": self.dataset_manifest_sha256,
            "labels_sha256": self.labels_sha256,
            "label_origin": self.label_origin.value,
            "independent_adjudication": (
                self.label_origin is LabelOriginKind.INDEPENDENT_ADJUDICATION
            ),
            "objective": self.objective.value,
            "domain": self.domain,
        }


def _evidence_header(document: Mapping[str, Any]) -> None:
    reader = _EVIDENCE_READER
    if document.get("artifact_kind") != EVALUATION_ARTIFACT_KIND:
        raise reader.fail("artifact_kind", f"must be {EVALUATION_ARTIFACT_KIND}")
    version = document.get("schema_version")
    if not _is_int(version) or version != EVALUATION_SCHEMA_VERSION:
        raise reader.fail("schema_version", f"must be {EVALUATION_SCHEMA_VERSION}")
    for key in _EVIDENCE_KEYS:
        reader.member(document, key, "")
    if document["mode"] != MODE:
        raise reader.fail("mode", f"must be {MODE}")


def _label_origin(labels: Mapping[str, Any]) -> LabelOriginKind:
    reader = _EVIDENCE_READER
    origin = reader.obj(labels, "origin", "dataset.labels")
    kind = reader.choice(origin, "kind", "dataset.labels.origin", LabelOriginKind)
    independent = reader.member(origin, "independent_adjudication", "dataset.labels.origin")
    if independent is not (kind is LabelOriginKind.INDEPENDENT_ADJUDICATION):
        raise reader.fail(
            "dataset.labels.origin.independent_adjudication", "contradicts the origin kind"
        )
    return kind


def _supported_languages(dataset: Mapping[str, Any]) -> tuple[str, ...]:
    reader = _EVIDENCE_READER
    languages = reader.items(dataset, "supported_languages", "dataset")
    for index, language in enumerate(languages):
        if not _is_text(language):
            field = f"dataset.supported_languages[{index}]"
            raise reader.fail(field, "must be a non-empty string")
    return tuple(languages)


def _replays(document: Mapping[str, Any]) -> tuple[_Replay, ...]:
    reader = _EVIDENCE_READER
    replays: list[_Replay] = []
    for index, raw in enumerate(reader.items(document, "annotations", "")):
        prefix = f"annotations[{index}]"
        replay = reader.obj(reader.as_object(raw, prefix), "replay", prefix)
        replays.append(
            _Replay(
                status=reader.text(replay, "status", f"{prefix}.replay"),
                replayed_items=reader.count(replay, "replayed_items", f"{prefix}.replay"),
                matched_items=reader.count(replay, "matched_items", f"{prefix}.replay"),
            )
        )
    return tuple(replays)


def _question_metrics(raw: Any, prefix: str) -> _QuestionMetrics:
    reader = _EVIDENCE_READER
    metrics = reader.as_object(raw, prefix)
    labels = reader.obj(metrics, "labels", prefix)
    return _QuestionMetrics(
        population=reader.count(metrics, "population", prefix),
        eligible=reader.count(metrics, "eligible", prefix),
        abstained=reader.count(metrics, "abstained", prefix),
        decided=reader.count(metrics, "decided", prefix),
        decision_rate=reader.rate(metrics, "decision_rate", prefix),
        labels_status=reader.text(labels, "status", f"{prefix}.labels"),
        labeled_decided=reader.count(labels, "labeled_decided", f"{prefix}.labels"),
        correct=reader.count(labels, "correct", f"{prefix}.labels"),
        incorrect=reader.count(labels, "incorrect", f"{prefix}.labels"),
        error_rate=reader.rate(labels, "error_rate", f"{prefix}.labels"),
        incorrect_items=len(reader.items(labels, "incorrect_items", f"{prefix}.labels")),
    )


def _held_out_block(
    document: Mapping[str, Any],
) -> tuple[_Population, dict[str, _QuestionMetrics]]:
    reader = _EVIDENCE_READER
    prefix = f"partitions.{HELD_OUT_PARTITION.value}"
    partitions = reader.obj(document, "partitions", "")
    block = reader.obj(partitions, HELD_OUT_PARTITION.value, "partitions")
    population = reader.obj(block, "population", prefix)
    counts = {
        key: reader.count(population, key, f"{prefix}.population") for key in _POPULATION_KEYS
    }
    questions = reader.obj(block, "questions", prefix)
    metrics = {
        question_id: _question_metrics(raw, f"{prefix}.questions.{question_id}")
        for question_id, raw in questions.items()
    }
    return _Population(**counts), metrics


def _held_out_audit(document: Mapping[str, Any]) -> _Audit:
    reader = _EVIDENCE_READER
    audit = reader.obj(document, "audit", "")
    kept: dict[str, tuple[dict[str, Any], ...]] = {}
    for name in _AUDIT_KEYS:
        entries: list[dict[str, Any]] = []
        for index, raw in enumerate(reader.items(audit, name, "audit")):
            field = f"audit.{name}[{index}]"
            entry = reader.as_object(raw, field)
            if name == "abstained":
                reader.text(entry, "question_id", field)
            if reader.text(entry, "partition", field) == HELD_OUT_PARTITION.value:
                entries.append(entry)
        kept[name] = tuple(entries)
    return _Audit(rejected=kept["rejected"], excluded=kept["excluded"], abstained=kept["abstained"])


def _load_evidence(path: Path) -> _Evidence:
    reader = _EVIDENCE_READER
    raw = _read_input(path, missing_code="evidence_missing", reader=reader, what="evaluation")
    digest = hashlib.sha256(raw).hexdigest()
    if path.suffix != ".json" or path.stem != digest:
        detail = f"the file name is not the sha256 of its bytes ({digest}) plus .json"
        raise _Refusal("evidence_digest_mismatch", None, detail)
    document = _decode_json_object(raw, reader, "the evaluation document")
    _evidence_header(document)
    dataset = reader.obj(document, "dataset", "")
    domain = reader.text(dataset, "domain", "dataset")
    if domain not in _BUILTIN_DOMAINS:
        raise reader.fail("dataset.domain", "is not a built-in domain")
    labels = reader.obj(dataset, "labels", "dataset")
    identity = reader.obj(document, "identity", "")
    population, questions = _held_out_block(document)
    return _Evidence(
        sha256=digest,
        dataset_manifest_sha256=reader.sha256(dataset, "manifest_sha256", "dataset"),
        labels_sha256=reader.sha256(labels, "sha256", "dataset.labels"),
        label_origin=_label_origin(labels),
        objective=reader.choice(dataset, "objective", "dataset", ObjectiveType),
        domain=domain,
        supported_languages=_supported_languages(dataset),
        identity=IdentityBindings(
            **{field: reader.text(identity, field, "identity") for field in _IDENTITY_FIELDS}
        ),
        integrity_status=reader.text(
            reader.obj(document, "partition_integrity", ""), "status", "partition_integrity"
        ),
        replays=_replays(document),
        population=population,
        questions=questions,
        audit=_held_out_audit(document),
    )


@dataclass(frozen=True)
class _Inputs:
    policy_declared: bool
    policy: _Policy | None
    policy_reasons: tuple[PolicyReason, ...]
    evidence: _Evidence | None
    evidence_reasons: tuple[PolicyReason, ...]
    current: IdentityBindings | None
    # The policy question's metrics in the held-out partition, when both inputs are loaded.
    held_out: _QuestionMetrics | None


_CheckOutput = list[PolicyReason] | None
_Check = Callable[[_Inputs, Mapping[str, CheckStatus]], _CheckOutput]


def _refusal_reason(refusal: _Refusal, check: str) -> PolicyReason:
    return PolicyReason(refusal.code, check, refusal.field, refusal.detail)


def _policy_input(policy_path: Path | None) -> tuple[_Policy | None, tuple[PolicyReason, ...]]:
    if policy_path is None:
        reason = PolicyReason("policy_not_declared", "policy", None, "no activation policy given")
        return None, (reason,)
    try:
        return _load_policy(policy_path), ()
    except _Refusal as refusal:
        return None, (_refusal_reason(refusal, "policy"),)


def _evidence_input(path: Path) -> tuple[_Evidence | None, tuple[PolicyReason, ...]]:
    try:
        return _load_evidence(path), ()
    except _Refusal as refusal:
        return None, (_refusal_reason(refusal, "evidence"),)


def _current_identity(runtime_manifest: RuntimeManifest, evidence: _Evidence) -> IdentityBindings:
    question_set = build_question_set(evidence.objective, load_domain_vocabulary(evidence.domain))
    return _current_bindings(runtime_manifest, question_set)


def _policy_check(inputs: _Inputs, statuses: Mapping[str, CheckStatus]) -> _CheckOutput:
    return list(inputs.policy_reasons)


def _evidence_check(inputs: _Inputs, statuses: Mapping[str, CheckStatus]) -> _CheckOutput:
    return list(inputs.evidence_reasons)


def _runtime_identity_check(inputs: _Inputs, statuses: Mapping[str, CheckStatus]) -> _CheckOutput:
    if inputs.evidence is None or inputs.current is None:
        return None
    current = inputs.current.to_dict()
    reasons = [
        PolicyReason(
            _STALE_CODES[field],
            "runtime_identity",
            f"identity.{field}",
            f"evidence {field} {bound!r} differs from the current runtime {current[field]!r}",
        )
        for field, bound in inputs.evidence.identity.to_dict().items()
        if bound != current[field]
    ]
    # The held-out eligible population is stratified by the dataset's supported languages; any
    # language other than the bound one was scored without a runtime language binding.
    languages, language = inputs.evidence.supported_languages, inputs.evidence.identity.language
    if languages != (language,):
        detail = f"supported languages {list(languages)!r} are not exactly [{language!r}]"
        field = "dataset.supported_languages"
        reasons.append(PolicyReason("stale_language_binding", "runtime_identity", field, detail))
    return reasons


def _policy_bindings_check(inputs: _Inputs, statuses: Mapping[str, CheckStatus]) -> _CheckOutput:
    if inputs.policy is None or inputs.evidence is None:
        return None
    observed = inputs.evidence.bound_values()
    reasons: list[PolicyReason] = []
    for field, bound in inputs.policy.bindings:
        name = f"bindings.{field}"
        if bound is None or bound == "":
            reasons.append(PolicyReason("unbound_binding", "policy_bindings", name, "is unbound"))
        elif bound != observed[field]:
            detail = f"policy binds {bound!r} but the evidence has {observed[field]!r}"
            reasons.append(PolicyReason(_STALE_CODES[field], "policy_bindings", name, detail))
    return reasons


def _label_provenance_check(inputs: _Inputs, statuses: Mapping[str, CheckStatus]) -> _CheckOutput:
    """Runtime scope, or no policy, accepts only independent adjudication; test_only scope
    accepts only a constructed test fixture. This is the only scope-dependent check."""
    if inputs.evidence is None or (inputs.policy_declared and inputs.policy is None):
        return None
    scope = PolicyScope.RUNTIME if inputs.policy is None else inputs.policy.scope
    origin = inputs.evidence.label_origin
    if scope is PolicyScope.TEST_ONLY:
        accepted, code = LabelOriginKind.CONSTRUCTED_TEST_FIXTURE, "test_fixture_origin_mismatch"
    else:
        accepted, code = LabelOriginKind.INDEPENDENT_ADJUDICATION, "held_out_labels_not_independent"
    if origin is accepted:
        return []
    detail = f"{scope.value} scope requires {accepted.value} labels; evidence has {origin.value}"
    return [PolicyReason(code, "label_provenance", "dataset.labels.origin.kind", detail)]


def _evidence_integrity_check(inputs: _Inputs, statuses: Mapping[str, CheckStatus]) -> _CheckOutput:
    if inputs.evidence is None:
        return None
    reasons: list[PolicyReason] = []
    if inputs.evidence.integrity_status != "passed":
        detail = f"partition integrity status is {inputs.evidence.integrity_status!r}"
        reasons.append(
            PolicyReason(
                "partition_integrity_failed", "evidence_integrity", "partition_integrity", detail
            )
        )
    for index, replay in enumerate(inputs.evidence.replays):
        if replay.status != "reproduced" or replay.matched_items != replay.replayed_items:
            detail = (
                f"replay status {replay.status!r} matched {replay.matched_items} "
                f"of {replay.replayed_items} replayed items"
            )
            field = f"annotations[{index}].replay"
            reason = PolicyReason("replay_not_reproduced", "evidence_integrity", field, detail)
            reasons.append(reason)
    return reasons


def _held_out_coverage_check(inputs: _Inputs, statuses: Mapping[str, CheckStatus]) -> _CheckOutput:
    if inputs.policy is None or inputs.evidence is None:
        return None
    field = f"partitions.{HELD_OUT_PARTITION.value}.questions.{inputs.policy.question_id}"
    metrics = inputs.held_out
    if metrics is None:
        detail = "the policy question is absent from the held-out partition"
        return [PolicyReason("decision_question_unbound", "held_out_coverage", field, detail)]
    reasons: list[PolicyReason] = []
    if metrics.eligible == 0 or metrics.decision_rate is None:
        detail = f"eligible {metrics.eligible}, decision_rate {metrics.decision_rate}"
        reasons.append(PolicyReason("coverage_unsupported", "held_out_coverage", field, detail))
    if (
        metrics.labels_status != "available"
        or metrics.labeled_decided == 0
        or metrics.error_rate is None
    ):
        status, labeled = metrics.labels_status, metrics.labeled_decided
        detail = f"labels status {status!r}, labeled_decided {labeled}"
        reasons.append(
            PolicyReason("held_out_labels_missing", "held_out_coverage", f"{field}.labels", detail)
        )
    return reasons


def _finite_values_check(inputs: _Inputs, statuses: Mapping[str, CheckStatus]) -> _CheckOutput:
    if inputs.policy is None or inputs.held_out is None:
        return None
    tolerances = inputs.policy.tolerances
    values: tuple[tuple[str, Any], ...] = (
        *((f"tolerances.{key}", tolerances.to_dict()[key]) for key in _TOLERANCE_KEYS),
        ("held_out.decision_rate", inputs.held_out.decision_rate),
        ("held_out.error_rate", inputs.held_out.error_rate),
    )
    return [
        PolicyReason("nonfinite_value", "finite_values", field, f"{value} is not finite")
        for field, value in values
        if _is_nonfinite(value)
    ]


def _tolerance_validity(tolerances: _Tolerances) -> list[PolicyReason]:
    """Non-finite tolerances are left to the finite_values check so they are reported once."""
    reasons: list[PolicyReason] = []
    for key in _RATE_TOLERANCES:
        value = tolerances.to_dict()[key]
        if _is_nonfinite(value):
            continue
        if not _is_number(value) or not 0 <= value <= 1:
            detail = f"{value!r} is not a rate in [0, 1]"
            field = f"tolerances.{key}"
            reasons.append(PolicyReason("tolerance_invalid", "tolerances", field, detail))
    minimum = tolerances.min_labeled_decided
    if not _is_nonfinite(minimum) and (not _is_int(minimum) or minimum < 1):
        detail = f"{minimum!r} is not a positive integer"
        field = "tolerances.min_labeled_decided"
        reasons.append(PolicyReason("tolerance_invalid", "tolerances", field, detail))
    return reasons


def _tolerances_check(inputs: _Inputs, statuses: Mapping[str, CheckStatus]) -> _CheckOutput:
    """Validate the declared tolerances whenever a policy loaded; compare them with held-out
    metrics only once held-out coverage and finite values passed."""
    if inputs.policy is None:
        return None
    tolerances = inputs.policy.tolerances
    invalid = _tolerance_validity(tolerances)
    if invalid:
        return invalid
    metrics = inputs.held_out
    prerequisites = ("held_out_coverage", "finite_values")
    usable = all(statuses[name] is CheckStatus.PASSED for name in prerequisites)
    if metrics is None or not usable:
        return None
    error_rate, decision_rate = metrics.error_rate, metrics.decision_rate
    if error_rate is None or decision_rate is None:
        return None
    labeled = metrics.labeled_decided
    comparisons = (
        (
            "error_above_tolerance",
            "max_error_rate",
            error_rate > tolerances.max_error_rate,
            f"error_rate {error_rate} > {tolerances.max_error_rate}",
        ),
        (
            "coverage_below_tolerance",
            "min_decision_rate",
            decision_rate < tolerances.min_decision_rate,
            f"decision_rate {decision_rate} < {tolerances.min_decision_rate}",
        ),
        (
            "insufficient_labeled_items",
            "min_labeled_decided",
            labeled < tolerances.min_labeled_decided,
            f"labeled_decided {labeled} < {tolerances.min_labeled_decided}",
        ),
    )
    return [
        PolicyReason(code, "tolerances", f"tolerances.{key}", detail)
        for code, key, violated, detail in comparisons
        if violated
    ]


# (field, detail) of one identity of the held-out evidence that does not hold.
_Problem = tuple[str, str]
_UNRECONCILED = "held-out counts do not reconcile with the evidence audit and population"


def _count_problems(
    evidence: _Evidence, metrics: _QuestionMetrics, question_id: str
) -> list[_Problem]:
    population = evidence.population
    audit = evidence.audit
    abstained = sum(1 for entry in audit.abstained if entry["question_id"] == question_id)
    question = f"partitions.{HELD_OUT_PARTITION.value}.questions.{question_id}"
    rejected = population.refused + population.unavailable
    excluded = population.excluded_language
    identities = (
        ("population.comments", population.comments == population.ok + rejected),
        ("population.eligible", population.eligible == population.ok - excluded),
        ("audit.rejected", len(audit.rejected) == rejected),
        ("audit.excluded", len(audit.excluded) == excluded),
        ("audit.abstained", abstained == metrics.abstained),
        (f"{question}.population", metrics.population == population.comments),
        (f"{question}.eligible", metrics.eligible == population.eligible),
        (f"{question}.decided", metrics.decided + metrics.abstained == metrics.eligible),
    )
    return [(field, _UNRECONCILED) for field, holds in identities if not holds]


def _rate_holds(observed: float | None, expected: float | None) -> bool:
    """A non-finite observed rate is reported by the finite_values check, not here."""
    return _is_nonfinite(observed) or observed == expected


def _rate_problems(metrics: _QuestionMetrics, question_id: str) -> list[_Problem]:
    """Recompute the rates and label counts the tolerances gate on, as PR-1 computes them."""
    question = f"partitions.{HELD_OUT_PARTITION.value}.questions.{question_id}"
    labels = f"{question}.labels"
    decision_rate = _rate(metrics.decided, metrics.population)
    error_rate = _rate(metrics.incorrect, metrics.labeled_decided)
    correct, incorrect = metrics.correct, metrics.incorrect
    identities = (
        (
            f"{question}.decision_rate",
            _rate_holds(metrics.decision_rate, decision_rate),
            f"decision_rate {metrics.decision_rate} is not decided / population {decision_rate}",
        ),
        (
            f"{labels}.labeled_decided",
            metrics.labeled_decided == correct + incorrect,
            f"labeled_decided {metrics.labeled_decided} is not {correct} + {incorrect}",
        ),
        (
            f"{labels}.error_rate",
            _rate_holds(metrics.error_rate, error_rate),
            f"error_rate {metrics.error_rate} is not incorrect / labeled_decided {error_rate}",
        ),
        (
            f"{labels}.incorrect_items",
            metrics.incorrect_items == incorrect,
            f"{metrics.incorrect_items} incorrect items listed for {incorrect} incorrect",
        ),
    )
    return [(field, detail) for field, holds, detail in identities if not holds]


def _audit_key(entry: Mapping[str, Any]) -> tuple[str, str] | None:
    thread_id, comment_id = entry.get("thread_id"), entry.get("comment_id")
    if not _is_text(thread_id) or not _is_text(comment_id):
        return None
    return thread_id, comment_id


def _is_reason_list(value: Any) -> bool:
    return isinstance(value, list) and bool(value) and all(isinstance(r, str) for r in value)


def _audit_kind_problem(name: str, entries: Sequence[Mapping[str, Any]]) -> str | None:
    keys: list[tuple[str, ...]] = []
    for entry in entries:
        key = _audit_key(entry)
        if key is None:
            return "an entry lacks a non-empty string thread_id or comment_id"
        if name == "rejected" and not _is_reason_list(entry.get("reasons")):
            return "a rejected entry lacks a non-empty list of string reasons"
        if name == "abstained":
            keys.append((entry["question_id"], *key))
        else:
            keys.append(key)
    if len(set(keys)) != len(keys):
        return "an entry is recorded more than once"
    return None


def _audit_problems(audit: _Audit) -> list[_Problem]:
    """Validate the held-out audit entries the counts are reconciled against."""
    kinds = (
        ("rejected", audit.rejected),
        ("excluded", audit.excluded),
        ("abstained", audit.abstained),
    )
    problems = [
        (f"audit.{name}", problem)
        for name, entries in kinds
        if (problem := _audit_kind_problem(name, entries)) is not None
    ]
    rejected = {_audit_key(entry) for entry in audit.rejected} - {None}
    excluded = {_audit_key(entry) for entry in audit.excluded} - {None}
    if rejected & excluded:
        problems.append(("audit.excluded", "a comment is recorded as both rejected and excluded"))
    return problems


def _reconciliation_check(inputs: _Inputs, statuses: Mapping[str, CheckStatus]) -> _CheckOutput:
    """Recompute the held-out partition's count and rate identities and validate its audit
    entries from the evidence itself; no reported total or rate is trusted."""
    metrics = inputs.held_out
    if inputs.policy is None or inputs.evidence is None or metrics is None:
        return None
    question_id = inputs.policy.question_id
    problems = (
        _count_problems(inputs.evidence, metrics, question_id)
        + _rate_problems(metrics, question_id)
        + _audit_problems(inputs.evidence.audit)
    )
    return [
        PolicyReason("evidence_counts_unreconciled", "reconciliation", field, detail)
        for field, detail in problems
    ]


_CHECKS: tuple[tuple[str, _Check], ...] = (
    ("policy", _policy_check),
    ("evidence", _evidence_check),
    ("runtime_identity", _runtime_identity_check),
    ("policy_bindings", _policy_bindings_check),
    ("label_provenance", _label_provenance_check),
    ("evidence_integrity", _evidence_integrity_check),
    ("held_out_coverage", _held_out_coverage_check),
    ("finite_values", _finite_values_check),
    ("tolerances", _tolerances_check),
    ("reconciliation", _reconciliation_check),
)

# Checks run, and are reported, in exactly this order.
CHECK_ORDER = tuple(name for name, _ in _CHECKS)


def _json_safe(value: Any) -> Any:
    """Record non-finite floats as their string form so the document stays strict JSON."""
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return value


def _held_out_document(inputs: _Inputs) -> dict[str, Any] | None:
    metrics = inputs.held_out
    if inputs.policy is None or inputs.evidence is None or metrics is None:
        return None
    question_id = inputs.policy.question_id
    audit = inputs.evidence.audit
    return {
        "partition": HELD_OUT_PARTITION.value,
        "question_id": question_id,
        "population": metrics.population,
        "eligible": metrics.eligible,
        "abstained": metrics.abstained,
        "decided": metrics.decided,
        "decision_rate": metrics.decision_rate,
        "labeled_decided": metrics.labeled_decided,
        "error_rate": metrics.error_rate,
        "audit": {
            "rejected": list(audit.rejected),
            "excluded": list(audit.excluded),
            "abstained": [e for e in audit.abstained if e["question_id"] == question_id],
        },
    }


def _document(
    inputs: _Inputs,
    checks: Sequence[tuple[str, CheckStatus]],
    reasons: Sequence[PolicyReason],
    eligibility: Eligibility,
) -> dict[str, Any]:
    scope = None if inputs.policy is None else inputs.policy.scope
    permitted = eligibility is Eligibility.ELIGIBLE and scope is PolicyScope.RUNTIME
    document = {
        "artifact_kind": POLICY_CHECK_KIND,
        "schema_version": POLICY_SCHEMA_VERSION,
        "mode": MODE,
        "eligibility": eligibility.value,
        "scope": None if scope is None else scope.value,
        "activation": {"permitted": permitted, "performed": False, "note": ACTIVATION_NOTE},
        "policy": None if inputs.policy is None else inputs.policy.to_dict(),
        "evidence": None if inputs.evidence is None else inputs.evidence.to_dict(),
        "current_identity": None if inputs.current is None else inputs.current.to_dict(),
        "checks": [{"name": name, "status": status.value} for name, status in checks],
        "reasons": [reason.to_dict() for reason in reasons],
        "held_out": _held_out_document(inputs),
    }
    safe: dict[str, Any] = _json_safe(document)
    return safe


def _status(found: _CheckOutput) -> CheckStatus:
    if found is None:
        return CheckStatus.NOT_RUN
    return CheckStatus.FAILED if found else CheckStatus.PASSED


def check_activation_policy(
    *, policy_path: Path | None, evaluation_path: Path, runtime_manifest: RuntimeManifest
) -> PolicyCheckResult:
    """Run every applicable check in fixed order; eligible iff no check reports a reason.

    Check failures are reasons in the result, never exceptions. The current runtime identity
    is computed from ``runtime_manifest`` and the evidence objective and domain only when the
    evidence loaded.
    """
    policy, policy_reasons = _policy_input(policy_path)
    evidence, evidence_reasons = _evidence_input(evaluation_path)
    held_out: _QuestionMetrics | None = None
    if policy is not None and evidence is not None:
        held_out = evidence.questions.get(policy.question_id)
    inputs = _Inputs(
        policy_declared=policy_path is not None,
        policy=policy,
        policy_reasons=policy_reasons,
        evidence=evidence,
        evidence_reasons=evidence_reasons,
        current=None if evidence is None else _current_identity(runtime_manifest, evidence),
        held_out=held_out,
    )
    statuses: dict[str, CheckStatus] = {}
    reasons: list[PolicyReason] = []
    for name, check in _CHECKS:
        found = check(inputs, statuses)
        statuses[name] = _status(found)
        reasons.extend(found or ())
    every_check_passed = all(status is CheckStatus.PASSED for status in statuses.values())
    eligibility = (
        Eligibility.ELIGIBLE if not reasons and every_check_passed else Eligibility.NOT_ELIGIBLE
    )
    return PolicyCheckResult(
        eligibility=eligibility,
        scope=None if policy is None else policy.scope,
        reasons=tuple(reasons),
        document=_document(inputs, tuple(statuses.items()), reasons, eligibility),
        evaluation_path=evaluation_path,
    )


def policy_check_bytes(document: dict[str, Any]) -> bytes:
    return decision_bytes(document)


def publish_policy_check(
    result: PolicyCheckResult,
    *,
    output_root: Path,
    protected_roots: tuple[Path, ...],
    input_paths: tuple[Path, ...],
) -> PolicyCheckOutcome:
    """Publish ``<output_root>/policy-checks/<sha256>.json``; identical bytes are reused."""
    root = check_output_location(output_root, result.evaluation_path, protected_roots)
    for input_path in input_paths:
        check_output_location(output_root, input_path, protected_roots)
    content = policy_check_bytes(result.document)
    digest = hashlib.sha256(content).hexdigest()
    directory = root / _POLICY_CHECKS_DIRNAME
    # The check directory is a generated descendant of the canonical root; a link there could
    # redirect publication.
    if directory.is_symlink():
        raise DecisionArtifactError(
            "policy check directories must not be symbolic links",
            details={"reason": "artifact_directory_symlink", "path": str(directory)},
        )
    path = directory / f"{digest}.json"
    for input_path in (result.evaluation_path, *input_paths):
        refuse_alias(path, input_path)
    publish_immutable(path, content, reuse_identical=True)
    return PolicyCheckOutcome(
        policy_check_path=path,
        policy_check_sha256=digest,
        eligibility=result.eligibility,
        scope=result.scope,
        activation_permitted=result.activation_permitted,
        reason_codes=tuple(reason.code for reason in result.reasons),
    )
