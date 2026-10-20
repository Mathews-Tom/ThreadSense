"""Contracts for offline assessment of saved typed decision artifacts.

An assessment replays frozen, content-addressed decision documents against a frozen
dataset manifest whose labels carry an explicit, disclosed origin. Nothing here calls
the typed-decision service, alters a saved artifact or turns an uncalibrated quantity
into a probability of correctness. Engineering labels authored for boundary cases are
never independent human adjudication, and the metrics they produce describe those
cases only, not population accuracy.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from threadsense.contracts import ObjectiveType
from threadsense.decisions.contracts import CoverageSummary, ItemStatus, QuestionType
from threadsense.models.canonical import Thread

EVALUATION_MANIFEST_KIND = "typed_decision_evaluation_manifest"
EVALUATION_LABELS_KIND = "typed_decision_evaluation_labels"
EVALUATION_ARTIFACT_KIND = "typed_decision_evaluation"
EVALUATION_RUN_ARTIFACT_KIND = "typed_decision_evaluation_run"
EVALUATION_SCHEMA_VERSION = 1

# Explicit decision rules. A score answer's `score` is an expected ordinal level and a
# noul answer's value is P(true); neither is a probability that the answer is correct.
SCORE_DECISION_RULE = "argmax level of the reported distribution; ties resolve to the lowest level"
NOUL_TRUE_THRESHOLD = 0.5
NOUL_DECISION_RULE = "true when the reported P(true) >= 0.5"
ABSTENTION_RULE = "a choice answer of other_or_uncertain is an abstention, not a decision"
LANGUAGE_EXCLUSION_REASON = "language_unsupported"


class Partition(StrEnum):
    DEVELOPMENT = "development"
    CALIBRATION = "calibration"
    EVALUATION = "evaluation"


class LabelOriginKind(StrEnum):
    """Who produced a label set. Only `independent_adjudication` can ever back activation."""

    ENGINEERING_AUTHORED = "engineering_authored"
    MODEL_GENERATED = "model_generated"
    INDEPENDENT_ADJUDICATION = "independent_adjudication"
    CONSTRUCTED_TEST_FIXTURE = "constructed_test_fixture"


class CaseCategory(StrEnum):
    BASELINE = "baseline"
    NEGATION = "negation"
    QUOTED_CLAIM = "quoted_claim"
    MIXED_INTENT = "mixed_intent"
    REPETITION = "repetition"
    LONG_CONTEXT_BOUNDARY = "long_context_boundary"
    OPTION_RELABELING = "option_relabeling"
    UNSUPPORTED_LANGUAGE = "unsupported_language"


@dataclass(frozen=True)
class IdentityBindings:
    """Identities a frozen dataset manifest and every decision it assesses must share."""

    runtime_manifest_sha256: str
    checkpoint_revision: str
    question_set_sha256: str
    settings_sha256: str
    context_policy_sha256: str
    language: str

    def to_dict(self) -> dict[str, str]:
        return {
            "runtime_manifest_sha256": self.runtime_manifest_sha256,
            "checkpoint_revision": self.checkpoint_revision,
            "question_set_sha256": self.question_set_sha256,
            "settings_sha256": self.settings_sha256,
            "context_policy_sha256": self.context_policy_sha256,
            "language": self.language,
        }


@dataclass(frozen=True)
class SamplingLimits:
    method: str
    selection: str
    max_threads: int
    max_comments_per_thread: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "selection": self.selection,
            "max_threads": self.max_threads,
            "max_comments_per_thread": self.max_comments_per_thread,
        }


@dataclass(frozen=True)
class FileBinding:
    """A manifest-relative POSIX path plus the sha256 of its exact bytes."""

    path: str
    sha256: str

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "sha256": self.sha256}


@dataclass(frozen=True)
class ManifestThread:
    thread_id: str
    file: FileBinding
    partition: Partition
    source_language: str
    # Declared per-comment language overrides, (comment_id, language), in manifest order.
    comment_languages: tuple[tuple[str, str], ...]

    def language_of(self, comment_id: str) -> str:
        for candidate, language in self.comment_languages:
            if candidate == comment_id:
                return language
        return self.source_language


@dataclass(frozen=True)
class IssueGroup:
    """Authored comments that report the same underlying issue; one partition only."""

    group_id: str
    members: tuple[tuple[str, str], ...]
    note: str


@dataclass(frozen=True)
class EvaluationManifest:
    sha256: str
    dataset_id: str
    description: str
    objective: ObjectiveType
    domain: str
    bindings: IdentityBindings
    supported_languages: tuple[str, ...]
    sampling: SamplingLimits
    labels: FileBinding
    threads: tuple[ManifestThread, ...]
    issue_groups: tuple[IssueGroup, ...]


@dataclass(frozen=True)
class LabelOrigin:
    kind: LabelOriginKind
    authored_by: str
    method: str
    disclosure: str

    @property
    def independent(self) -> bool:
        return self.kind is LabelOriginKind.INDEPENDENT_ADJUDICATION

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "authored_by": self.authored_by,
            "method": self.method,
            "disclosure": self.disclosure,
            "independent_adjudication": self.independent,
        }


ExpectedValue = str | int | bool


@dataclass(frozen=True)
class Label:
    """Expected outcome: a choice label, an integer score level or a noul boolean."""

    thread_id: str
    comment_id: str
    question_id: str
    expected: ExpectedValue
    case: CaseCategory
    rationale: str


@dataclass(frozen=True)
class LabelSet:
    sha256: str
    origin: LabelOrigin
    labels: tuple[Label, ...]


@dataclass(frozen=True)
class CorpusComment:
    thread_id: str
    comment_id: str
    index: int
    text_sha256: str
    # sha256 of the casefolded, whitespace-collapsed body: detects copies across threads.
    normalized_text_sha256: str
    language: str
    chars: int


@dataclass(frozen=True)
class CorpusThread:
    entry: ManifestThread
    thread: Thread
    snapshot_sha256: str
    thread_text_sha256: str
    comments: tuple[CorpusComment, ...]


@dataclass(frozen=True)
class IntegrityCheck:
    name: str
    groups_checked: int
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": "passed",
            "groups_checked": self.groups_checked,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class PartitionIntegrity:
    """Only constructed when every check passed; a leak raises instead."""

    checks: tuple[IntegrityCheck, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"status": "passed", "checks": [check.to_dict() for check in self.checks]}


@dataclass(frozen=True)
class SavedAnswer:
    question_id: str
    question_type: QuestionType
    choice: str | None
    score: float | None
    noul: float | None
    probabilities: tuple[tuple[str, float], ...]
    answer_confidence: float
    # The exact serialized answer, compared verbatim during replay.
    raw: dict[str, Any]


@dataclass(frozen=True)
class SavedItem:
    index: int
    comment_id: str
    comment_text_sha256: str
    status: ItemStatus
    reasons: tuple[str, ...]
    batch_index: int | None
    answers: tuple[SavedAnswer, ...]
    # The exact serialized item, compared verbatim during replay.
    raw: dict[str, Any]

    def answer(self, question_id: str) -> SavedAnswer | None:
        for candidate in self.answers:
            if candidate.question_id == question_id:
                return candidate
        return None


@dataclass(frozen=True)
class SavedDecision:
    """A strictly validated saved decision document; `decision_sha256` is its file hash."""

    decision_sha256: str
    snapshot_sha256: str
    thread_id: str
    comment_count: int
    requirement: str
    runtime_manifest_sha256: str
    checkpoint_revision: str
    checkpoint_verified: bool
    question_set_sha256: str
    settings_sha256: str
    context_policy_sha256: str
    language: str
    route_model: str
    objective: str
    domain: str
    coverage: CoverageSummary
    items: tuple[SavedItem, ...]
    document: dict[str, Any]

    def bindings(self) -> IdentityBindings:
        return IdentityBindings(
            runtime_manifest_sha256=self.runtime_manifest_sha256,
            checkpoint_revision=self.checkpoint_revision,
            question_set_sha256=self.question_set_sha256,
            settings_sha256=self.settings_sha256,
            context_policy_sha256=self.context_policy_sha256,
            language=self.language,
        )


@dataclass(frozen=True)
class ReplayReport:
    """Offline re-derivation of saved items from the recorded exchange bodies."""

    decision_sha256: str
    replayed_items: int
    matched_items: int
    # (comment_id, reason) for items that never produced a recorded response.
    not_replayable: tuple[tuple[str, str], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_sha256": self.decision_sha256,
            "status": "reproduced",
            "replayed_items": self.replayed_items,
            "matched_items": self.matched_items,
            "not_replayable": [
                {"comment_id": comment_id, "reason": reason}
                for comment_id, reason in self.not_replayable
            ],
        }


@dataclass(frozen=True)
class HeuristicDecision:
    """The existing keyword heuristic's per-comment theme, mapped onto typed options.

    `label` is None when the heuristic made no decision (empty or noise-filtered text).
    `basis` names which existing heuristic rule produced the outcome.
    """

    comment_id: str
    label: str | None
    basis: str
    heuristic_theme: str | None
