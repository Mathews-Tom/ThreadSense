"""Activation policy checks over constructed, disclosed test-only evaluation evidence.

The evidence is an evaluation document built directly in the shape PR-1 publishes, with
known non-boundary held-out theme metrics (decision_rate 0.75, error_rate 0.25, labeled_decided
4). Its identity is computed from the real pinned runtime manifest and question set, so the
runtime identity check genuinely passes. Every invalidation changes exactly one field.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from threadsense.contracts.analysis_contract import ObjectiveType
from threadsense.decisions.artifact import decision_bytes
from threadsense.decisions.contracts import (
    PINNED_CONTEXT_POLICY,
    PINNED_INFERENCE_SETTINGS,
    decode_strict_json_object,
)
from threadsense.decisions.evaluation_contracts import IdentityBindings, LabelOriginKind
from threadsense.decisions.policy import (
    CHECK_ORDER,
    POLICY_CHECK_KIND,
    POLICY_KIND,
    Eligibility,
    PolicyCheckResult,
    PolicyScope,
    check_activation_policy,
    policy_check_bytes,
    publish_policy_check,
)
from threadsense.decisions.questions import build_question_set
from threadsense.decisions.runtime import load_pinned_runtime_manifest
from threadsense.domains import load_domain_vocabulary
from threadsense.errors import DecisionArtifactError, DecisionInputError

RUNTIME_MANIFEST = load_pinned_runtime_manifest()
QUESTION_SET = build_question_set(
    ObjectiveType.FRICTION_ANALYSIS, load_domain_vocabulary("developer_tools")
)
CURRENT_IDENTITY = IdentityBindings(
    runtime_manifest_sha256=RUNTIME_MANIFEST.sha256(),
    checkpoint_revision=RUNTIME_MANIFEST.checkpoint.revision,
    question_set_sha256=QUESTION_SET.sha256(),
    settings_sha256=PINNED_INFERENCE_SETTINGS.sha256(),
    context_policy_sha256=PINNED_CONTEXT_POLICY.sha256(),
    language=PINNED_INFERENCE_SETTINGS.lang,
)
DATASET_MANIFEST_SHA256 = hashlib.sha256(b"constructed test-only evaluation manifest").hexdigest()
LABELS_SHA256 = hashlib.sha256(b"constructed test-only evaluation labels").hexdigest()
OTHER_SHA256 = "0" * 64
FIXTURE_DISCLOSURE = (
    "Constructed test fixture for unit tests of the activation policy checker; its labels and "
    "tolerances are fixture values, not quality thresholds and not adjudicated ground truth."
)
HELD_OUT_THREAD = "fixture:held-out"
DEVELOPMENT_THREAD = "fixture:development"
CONFIDENCE_DEFINITION = "answer_confidence; uncalibrated; not a probability of correctness"
REFUSAL_REASON = "comment_exceeds_client_char_budget"
ALL_PASSED = ["passed"] * len(CHECK_ORDER)


def _labels(*, labeled_decided: int, correct: int) -> dict[str, Any]:
    if labeled_decided == 0:
        return {
            "status": "unavailable",
            "reason": "no_labels_for_question",
            "labeled": 0,
            "labeled_not_covered": 0,
            "labeled_excluded": 0,
            "labeled_abstained": 0,
            "labeled_decided": 0,
            "correct": 0,
            "incorrect": 0,
            "error_rate": None,
            "incorrect_items": [],
        }
    incorrect = labeled_decided - correct
    return {
        "status": "available",
        "reason": None,
        "labeled": labeled_decided,
        "labeled_not_covered": 0,
        "labeled_excluded": 0,
        "labeled_abstained": 0,
        "labeled_decided": labeled_decided,
        "correct": correct,
        "incorrect": incorrect,
        "error_rate": round(incorrect / labeled_decided, 6),
        "incorrect_items": [
            {
                "thread_id": HELD_OUT_THREAD,
                "comment_id": f"fixture:c{6 + index}",
                "expected": "workflow",
                "actual": "architecture",
            }
            for index in range(incorrect)
        ],
    }


def _question(
    question_type: str, *, population: int, eligible: int, abstained: int, labels: dict[str, Any]
) -> dict[str, Any]:
    decided = eligible - abstained
    return {
        "question_type": question_type,
        "population": population,
        "eligible": eligible,
        "abstained": abstained,
        "decided": decided,
        "decision_rate": None if population == 0 else round(decided / population, 6),
        "labels": labels,
        "confidence": {
            "definition": CONFIDENCE_DEFINITION,
            "mean_correct": None,
            "mean_incorrect": None,
            "mean_decided": None,
        },
    }


def _population(**counts: int) -> dict[str, int]:
    keys = ("threads", "comments", "ok", "refused", "unavailable", "excluded_language", "eligible")
    return {key: counts.get(key, 0) for key in keys}


def _held_out_partition() -> dict[str, Any]:
    """Eight comments: one refused, one excluded by language, six eligible."""
    unlabeled = _labels(labeled_decided=0, correct=0)
    return {
        "population": _population(
            threads=1, comments=8, ok=7, refused=1, excluded_language=1, eligible=6
        ),
        "questions": {
            "evidence_type": _question(
                "choice", population=8, eligible=6, abstained=1, labels=unlabeled
            ),
            "promotional_framing": _question(
                "noul", population=8, eligible=6, abstained=0, labels=unlabeled
            ),
            "theme": _question(
                "choice",
                population=8,
                eligible=6,
                abstained=0,
                labels=_labels(labeled_decided=4, correct=3),
            ),
        },
    }


def _development_partition() -> dict[str, Any]:
    unlabeled = _labels(labeled_decided=0, correct=0)
    return {
        "population": _population(threads=1, comments=2, ok=1, unavailable=1, eligible=1),
        "questions": {
            "theme": _question("choice", population=2, eligible=1, abstained=0, labels=unlabeled)
        },
    }


def _abstained_entry(comment_id: str, question_id: str) -> dict[str, Any]:
    return {
        "partition": "evaluation",
        "thread_id": HELD_OUT_THREAD,
        "comment_id": comment_id,
        "question_id": question_id,
        "reason": "chose other_or_uncertain",
    }


def _audit() -> dict[str, list[dict[str, Any]]]:
    return {
        "rejected": [
            {
                "partition": "evaluation",
                "thread_id": HELD_OUT_THREAD,
                "comment_id": "fixture:c1",
                "status": "refused",
                "reasons": [REFUSAL_REASON],
            },
            {
                "partition": "development",
                "thread_id": DEVELOPMENT_THREAD,
                "comment_id": "fixture:d2",
                "status": "unavailable",
                "reasons": ["invalid_typed_answer"],
            },
        ],
        "excluded": [
            {
                "partition": "evaluation",
                "thread_id": HELD_OUT_THREAD,
                "comment_id": "fixture:c2",
                "language": "es",
                "reason": "language_unsupported",
            }
        ],
        "abstained": [_abstained_entry("fixture:c3", "evidence_type")],
    }


def _annotation(thread_id: str, *, coverage: dict[str, Any], replayed: int) -> dict[str, Any]:
    decision_sha256 = hashlib.sha256(f"decision of {thread_id}".encode()).hexdigest()
    not_replayable = [{"comment_id": "fixture:c1", "reason": REFUSAL_REASON}] * coverage["refused"]
    return {
        "thread_id": thread_id,
        "snapshot_sha256": hashlib.sha256(f"snapshot of {thread_id}".encode()).hexdigest(),
        "decision_sha256": decision_sha256,
        "requirement": "optional",
        "coverage": coverage,
        "replay": {
            "decision_sha256": decision_sha256,
            "status": "reproduced",
            "replayed_items": replayed,
            "matched_items": replayed,
            "not_replayable": not_replayable,
        },
    }


def _dataset(origin_kind: LabelOriginKind) -> dict[str, Any]:
    independent = origin_kind is LabelOriginKind.INDEPENDENT_ADJUDICATION
    return {
        "manifest_sha256": DATASET_MANIFEST_SHA256,
        "dataset_id": "constructed-policy-check-fixture",
        "description": FIXTURE_DISCLOSURE,
        "objective": "friction_analysis",
        "domain": "developer_tools",
        "supported_languages": [CURRENT_IDENTITY.language],
        "sampling": {
            "method": "authored_engineering_cases",
            "selection": "every comment of every listed thread",
            "max_threads": 12,
            "max_comments_per_thread": 12,
        },
        "labels": {
            "sha256": LABELS_SHA256,
            "origin": {
                "kind": origin_kind.value,
                "authored_by": "ThreadSense unit tests",
                "method": "Declared directly in a constructed evaluation document.",
                "disclosure": FIXTURE_DISCLOSURE,
                "independent_adjudication": independent,
            },
            "count": 4,
            "by_case": {"baseline": 4},
        },
        "threads": [],
        "issue_groups": [],
    }


def evidence_document(
    origin_kind: LabelOriginKind = LabelOriginKind.CONSTRUCTED_TEST_FIXTURE,
) -> dict[str, Any]:
    """A PR-1-shaped evaluation document with known held-out metrics."""
    held_out_coverage = {"status": "partial", "total": 8, "ok": 7, "refused": 1, "unavailable": 0}
    development_coverage = {
        "status": "partial",
        "total": 2,
        "ok": 1,
        "refused": 0,
        "unavailable": 1,
    }
    return {
        "artifact_kind": "typed_decision_evaluation",
        "schema_version": 1,
        "mode": "shadow",
        "calibration": "uncalibrated",
        "policy": "none",
        "claims": {
            "result_scope": "engineering_assessment",
            "population_accuracy": "not_claimed",
            "label_origin": origin_kind.value,
            "independent_adjudication": origin_kind is LabelOriginKind.INDEPENDENT_ADJUDICATION,
            "calibration_fitted": False,
            "note": FIXTURE_DISCLOSURE,
        },
        "dataset": _dataset(origin_kind),
        "identity": {**CURRENT_IDENTITY.to_dict(), "language_basis": "declared"},
        "partition_integrity": {
            "status": "passed",
            "checks": [
                {
                    "name": "thread_partition",
                    "status": "passed",
                    "groups_checked": 2,
                    "detail": "every thread id is assigned to exactly one partition",
                }
            ],
        },
        "annotations": [
            _annotation(DEVELOPMENT_THREAD, coverage=development_coverage, replayed=2),
            _annotation(HELD_OUT_THREAD, coverage=held_out_coverage, replayed=7),
        ],
        "partitions": {
            "development": _development_partition(),
            "calibration": {"population": _population(), "questions": {}},
            "evaluation": _held_out_partition(),
        },
        "audit": _audit(),
    }


def unsupported_coverage_document() -> dict[str, Any]:
    """Every held-out comment is rejected, so no held-out item is eligible for any question."""
    document = evidence_document()
    held_out = document["partitions"]["evaluation"]
    held_out["population"] = _population(threads=1, comments=8, refused=1, unavailable=7)
    not_covered = _labels(labeled_decided=0, correct=0)
    not_covered.update(status="available", reason=None, labeled=4, labeled_not_covered=4)
    held_out["questions"] = {
        "theme": _question("choice", population=8, eligible=0, abstained=0, labels=not_covered)
    }
    document["audit"] = {
        "rejected": [
            {
                "partition": "evaluation",
                "thread_id": HELD_OUT_THREAD,
                "comment_id": f"fixture:c{index}",
                "status": "refused" if index == 1 else "unavailable",
                "reasons": [REFUSAL_REASON if index == 1 else "runtime_unreachable"],
            }
            for index in range(1, 9)
        ],
        "excluded": [],
        "abstained": [],
    }
    return document


def policy_document(evidence: dict[str, Any], *, scope: str = "test_only") -> dict[str, Any]:
    """A policy bound to ``evidence`` with fixture tolerances that clearly hold."""
    identity = dict(evidence["identity"])
    del identity["language_basis"]
    return {
        "artifact_kind": POLICY_KIND,
        "schema_version": 1,
        "policy_id": "constructed-theme-fixture-policy",
        "scope": scope,
        "disclosure": FIXTURE_DISCLOSURE,
        "decision": {"question_id": "theme", "consumer": "semantic_reranking"},
        "bindings": {
            **identity,
            "dataset_manifest_sha256": evidence["dataset"]["manifest_sha256"],
            "labels_sha256": evidence["dataset"]["labels"]["sha256"],
        },
        "tolerances": {"max_error_rate": 0.5, "min_decision_rate": 0.5, "min_labeled_decided": 2},
    }


def _write_named(directory: Path, content: bytes) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{hashlib.sha256(content).hexdigest()}.json"
    path.write_bytes(content)
    return path


def write_evidence(directory: Path, document: dict[str, Any]) -> Path:
    return _write_named(directory, decision_bytes(document))


def write_nonfinite_evidence(directory: Path, document: dict[str, Any]) -> Path:
    """``decision_bytes`` refuses non-finite numbers, so these bytes carry NaN tokens."""
    content = (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    return _write_named(directory, content)


def write_policy_text(directory: Path, text: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "policy.json"
    path.write_text(text, encoding="utf-8")
    return path


def write_policy(directory: Path, policy: dict[str, Any]) -> Path:
    # json.dumps writes float("nan") and float("inf") as the literal NaN and Infinity tokens.
    return write_policy_text(directory, json.dumps(policy, indent=2) + "\n")


def run_check(
    tmp_path: Path, evidence: dict[str, Any], policy: dict[str, Any] | None
) -> PolicyCheckResult:
    evaluation_path = write_evidence(tmp_path / "evidence", evidence)
    policy_path = None if policy is None else write_policy(tmp_path / "policy", policy)
    return check_activation_policy(
        policy_path=policy_path, evaluation_path=evaluation_path, runtime_manifest=RUNTIME_MANIFEST
    )


def codes(result: PolicyCheckResult) -> list[str]:
    return [reason.code for reason in result.reasons]


def statuses(result: PolicyCheckResult) -> dict[str, str]:
    return {check["name"]: check["status"] for check in result.document["checks"]}


def _assign(document: dict[str, Any], keys: tuple[str | int, ...], value: Any) -> None:
    target: Any = document
    for key in keys[:-1]:
        target = target[key]
    target[keys[-1]] = value


def test_check_activation_policy_constructed_test_fixture_is_eligible_without_permission(
    tmp_path: Path,
) -> None:
    # Arrange
    evidence = evidence_document()
    policy = policy_document(evidence)

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert result.eligibility is Eligibility.ELIGIBLE
    assert result.scope is PolicyScope.TEST_ONLY
    assert result.reasons == ()
    assert result.activation_permitted is False
    assert result.document["activation"]["permitted"] is False
    assert result.document["activation"]["performed"] is False
    assert [check["name"] for check in result.document["checks"]] == list(CHECK_ORDER)
    assert list(statuses(result).values()) == ALL_PASSED


def test_check_activation_policy_constructed_test_fixture_records_held_out_metrics(
    tmp_path: Path,
) -> None:
    # Arrange
    evidence = evidence_document()

    # Act
    result = run_check(tmp_path, evidence, policy_document(evidence))

    # Assert
    document = result.document
    held_out = document["held_out"]
    assert document["artifact_kind"] == POLICY_CHECK_KIND
    assert document["mode"] == "shadow"
    assert document["current_identity"] == CURRENT_IDENTITY.to_dict()
    assert document["evidence"]["label_origin"] == "constructed_test_fixture"
    assert document["evidence"]["independent_adjudication"] is False
    assert (held_out["partition"], held_out["question_id"]) == ("evaluation", "theme")
    assert (held_out["decision_rate"], held_out["error_rate"]) == (0.75, 0.25)
    assert (held_out["eligible"], held_out["decided"], held_out["labeled_decided"]) == (6, 6, 4)
    assert [entry["comment_id"] for entry in held_out["audit"]["rejected"]] == ["fixture:c1"]
    assert [entry["comment_id"] for entry in held_out["audit"]["excluded"]] == ["fixture:c2"]
    assert held_out["audit"]["abstained"] == []


def test_check_activation_policy_runtime_scope_independent_labels_permits_activation(
    tmp_path: Path,
) -> None:
    # Arrange: a constructed document that declares independent adjudication, proving the
    # runtime guard can pass; it is not a claim about any real label set.
    evidence = evidence_document(LabelOriginKind.INDEPENDENT_ADJUDICATION)
    policy = policy_document(evidence, scope="runtime")

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert result.eligibility is Eligibility.ELIGIBLE
    assert result.scope is PolicyScope.RUNTIME
    assert result.activation_permitted is True
    assert result.document["activation"]["permitted"] is True
    assert result.document["activation"]["performed"] is False
    assert list(statuses(result).values()) == ALL_PASSED


SINGLE_FIELD_INVALIDATIONS = [
    pytest.param(
        "policy",
        ("bindings", "checkpoint_revision"),
        "f" * 40,
        ["stale_checkpoint_binding"],
        id="checkpoint_revision",
    ),
    pytest.param(
        "policy",
        ("bindings", "runtime_manifest_sha256"),
        OTHER_SHA256,
        ["stale_checkpoint_binding"],
        id="runtime_manifest_sha256",
    ),
    pytest.param(
        "policy",
        ("bindings", "question_set_sha256"),
        OTHER_SHA256,
        ["stale_question_binding"],
        id="question_set_sha256",
    ),
    pytest.param(
        "policy",
        ("bindings", "settings_sha256"),
        OTHER_SHA256,
        ["stale_settings_binding"],
        id="settings_sha256",
    ),
    pytest.param(
        "policy",
        ("bindings", "context_policy_sha256"),
        OTHER_SHA256,
        ["stale_context_policy_binding"],
        id="context_policy_sha256",
    ),
    pytest.param(
        "policy", ("bindings", "language"), "es", ["stale_language_binding"], id="language"
    ),
    pytest.param(
        "policy",
        ("bindings", "dataset_manifest_sha256"),
        OTHER_SHA256,
        ["stale_dataset_binding"],
        id="dataset_manifest_sha256",
    ),
    pytest.param(
        "policy",
        ("bindings", "labels_sha256"),
        OTHER_SHA256,
        ["stale_dataset_binding"],
        id="labels_sha256",
    ),
    pytest.param(
        "policy",
        ("bindings", "checkpoint_revision"),
        "",
        ["unbound_binding"],
        id="binding_empty",
    ),
    pytest.param(
        "policy", ("bindings", "labels_sha256"), None, ["unbound_binding"], id="binding_null"
    ),
    pytest.param(
        "policy", ("scope",), "runtime", ["held_out_labels_not_independent"], id="scope_runtime"
    ),
    pytest.param(
        "policy",
        ("tolerances", "max_error_rate"),
        0.1,
        ["error_above_tolerance"],
        id="max_error_rate_below_observed",
    ),
    pytest.param(
        "policy",
        ("tolerances", "min_decision_rate"),
        0.9,
        ["coverage_below_tolerance"],
        id="min_decision_rate_above_observed",
    ),
    pytest.param(
        "policy",
        ("tolerances", "min_labeled_decided"),
        5,
        ["insufficient_labeled_items"],
        id="min_labeled_decided_above_observed",
    ),
    pytest.param(
        "policy",
        ("tolerances", "min_decision_rate"),
        1.5,
        ["tolerance_invalid"],
        id="min_decision_rate_outside_unit_interval",
    ),
    pytest.param(
        "policy",
        ("tolerances", "min_labeled_decided"),
        0,
        ["tolerance_invalid"],
        id="min_labeled_decided_zero",
    ),
    pytest.param(
        "policy",
        ("tolerances", "min_labeled_decided"),
        True,
        ["tolerance_invalid"],
        id="min_labeled_decided_boolean",
    ),
    pytest.param(
        "policy",
        ("decision", "question_id"),
        "promotional_framing",
        ["held_out_labels_missing"],
        id="question_without_held_out_labels",
    ),
    pytest.param(
        "policy",
        ("decision", "question_id"),
        "sarcasm",
        ["decision_question_unbound"],
        id="question_absent",
    ),
    pytest.param(
        "evidence",
        ("dataset", "labels", "origin", "kind"),
        "engineering_authored",
        ["test_fixture_origin_mismatch"],
        id="engineering_labels_with_test_only_policy",
    ),
    pytest.param(
        "evidence",
        ("partition_integrity", "status"),
        "failed",
        ["partition_integrity_failed"],
        id="partition_integrity_failed",
    ),
    pytest.param(
        "evidence",
        ("annotations", 1, "replay", "matched_items"),
        6,
        ["replay_not_reproduced"],
        id="replay_matched_fewer_items",
    ),
    pytest.param(
        "evidence",
        ("annotations", 1, "replay", "status"),
        "mismatched",
        ["replay_not_reproduced"],
        id="replay_status_not_reproduced",
    ),
    pytest.param(
        "evidence",
        ("audit", "excluded"),
        [],
        ["evidence_counts_unreconciled"],
        id="excluded_audit_short_by_one",
    ),
    pytest.param(
        "evidence",
        ("audit", "abstained"),
        [
            _abstained_entry("fixture:c3", "evidence_type"),
            _abstained_entry("fixture:c4", "theme"),
        ],
        ["evidence_counts_unreconciled"],
        id="abstained_audit_long_by_one",
    ),
]


@pytest.mark.parametrize(("target", "keys", "value", "expected"), SINGLE_FIELD_INVALIDATIONS)
def test_check_activation_policy_one_field_invalidation_reports_exactly_that_reason(
    tmp_path: Path, target: str, keys: tuple[str | int, ...], value: Any, expected: list[str]
) -> None:
    # Arrange
    evidence = evidence_document()
    policy = policy_document(evidence)
    _assign(policy if target == "policy" else evidence, keys, value)

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert result.eligibility is Eligibility.NOT_ELIGIBLE
    assert codes(result) == expected
    assert result.activation_permitted is False


HELD_OUT_THEME = ("partitions", "evaluation", "questions", "theme")
THEME_FIELD = "partitions.evaluation.questions.theme"

UNRECONCILED_INVALIDATIONS = [
    pytest.param(
        (*HELD_OUT_THEME, "labels", "error_rate"),
        0.0,
        f"{THEME_FIELD}.labels.error_rate",
        id="error_rate_not_recomputed_from_counts",
    ),
    pytest.param(
        (*HELD_OUT_THEME, "decision_rate"),
        0.7,
        f"{THEME_FIELD}.decision_rate",
        id="decision_rate_not_recomputed_from_counts",
    ),
    pytest.param(
        (*HELD_OUT_THEME, "labels", "correct"),
        2,
        f"{THEME_FIELD}.labels.labeled_decided",
        id="labeled_decided_not_correct_plus_incorrect",
    ),
    pytest.param(
        (*HELD_OUT_THEME, "labels", "incorrect_items"),
        [],
        f"{THEME_FIELD}.labels.incorrect_items",
        id="incorrect_items_short_of_incorrect",
    ),
    pytest.param(("audit", "rejected", 0, "reasons"), [], "audit.rejected", id="empty_reasons"),
    pytest.param(
        ("audit", "rejected", 0, "reasons"), [7], "audit.rejected", id="non_string_reason"
    ),
    pytest.param(("audit", "excluded", 0, "thread_id"), "", "audit.excluded", id="empty_thread"),
    pytest.param(
        ("audit", "excluded", 0, "comment_id"),
        "fixture:c1",
        "audit.excluded",
        id="comment_both_rejected_and_excluded",
    ),
    pytest.param(
        ("audit", "abstained"),
        [
            _abstained_entry("fixture:c3", "evidence_type"),
            _abstained_entry("fixture:c3", "evidence_type"),
        ],
        "audit.abstained",
        id="duplicate_abstained_entry",
    ),
]


@pytest.mark.parametrize(("keys", "value", "field"), UNRECONCILED_INVALIDATIONS)
def test_check_activation_policy_inconsistent_evidence_reports_only_unreconciled(
    tmp_path: Path, keys: tuple[str | int, ...], value: Any, field: str
) -> None:
    # Arrange: one edited field; every count the tolerances gate on is otherwise unchanged.
    evidence = evidence_document()
    policy = policy_document(evidence)
    _assign(evidence, keys, value)

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert result.eligibility is Eligibility.NOT_ELIGIBLE
    assert codes(result) == ["evidence_counts_unreconciled"]
    assert result.reasons[0].field == field
    assert statuses(result)["tolerances"] == "passed"
    assert statuses(result)["reconciliation"] == "failed"


@pytest.mark.parametrize(
    ("keys", "rate_field"),
    [
        pytest.param(
            (*HELD_OUT_THEME, "decided"),
            f"{THEME_FIELD}.decision_rate",
            id="decided_overflows_recomputed_rate",
        ),
        pytest.param(
            (*HELD_OUT_THEME, "labels", "incorrect"),
            f"{THEME_FIELD}.labels.error_rate",
            id="incorrect_overflows_recomputed_rate",
        ),
    ],
)
def test_check_activation_policy_overflowing_count_reports_unreconciled(
    tmp_path: Path, keys: tuple[str | int, ...], rate_field: str
) -> None:
    # Arrange: the same checker first accepts this constructed fixture, then one count changes.
    evidence = evidence_document()
    policy = policy_document(evidence)
    eligible = run_check(tmp_path / "baseline", evidence, policy)
    assert eligible.eligibility is Eligibility.ELIGIBLE
    assert eligible.reasons == ()
    _assign(evidence, keys, 10**400)

    # Act
    result = run_check(tmp_path / "invalidated", evidence, policy)

    # Assert: the count and rate failures remain machine-readable without activation.
    assert result.eligibility is Eligibility.NOT_ELIGIBLE
    assert codes(result)
    assert set(codes(result)) == {"evidence_counts_unreconciled"}
    assert rate_field in {reason.field for reason in result.reasons}
    assert statuses(result)["reconciliation"] == "failed"
    assert statuses(result)["finite_values"] == "passed"
    assert statuses(result)["tolerances"] == "passed"
    assert result.activation_permitted is False
    assert result.document["activation"]["permitted"] is False
    assert result.document["activation"]["performed"] is False
    assert [check["name"] for check in result.document["checks"]] == list(CHECK_ORDER)
    assert (
        decode_strict_json_object(
            policy_check_bytes(result.document), DecisionArtifactError, "policy check"
        )
        == result.document
    )


def test_check_activation_policy_duplicate_rejected_entry_reports_unreconciled(
    tmp_path: Path,
) -> None:
    # Arrange: eight rejected held-out entries, two of them for the same comment.
    evidence = unsupported_coverage_document()
    evidence["audit"]["rejected"][1]["comment_id"] = "fixture:c1"
    policy = policy_document(evidence)

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert codes(result) == [
        "coverage_unsupported",
        "held_out_labels_missing",
        "evidence_counts_unreconciled",
    ]
    assert result.reasons[-1].field == "audit.rejected"


def test_check_activation_policy_extra_supported_language_reports_stale_language_binding(
    tmp_path: Path,
) -> None:
    # Arrange: the held-out population was stratified with a language the runtime does not bind.
    evidence = evidence_document()
    evidence["dataset"]["supported_languages"] = [CURRENT_IDENTITY.language, "es"]
    policy = policy_document(evidence)

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert codes(result) == ["stale_language_binding"]
    assert result.reasons[0].check == "runtime_identity"
    assert result.reasons[0].field == "dataset.supported_languages"


@pytest.mark.parametrize(
    ("question_id", "expected"),
    [
        ("theme", ["coverage_unsupported", "held_out_labels_missing", "tolerance_invalid"]),
        ("sarcasm", ["decision_question_unbound", "tolerance_invalid"]),
    ],
)
def test_check_activation_policy_invalid_tolerance_is_reported_without_usable_metrics(
    tmp_path: Path, question_id: str, expected: list[str]
) -> None:
    # Arrange
    evidence = unsupported_coverage_document()
    policy = policy_document(evidence)
    policy["decision"]["question_id"] = question_id
    policy["tolerances"]["min_decision_rate"] = 1.5

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert codes(result) == expected
    assert result.reasons[-1].field == "tolerances.min_decision_rate"
    assert statuses(result)["tolerances"] == "failed"


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity", "1e999"])
def test_check_activation_policy_nonfinite_tolerance_token_reports_nonfinite_value(
    tmp_path: Path, token: str
) -> None:
    # Arrange
    evidence = evidence_document()
    text = json.dumps(policy_document(evidence), indent=2)
    evaluation_path = write_evidence(tmp_path / "evidence", evidence)
    policy_path = write_policy_text(
        tmp_path / "policy", text.replace('"max_error_rate": 0.5', f'"max_error_rate": {token}')
    )

    # Act
    result = check_activation_policy(
        policy_path=policy_path, evaluation_path=evaluation_path, runtime_manifest=RUNTIME_MANIFEST
    )

    # Assert
    assert codes(result) == ["nonfinite_value"]
    assert result.reasons[0].field == "tolerances.max_error_rate"
    assert statuses(result)["tolerances"] == "not_run"


@pytest.mark.parametrize(
    ("keys", "value", "field"),
    [
        (("labels", "error_rate"), float("nan"), "held_out.error_rate"),
        (("decision_rate",), float("inf"), "held_out.decision_rate"),
    ],
)
def test_check_activation_policy_nonfinite_observed_rate_reports_nonfinite_value(
    tmp_path: Path, keys: tuple[str, ...], value: float, field: str
) -> None:
    # Arrange
    evidence = evidence_document()
    policy = policy_document(evidence)
    _assign(evidence["partitions"]["evaluation"]["questions"]["theme"], keys, value)
    evaluation_path = write_nonfinite_evidence(tmp_path / "evidence", evidence)
    policy_path = write_policy(tmp_path / "policy", policy)

    # Act
    result = check_activation_policy(
        policy_path=policy_path, evaluation_path=evaluation_path, runtime_manifest=RUNTIME_MANIFEST
    )

    # Assert
    assert result.eligibility is Eligibility.NOT_ELIGIBLE
    assert codes(result) == ["nonfinite_value"]
    assert result.reasons[0].field == field


def test_check_activation_policy_evidence_stale_against_runtime_reports_runtime_identity(
    tmp_path: Path,
) -> None:
    # Arrange: the policy is bound to the evidence, which is stale against the runtime.
    evidence = evidence_document()
    evidence["identity"]["checkpoint_revision"] = "f" * 40
    policy = policy_document(evidence)

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert codes(result) == ["stale_checkpoint_binding"]
    assert result.reasons[0].check == "runtime_identity"
    assert statuses(result)["policy_bindings"] == "passed"


def test_check_activation_policy_unsupported_coverage_reports_coverage_and_missing_labels(
    tmp_path: Path,
) -> None:
    # Arrange: with no eligible held-out item no label can be decided either.
    evidence = unsupported_coverage_document()
    policy = policy_document(evidence)

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert codes(result) == ["coverage_unsupported", "held_out_labels_missing"]
    assert statuses(result)["tolerances"] == "not_run"
    assert statuses(result)["reconciliation"] == "passed"


def _duplicate_scope(text: str) -> str:
    return text.replace('"scope": "test_only"', '"scope": "test_only",\n  "scope": "test_only"')


def _unknown_key(text: str) -> str:
    return text.replace('"scope": "test_only"', '"scope": "test_only",\n  "activate": true')


def _version_two(text: str) -> str:
    return text.replace('"schema_version": 1', '"schema_version": 2')


def _undisclosed_test_scope(text: str) -> str:
    return text.replace(json.dumps(FIXTURE_DISCLOSURE), json.dumps("Operator fixture."))


def _contest_disclosure(text: str) -> str:
    disclosure = "Constructed fixture for contest thresholds."
    return text.replace(json.dumps(FIXTURE_DISCLOSURE), json.dumps(disclosure))


def _unconstructed_test_disclosure(text: str) -> str:
    disclosure = "Fixture for tests of the latest thresholds."
    return text.replace(json.dumps(FIXTURE_DISCLOSURE), json.dumps(disclosure))


def _unknown_consumer(text: str) -> str:
    return text.replace('"semantic_reranking"', '"semantic_ranking"')


def _missing_tolerances(text: str) -> str:
    document = json.loads(text)
    del document["tolerances"]
    return json.dumps(document, indent=2)


@pytest.mark.parametrize(
    ("edit", "expected"),
    [
        (_duplicate_scope, "policy_invalid"),
        (_unknown_key, "policy_invalid"),
        (_undisclosed_test_scope, "policy_invalid"),
        (_contest_disclosure, "policy_invalid"),
        (_unconstructed_test_disclosure, "policy_invalid"),
        (_unknown_consumer, "policy_invalid"),
        (_missing_tolerances, "policy_invalid"),
        (_version_two, "policy_version_unsupported"),
    ],
)
def test_check_activation_policy_malformed_policy_reports_policy_reason(
    tmp_path: Path, edit: Callable[[str], str], expected: str
) -> None:
    # Arrange
    evidence = evidence_document()
    text = json.dumps(policy_document(evidence), indent=2)
    evaluation_path = write_evidence(tmp_path / "evidence", evidence)
    policy_path = write_policy_text(tmp_path / "policy", edit(text))

    # Act
    result = check_activation_policy(
        policy_path=policy_path, evaluation_path=evaluation_path, runtime_manifest=RUNTIME_MANIFEST
    )

    # Assert
    assert codes(result) == [expected]
    assert result.scope is None
    assert result.document["policy"] is None
    assert statuses(result)["label_provenance"] == "not_run"
    assert statuses(result)["policy_bindings"] == "not_run"


@pytest.mark.parametrize(
    ("keys", "value"),
    [
        (("artifact_kind",), "typed_decision_annotations"),
        (("schema_version",), 2),
        (("partition_integrity",), "passed"),
        (("dataset", "labels", "origin", "independent_adjudication"), True),
        (("dataset", "supported_languages"), "en"),
        (("dataset", "supported_languages"), [""]),
        ((*HELD_OUT_THEME, "labels", "incorrect_items"), 1),
        ((*HELD_OUT_THEME, "decision_rate"), 10**400),
    ],
)
def test_check_activation_policy_malformed_evidence_reports_evidence_invalid(
    tmp_path: Path, keys: tuple[str, ...], value: Any
) -> None:
    # Arrange
    evidence = evidence_document()
    policy = policy_document(evidence)
    _assign(evidence, keys, value)

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert codes(result) == ["evidence_invalid"]
    assert result.document["evidence"] is None


def test_check_activation_policy_absent_supported_languages_reports_evidence_invalid(
    tmp_path: Path,
) -> None:
    # Arrange
    evidence = evidence_document()
    policy = policy_document(evidence)
    del evidence["dataset"]["supported_languages"]

    # Act
    result = run_check(tmp_path, evidence, policy)

    # Assert
    assert codes(result) == ["evidence_invalid"]
    assert result.reasons[0].field == "dataset.supported_languages"


def test_check_activation_policy_renamed_evidence_reports_digest_mismatch(tmp_path: Path) -> None:
    # Arrange
    evidence = evidence_document()
    original = write_evidence(tmp_path / "evidence", evidence)
    renamed = original.rename(original.with_name(f"{OTHER_SHA256}.json"))
    policy_path = write_policy(tmp_path / "policy", policy_document(evidence))

    # Act
    result = check_activation_policy(
        policy_path=policy_path, evaluation_path=renamed, runtime_manifest=RUNTIME_MANIFEST
    )

    # Assert
    assert codes(result) == ["evidence_digest_mismatch"]


def test_check_activation_policy_missing_evidence_leaves_dependent_checks_not_run(
    tmp_path: Path,
) -> None:
    # Arrange
    policy_path = write_policy(tmp_path / "policy", policy_document(evidence_document()))
    missing = tmp_path / "evidence" / f"{OTHER_SHA256}.json"

    # Act
    result = check_activation_policy(
        policy_path=policy_path, evaluation_path=missing, runtime_manifest=RUNTIME_MANIFEST
    )

    # Assert
    assert result.eligibility is Eligibility.NOT_ELIGIBLE
    assert codes(result) == ["evidence_missing"]
    assert statuses(result) == {
        name: "passed" if name == "policy" else "failed" if name == "evidence" else "not_run"
        for name in CHECK_ORDER
    }
    assert result.document["current_identity"] is None
    assert result.document["held_out"] is None


def test_check_activation_policy_no_policy_with_engineering_labels_is_not_eligible(
    tmp_path: Path,
) -> None:
    # Arrange
    evidence = evidence_document(LabelOriginKind.ENGINEERING_AUTHORED)

    # Act
    result = run_check(tmp_path, evidence, None)

    # Assert
    assert result.eligibility is Eligibility.NOT_ELIGIBLE
    assert result.scope is None
    assert result.activation_permitted is False
    assert codes(result) == ["policy_not_declared", "held_out_labels_not_independent"]
    assert statuses(result) == {
        "policy": "failed",
        "evidence": "passed",
        "runtime_identity": "passed",
        "policy_bindings": "not_run",
        "label_provenance": "failed",
        "evidence_integrity": "passed",
        "held_out_coverage": "not_run",
        "finite_values": "not_run",
        "tolerances": "not_run",
        "reconciliation": "not_run",
    }


def test_check_activation_policy_repeated_check_reproduces_identical_document(
    tmp_path: Path,
) -> None:
    # Arrange
    evidence = evidence_document()
    first = run_check(tmp_path / "first", evidence, policy_document(evidence))

    # Act
    second = run_check(tmp_path / "second", evidence, policy_document(evidence))

    # Assert
    assert policy_check_bytes(first.document) == policy_check_bytes(second.document)


def _eligible_result(tmp_path: Path) -> PolicyCheckResult:
    evidence = evidence_document()
    return run_check(tmp_path, evidence, policy_document(evidence))


def test_publish_policy_check_repeated_publication_reuses_content_addressed_path(
    tmp_path: Path,
) -> None:
    # Arrange
    result = _eligible_result(tmp_path)
    output_root = tmp_path / "out"
    protected = (tmp_path / "protected",)

    # Act
    first = publish_policy_check(
        result, output_root=output_root, protected_roots=protected, input_paths=()
    )
    second = publish_policy_check(
        result, output_root=output_root, protected_roots=protected, input_paths=()
    )

    # Assert
    content = first.policy_check_path.read_bytes()
    assert first == second
    assert content == policy_check_bytes(result.document)
    assert first.policy_check_sha256 == hashlib.sha256(content).hexdigest()
    assert first.policy_check_path == (
        output_root.resolve() / "policy-checks" / f"{first.policy_check_sha256}.json"
    )
    assert first.to_dict() == {
        "policy_check_path": str(first.policy_check_path),
        "policy_check_sha256": first.policy_check_sha256,
        "eligibility": "eligible",
        "scope": "test_only",
        "activation_permitted": False,
        "reasons": [],
    }


def test_publish_policy_check_nonfinite_tolerance_is_recorded_as_string(tmp_path: Path) -> None:
    # Arrange
    evidence = evidence_document()
    policy = policy_document(evidence)
    policy["tolerances"]["max_error_rate"] = float("nan")
    result = run_check(tmp_path, evidence, policy)

    # Act
    outcome = publish_policy_check(
        result, output_root=tmp_path / "out", protected_roots=(), input_paths=()
    )

    # Assert
    content = outcome.policy_check_path.read_bytes()
    published = decode_strict_json_object(content, DecisionArtifactError, "policy check")
    assert published["policy"]["tolerances"]["max_error_rate"] == "NaN"
    assert outcome.reason_codes == ("nonfinite_value",)


def test_publish_policy_check_output_aliasing_an_input_is_refused(tmp_path: Path) -> None:
    # Arrange
    result = _eligible_result(tmp_path)
    output_root = tmp_path / "out"
    digest = hashlib.sha256(policy_check_bytes(result.document)).hexdigest()
    aliased_input = output_root / "policy-checks" / f"{digest}.json"

    # Act
    with pytest.raises(DecisionInputError) as raised:
        publish_policy_check(
            result, output_root=output_root, protected_roots=(), input_paths=(aliased_input,)
        )

    # Assert
    assert raised.value.details["reason"] == "output_aliases_input"
    assert not aliased_input.exists()


def test_publish_policy_check_symlinked_check_directory_is_refused(tmp_path: Path) -> None:
    # Arrange
    result = _eligible_result(tmp_path)
    output_root = tmp_path / "out"
    elsewhere = tmp_path / "elsewhere"
    output_root.mkdir()
    elsewhere.mkdir()
    (output_root / "policy-checks").symlink_to(elsewhere, target_is_directory=True)

    # Act
    with pytest.raises(DecisionArtifactError) as raised:
        publish_policy_check(result, output_root=output_root, protected_roots=(), input_paths=())

    # Assert
    assert raised.value.details["reason"] == "artifact_directory_symlink"
    assert list(elsewhere.iterdir()) == []


def test_publish_policy_check_output_inside_protected_root_is_refused(tmp_path: Path) -> None:
    # Arrange
    result = _eligible_result(tmp_path)
    protected = tmp_path / "protected"

    # Act
    with pytest.raises(DecisionInputError) as raised:
        publish_policy_check(
            result,
            output_root=protected / "out",
            protected_roots=(protected,),
            input_paths=(),
        )

    # Assert
    assert raised.value.details["reason"] == "output_inside_protected_storage"
    assert not protected.exists()
