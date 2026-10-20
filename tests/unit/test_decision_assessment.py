"""Evaluation document and comparators over in-memory frozen inputs.

The scenario is engineering-authored: three threads across two partitions with ok, refused,
unavailable and unsupported-language comments, labels chosen so every metric has a
hand-computed value. Only the HTTP transport of the availability probe is replaced.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from typing import Any

import httpx
import pytest

from threadsense.config import RuntimeConfig
from threadsense.contracts.analysis_contract import ObjectiveType
from threadsense.decisions import assessment
from threadsense.decisions.assessment import build_evaluation_document, evaluation_bytes
from threadsense.decisions.comparators import (
    GENERATIVE_COMPARATOR_NAME,
    generative_shape_support,
    heuristic_theme_decisions,
    observe_generative_availability,
)
from threadsense.decisions.contracts import (
    OTHER_OR_UNCERTAIN,
    ChoiceQuestion,
    CoverageSummary,
    ItemStatus,
    QuestionSet,
    QuestionType,
)
from threadsense.decisions.evaluation_contracts import (
    CaseCategory,
    CorpusComment,
    CorpusThread,
    EvaluationManifest,
    ExpectedValue,
    FileBinding,
    HeuristicDecision,
    IdentityBindings,
    IntegrityCheck,
    IssueGroup,
    Label,
    LabelOrigin,
    LabelOriginKind,
    LabelSet,
    ManifestThread,
    Partition,
    PartitionIntegrity,
    ReplayReport,
    SamplingLimits,
    SavedAnswer,
    SavedDecision,
    SavedItem,
)
from threadsense.decisions.questions import build_question_set
from threadsense.domains import load_domain_vocabulary
from threadsense.errors import DecisionEvaluationError
from threadsense.models.canonical import canonical_thread_from_payload
from threadsense.monitoring.observations import comment_text_sha256, thread_text_sha256

VOCABULARY = load_domain_vocabulary("developer_tools")
QUESTION_SET: QuestionSet = build_question_set(ObjectiveType.FRICTION_ANALYSIS, VOCABULARY)
BINDINGS = IdentityBindings(
    runtime_manifest_sha256="a772fbf43bb9bb610f065c584b5f17f27e3de892cee289a7d7ed9a840e6f885c",
    checkpoint_revision="7b928d828b7b0e022f929d9bd2e44165aa270148",
    question_set_sha256=QUESTION_SET.sha256(),
    settings_sha256="d8d0af9b32f2734236690925f3fa139cd050fc0b450f6f6e1fe93c0a36ea75be",
    context_policy_sha256="f06c4e021aba1e660349d35f438ac2add32dd30f33fff2406e24fcea294e5fa0",
    language="en",
)
LABELS_SHA256 = hashlib.sha256(b"engineering labels").hexdigest()
REFUSAL_REASON = "server_reported_truncation"
UNAVAILABLE_REASON = "invalid_typed_answer"
LOOPBACK = "http://127.0.0.1:11434"
CONFIGURED_MODEL = "gemma4:latest"

THREAD_A = "thread-a"
THREAD_B = "thread-b"
THREAD_C = "thread-c"
BODIES: dict[str, tuple[tuple[str, str], ...]] = {
    THREAD_A: (
        ("a1", "The docs guide for setup is missing the quickstart steps entirely."),
        ("a2", "This issue keeps happening every single morning after we upgrade things."),
        (
            "a3",
            "Honestly I have mixed feelings about the overall direction of this project lately.",
        ),
        ("a4", "thanks"),
    ),
    THREAD_B: (
        ("b1", "same"),
        ("b2", "La documentación está incompleta y la guía no explica nada útil."),
        ("b3", "The export dashboard crashes whenever I open the batch view."),
        ("b4", "!!!"),
    ),
    THREAD_C: (
        ("c1", "Could you please consider allowing more than one profile per account soon?"),
    ),
}


def _choice_question(question_id: str) -> ChoiceQuestion:
    question = QUESTION_SET.by_id()[question_id]
    assert isinstance(question, ChoiceQuestion)
    return question


THEME_QUESTION = _choice_question("theme")


def choice_answer(question_id: str, choice: str, confidence: float = 0.8) -> SavedAnswer:
    labels = _choice_question(question_id).labels
    rest = round((1 - confidence) / (len(labels) - 1), 6)
    probabilities = tuple((label, confidence if label == choice else rest) for label in labels)
    raw = {
        "question_id": question_id,
        "type": "choice",
        "choice": choice,
        "probabilities": [[label, p] for label, p in probabilities],
    }
    return SavedAnswer(
        question_id, QuestionType.CHOICE, choice, None, None, probabilities, confidence, raw
    )


def score_answer(question_id: str, levels: tuple[float, ...], score: float) -> SavedAnswer:
    probabilities = tuple((str(index), p) for index, p in enumerate(levels))
    raw = {
        "question_id": question_id,
        "type": "score",
        "score": score,
        "probabilities": [[label, p] for label, p in probabilities],
    }
    return SavedAnswer(
        question_id, QuestionType.SCORE, None, score, None, probabilities, max(levels), raw
    )


def noul_answer(question_id: str, noul: float) -> SavedAnswer:
    raw = {"question_id": question_id, "type": "noul", "noul": noul, "probabilities": []}
    confidence = max(noul, 1 - noul)
    return SavedAnswer(question_id, QuestionType.NOUL, None, None, noul, (), confidence, raw)


def answers(overrides: dict[str, SavedAnswer] | None = None) -> tuple[SavedAnswer, ...]:
    defaults = {
        "objective_relevance": score_answer("objective_relevance", (0.6, 0.3, 0.1), 0.5),
        "evidence_type": choice_answer("evidence_type", "firsthand_experience"),
        "request_intent": choice_answer("request_intent", "problem_report"),
        "actionability": score_answer("actionability", (0.6, 0.3, 0.1), 0.5),
        "promotional_framing": noul_answer("promotional_framing", 0.1),
        "theme": choice_answer("theme", "workflow"),
    }
    merged = {**defaults, **(overrides or {})}
    return tuple(merged[question_id] for question_id in QUESTION_SET.question_ids())


@dataclass(frozen=True)
class ItemPlan:
    status: ItemStatus
    reasons: tuple[str, ...] = ()
    overrides: dict[str, SavedAnswer] | None = None


PLANS: dict[str, ItemPlan] = {
    "a1": ItemPlan(
        ItemStatus.OK,
        overrides={
            "objective_relevance": score_answer("objective_relevance", (0.1, 0.2, 0.7), 1.6),
            "promotional_framing": noul_answer("promotional_framing", 0.5),
            "theme": choice_answer("theme", "documentation"),
        },
    ),
    "a2": ItemPlan(
        ItemStatus.OK,
        overrides={
            "objective_relevance": score_answer("objective_relevance", (0.3, 0.4, 0.3), 1.0),
            "promotional_framing": noul_answer("promotional_framing", 0.4999),
            "theme": choice_answer("theme", "performance", 0.55),
        },
    ),
    "a3": ItemPlan(
        ItemStatus.OK,
        overrides={
            "objective_relevance": score_answer("objective_relevance", (0.7, 0.2, 0.1), 0.4),
            "theme": choice_answer("theme", OTHER_OR_UNCERTAIN, 0.4),
        },
    ),
    "a4": ItemPlan(ItemStatus.REFUSED, (REFUSAL_REASON,)),
    # Expected ordinal 1.0 would round to level 1; the argmax with ties-lowest is level 0.
    "b1": ItemPlan(
        ItemStatus.OK,
        overrides={
            "objective_relevance": score_answer("objective_relevance", (0.45, 0.10, 0.45), 1.0)
        },
    ),
    "b2": ItemPlan(ItemStatus.OK),
    "b3": ItemPlan(ItemStatus.UNAVAILABLE, (UNAVAILABLE_REASON,)),
    "b4": ItemPlan(ItemStatus.OK),
    "c1": ItemPlan(ItemStatus.OK),
}

LABEL_ROWS: tuple[tuple[str, str, str, ExpectedValue, CaseCategory], ...] = (
    (THREAD_A, "a1", "theme", "documentation", CaseCategory.BASELINE),
    (THREAD_A, "a2", "theme", "reliability", CaseCategory.BASELINE),
    (THREAD_A, "a3", "theme", OTHER_OR_UNCERTAIN, CaseCategory.NEGATION),
    (THREAD_A, "a4", "theme", "documentation", CaseCategory.BASELINE),
    (THREAD_B, "b2", "theme", "documentation", CaseCategory.UNSUPPORTED_LANGUAGE),
    (THREAD_A, "a1", "objective_relevance", 2, CaseCategory.BASELINE),
    (THREAD_A, "a2", "objective_relevance", 1, CaseCategory.BASELINE),
    (THREAD_A, "a3", "objective_relevance", 1, CaseCategory.BASELINE),
    (THREAD_A, "a4", "objective_relevance", 1, CaseCategory.BASELINE),
    (THREAD_B, "b1", "objective_relevance", 0, CaseCategory.BASELINE),
    (THREAD_B, "b2", "objective_relevance", 2, CaseCategory.UNSUPPORTED_LANGUAGE),
    (THREAD_A, "a1", "promotional_framing", True, CaseCategory.MIXED_INTENT),
    (THREAD_A, "a2", "promotional_framing", True, CaseCategory.MIXED_INTENT),
)


def thread_payload(thread_id: str) -> dict[str, Any]:
    author = {"username": "author", "source_author_id": None}
    comments = [
        {
            "comment_id": comment_id,
            "parent_comment_id": None,
            "author": author,
            "body": body,
            "score": 1,
            "created_utc": 1700000000.0 + index,
            "depth": 0,
            "permalink": f"https://example.invalid/{thread_id}/{comment_id}",
        }
        for index, (comment_id, body) in enumerate(BODIES[thread_id])
    ]
    return {
        "artifact_kind": "canonical_thread",
        "schema_version": 2,
        "normalization_version": "canonical-thread-v2",
        "thread": {
            "thread_id": thread_id,
            "source": {
                "source_name": "github_discussions",
                "community": "tooling",
                "source_thread_id": thread_id,
                "thread_url": f"https://example.invalid/{thread_id}",
            },
            "title": f"Feedback thread {thread_id}",
            "body": None,
            "permalink": f"https://example.invalid/{thread_id}",
            "author": author,
            "comments": comments,
            "comment_count": len(comments),
            "provenance": {
                "raw_artifact_path": f"raw/{thread_id}.json",
                "raw_sha256": hashlib.sha256(thread_id.encode()).hexdigest(),
                "retrieved_at_utc": 1700000000.0,
                "normalized_at_utc": 1700000001.0,
                "schema_version": 2,
                "normalization_version": "canonical-thread-v2",
            },
        },
    }


def corpus_thread(entry: ManifestThread) -> CorpusThread:
    payload = thread_payload(entry.thread_id)
    raw = json.dumps(payload, sort_keys=True).encode("utf-8")
    thread = canonical_thread_from_payload(payload)
    comments = tuple(
        CorpusComment(
            thread_id=entry.thread_id,
            comment_id=comment.comment_id,
            index=index,
            text_sha256=comment_text_sha256(comment.body),
            normalized_text_sha256=hashlib.sha256(
                " ".join(comment.body.casefold().split()).encode("utf-8")
            ).hexdigest(),
            language=entry.language_of(comment.comment_id),
            chars=len(comment.body),
        )
        for index, comment in enumerate(thread.comments)
    )
    return CorpusThread(
        entry=entry,
        thread=thread,
        snapshot_sha256=hashlib.sha256(raw).hexdigest(),
        thread_text_sha256=thread_text_sha256(thread.title, thread.body),
        comments=comments,
    )


def saved_item(comment: CorpusComment) -> SavedItem:
    plan = PLANS[comment.comment_id]
    item_answers = answers(plan.overrides) if plan.status is ItemStatus.OK else ()
    raw = {
        "index": comment.index,
        "comment_id": comment.comment_id,
        "status": plan.status.value,
        "reasons": list(plan.reasons),
        "answers": [answer.raw for answer in item_answers],
    }
    return SavedItem(
        index=comment.index,
        comment_id=comment.comment_id,
        comment_text_sha256=comment.text_sha256,
        status=plan.status,
        reasons=plan.reasons,
        batch_index=0 if plan.status is not ItemStatus.REFUSED else None,
        answers=item_answers,
        raw=raw,
    )


def saved_decision(thread: CorpusThread) -> SavedDecision:
    items = tuple(saved_item(comment) for comment in thread.comments)
    return SavedDecision(
        decision_sha256=hashlib.sha256(f"decision {thread.entry.thread_id}".encode()).hexdigest(),
        snapshot_sha256=thread.snapshot_sha256,
        thread_id=thread.entry.thread_id,
        comment_count=len(items),
        requirement="optional",
        runtime_manifest_sha256=BINDINGS.runtime_manifest_sha256,
        checkpoint_revision=BINDINGS.checkpoint_revision,
        checkpoint_verified=True,
        question_set_sha256=BINDINGS.question_set_sha256,
        settings_sha256=BINDINGS.settings_sha256,
        context_policy_sha256=BINDINGS.context_policy_sha256,
        language="en",
        route_model="english",
        objective="friction_analysis",
        domain="developer_tools",
        coverage=CoverageSummary.from_statuses(item.status for item in items),
        items=items,
        document={},
    )


def replay_report(decision: SavedDecision) -> ReplayReport:
    sent = [item for item in decision.items if item.batch_index is not None]
    never_sent = tuple(
        (item.comment_id, item.reasons[0]) for item in decision.items if item.batch_index is None
    )
    return ReplayReport(decision.decision_sha256, len(sent), len(sent), never_sent)


@dataclass(frozen=True)
class Scenario:
    manifest: EvaluationManifest
    labels: LabelSet
    corpus: tuple[CorpusThread, ...]
    integrity: PartitionIntegrity
    decisions: tuple[SavedDecision, ...]
    replays: tuple[ReplayReport, ...]
    heuristic: dict[str, tuple[HeuristicDecision, ...]]

    def document(self, generative: Any = None) -> dict[str, Any]:
        return build_evaluation_document(
            manifest=self.manifest,
            labels=self.labels,
            corpus=self.corpus,
            integrity=self.integrity,
            decisions=self.decisions,
            replays=self.replays,
            question_set=QUESTION_SET,
            heuristic=self.heuristic,
            generative=generative,
        )


def manifest_entry(
    thread_id: str, partition: Partition, languages: tuple[tuple[str, str], ...]
) -> ManifestThread:
    digest = hashlib.sha256(thread_id.encode()).hexdigest()
    binding = FileBinding(f"threads/{thread_id}.json", digest)
    return ManifestThread(thread_id, binding, partition, "en", languages)


def manifest_entries() -> tuple[ManifestThread, ...]:
    return (
        manifest_entry(THREAD_A, Partition.DEVELOPMENT, ()),
        manifest_entry(THREAD_B, Partition.DEVELOPMENT, (("b2", "es"),)),
        manifest_entry(THREAD_C, Partition.EVALUATION, ()),
    )


def build_scenario() -> Scenario:
    entries = manifest_entries()
    manifest = EvaluationManifest(
        sha256=hashlib.sha256(b"engineering manifest").hexdigest(),
        dataset_id="threadsense-engineering-cases-v1",
        description="Engineering-authored boundary cases for typed decisions.",
        objective=ObjectiveType.FRICTION_ANALYSIS,
        domain="developer_tools",
        bindings=BINDINGS,
        supported_languages=("en",),
        sampling=SamplingLimits(
            "authored_engineering_cases", "every comment of every listed thread", 12, 12
        ),
        labels=FileBinding("labels.json", LABELS_SHA256),
        threads=entries,
        issue_groups=(
            IssueGroup("setup-docs", ((THREAD_A, "a1"), (THREAD_B, "b1")), "same setup gap"),
        ),
    )
    origin = LabelOrigin(
        LabelOriginKind.ENGINEERING_AUTHORED,
        "threadsense engineering",
        "authored boundary cases",
        "engineering-authored labels; not independent adjudication",
    )
    labels = LabelSet(
        LABELS_SHA256,
        origin,
        tuple(
            Label(thread_id, comment_id, question_id, expected, case, "authored rationale")
            for thread_id, comment_id, question_id, expected, case in LABEL_ROWS
        ),
    )
    corpus = tuple(corpus_thread(entry) for entry in entries)
    decisions = tuple(saved_decision(thread) for thread in corpus)
    return Scenario(
        manifest=manifest,
        labels=labels,
        corpus=corpus,
        integrity=PartitionIntegrity(
            (IntegrityCheck("thread_partition", 3, "every thread id appears once"),)
        ),
        decisions=decisions,
        replays=tuple(replay_report(decision) for decision in decisions),
        heuristic={
            thread.entry.thread_id: heuristic_theme_decisions(
                thread.thread, VOCABULARY, THEME_QUESTION
            )
            for thread in corpus
        },
    )


@pytest.fixture
def scenario() -> Scenario:
    return build_scenario()


@pytest.fixture
def document(scenario: Scenario) -> dict[str, Any]:
    return scenario.document()


def ref(thread_id: str, comment_id: str) -> dict[str, str]:
    return {"thread_id": thread_id, "comment_id": comment_id}


def test_build_evaluation_document_development_population_reconciles_statuses(
    document: dict[str, Any],
) -> None:
    assert list(document["partitions"]) == ["development", "calibration", "evaluation"]
    assert document["partitions"]["development"]["population"] == {
        "threads": 2,
        "comments": 8,
        "ok": 6,
        "refused": 1,
        "unavailable": 1,
        "excluded_language": 1,
        "eligible": 5,
    }
    assert document["partitions"]["evaluation"]["population"] == {
        "threads": 1,
        "comments": 1,
        "ok": 1,
        "refused": 0,
        "unavailable": 0,
        "excluded_language": 0,
        "eligible": 1,
    }


def test_build_evaluation_document_empty_partition_has_zero_population_and_null_rates(
    document: dict[str, Any],
) -> None:
    calibration = document["partitions"]["calibration"]
    theme = calibration["questions"]["theme"]

    assert set(calibration["population"].values()) == {0}
    assert theme["decision_rate"] is None
    assert theme["labels"]["error_rate"] is None
    assert theme["confidence"]["mean_decided"] is None


def test_build_evaluation_document_theme_labels_count_correct_incorrect_and_error_rate(
    document: dict[str, Any],
) -> None:
    theme = document["partitions"]["development"]["questions"]["theme"]

    assert (theme["eligible"], theme["abstained"], theme["decided"]) == (5, 1, 4)
    assert theme["decision_rate"] == 0.5
    assert theme["labels"] == {
        "status": "available",
        "reason": None,
        "labeled": 5,
        "labeled_not_covered": 1,
        "labeled_excluded": 1,
        "labeled_abstained": 1,
        "labeled_decided": 2,
        "correct": 1,
        "incorrect": 1,
        "error_rate": 0.5,
        "incorrect_items": [
            {**ref(THREAD_A, "a2"), "expected": "reliability", "actual": "performance"}
        ],
    }


def test_build_evaluation_document_other_or_uncertain_answer_is_audited_abstention(
    document: dict[str, Any],
) -> None:
    abstained = document["audit"]["abstained"]

    assert abstained == [
        {
            "partition": "development",
            **ref(THREAD_A, "a3"),
            "question_id": "theme",
            "reason": "chose other_or_uncertain",
        }
    ]
    assert document["partitions"]["development"]["questions"]["evidence_type"]["abstained"] == 0


def test_build_evaluation_document_audit_lists_rejected_and_excluded_items(
    document: dict[str, Any],
) -> None:
    assert document["audit"]["rejected"] == [
        {
            "partition": "development",
            **ref(THREAD_A, "a4"),
            "status": "refused",
            "reasons": [REFUSAL_REASON],
        },
        {
            "partition": "development",
            **ref(THREAD_B, "b3"),
            "status": "unavailable",
            "reasons": [UNAVAILABLE_REASON],
        },
    ]
    assert document["audit"]["excluded"] == [
        {
            "partition": "development",
            **ref(THREAD_B, "b2"),
            "language": "es",
            "reason": "language_unsupported",
        }
    ]


def test_build_evaluation_document_score_decision_uses_argmax_not_expected_ordinal(
    document: dict[str, Any],
) -> None:
    baseline = next(case for case in document["engineering_cases"] if case["case"] == "baseline")
    b1 = next(item for item in baseline["items"] if item["comment_id"] == "b1")
    relevance = document["partitions"]["development"]["questions"]["objective_relevance"]

    assert b1 == {
        "partition": "development",
        **ref(THREAD_B, "b1"),
        "question_id": "objective_relevance",
        "expected": 0,
        "actual": 0,
        "outcome": "correct",
    }
    assert relevance["labels"]["correct"] == 3
    assert relevance["labels"]["incorrect"] == 1
    assert relevance["labels"]["error_rate"] == 0.25
    assert relevance["labels"]["incorrect_items"] == [
        {**ref(THREAD_A, "a3"), "expected": 1, "actual": 0}
    ]


def test_build_evaluation_document_noul_threshold_decides_true_at_half(
    document: dict[str, Any],
) -> None:
    labels = document["partitions"]["development"]["questions"]["promotional_framing"]["labels"]

    assert (labels["correct"], labels["incorrect"], labels["error_rate"]) == (1, 1, 0.5)
    assert labels["incorrect_items"] == [{**ref(THREAD_A, "a2"), "expected": True, "actual": False}]


def test_build_evaluation_document_missing_labels_report_unavailable_not_zero_error(
    document: dict[str, Any],
) -> None:
    evaluation_theme = document["partitions"]["evaluation"]["questions"]["theme"]["labels"]
    evidence = document["partitions"]["development"]["questions"]["evidence_type"]["labels"]

    for labels in (evaluation_theme, evidence):
        assert labels["status"] == "unavailable"
        assert labels["reason"] == "no_labels_for_question"
        assert labels["labeled"] == 0
        assert labels["error_rate"] is None


def test_build_evaluation_document_ranking_agreement_matches_hand_computed_pairs(
    document: dict[str, Any],
) -> None:
    relevance = document["partitions"]["development"]["questions"]["objective_relevance"]

    # Decided labeled items (level, expected ordinal): a1 (2, 1.6), a2 (1, 1.0), a3 (1, 0.4),
    # b1 (0, 1.0). Pairs with different levels: a1>a2 1, a1>a3 1, a1>b1 1, a2=b1 0.5,
    # a3<b1 0 -> 3.5 / 5. Absolute errors 0.4 + 0 + 0.6 + 1.0 -> 2.0 / 4.
    assert relevance["ranking"]["status"] == "available"
    assert relevance["ranking"]["reason"] is None
    assert relevance["ranking"]["comparable_pairs"] == 5
    assert relevance["ranking"]["agreement"] == 0.7
    assert relevance["score_diagnostics"]["mean_absolute_error"] == 0.5


def test_build_evaluation_document_ranking_without_pairs_is_unavailable(
    document: dict[str, Any],
) -> None:
    ranking = document["partitions"]["evaluation"]["questions"]["objective_relevance"]["ranking"]
    actionability = document["partitions"]["development"]["questions"]["actionability"]

    assert ranking["status"] == "unavailable"
    assert ranking["reason"] == "no_comparable_labeled_pairs"
    assert ranking["comparable_pairs"] == 0
    assert ranking["agreement"] is None
    assert actionability["ranking"]["status"] == "unavailable"
    assert "relevant_evidence_misses" not in actionability


def test_build_evaluation_document_relevant_evidence_misses_lists_items(
    document: dict[str, Any],
) -> None:
    development = document["partitions"]["development"]["questions"]["objective_relevance"]
    evaluation = document["partitions"]["evaluation"]["questions"]["objective_relevance"]

    assert development["relevant_evidence_misses"] == {
        "status": "available",
        "reason": None,
        "definition": "labeled level >= 1",
        "labeled_relevant": 5,
        "predicted_not_relevant": [ref(THREAD_A, "a3")],
        "not_covered": [ref(THREAD_A, "a4")],
        "excluded": [ref(THREAD_B, "b2")],
    }
    assert evaluation["relevant_evidence_misses"]["status"] == "unavailable"
    assert evaluation["relevant_evidence_misses"]["reason"] == "no_labels_for_question"


def test_build_evaluation_document_confidence_means_split_by_outcome(
    document: dict[str, Any],
) -> None:
    questions = document["partitions"]["development"]["questions"]
    relevance = questions["objective_relevance"]["confidence"]
    theme = questions["theme"]["confidence"]

    assert (relevance["mean_correct"], relevance["mean_incorrect"]) == (0.516667, 0.7)
    assert relevance["mean_decided"] == 0.57
    assert (theme["mean_correct"], theme["mean_incorrect"], theme["mean_decided"]) == (
        0.8,
        0.55,
        0.7375,
    )


def test_build_evaluation_document_engineering_cases_group_outcomes_by_case(
    document: dict[str, Any],
) -> None:
    cases = {case["case"]: case for case in document["engineering_cases"]}

    assert list(cases) == ["baseline", "negation", "mixed_intent", "unsupported_language"]
    assert cases["baseline"]["labels"] == 8
    assert cases["baseline"]["outcomes"] == {
        "correct": 4,
        "incorrect": 2,
        "abstained": 0,
        "not_covered": 2,
        "excluded": 0,
    }
    assert cases["negation"]["items"] == [
        {
            "partition": "development",
            **ref(THREAD_A, "a3"),
            "question_id": "theme",
            "expected": OTHER_OR_UNCERTAIN,
            "actual": OTHER_OR_UNCERTAIN,
            "outcome": "abstained",
        }
    ]
    unsupported = cases["unsupported_language"]["items"]
    assert [(i["question_id"], i["actual"], i["outcome"]) for i in unsupported] == [
        ("objective_relevance", None, "excluded"),
        ("theme", None, "excluded"),
    ]


def test_build_evaluation_document_claims_disclose_engineering_label_origin(
    document: dict[str, Any],
) -> None:
    assert document["claims"]["label_origin"] == "engineering_authored"
    assert document["claims"]["independent_adjudication"] is False
    assert document["claims"]["population_accuracy"] == "not_claimed"
    assert document["dataset"]["labels"]["count"] == 13
    assert document["dataset"]["labels"]["by_case"] == {
        "baseline": 8,
        "negation": 1,
        "mixed_intent": 2,
        "unsupported_language": 2,
    }
    assert document["dataset"]["threads"][1]["comment_languages"] == {"b2": "es"}
    assert document["dataset"]["issue_groups"][0]["partition"] == "development"
    assert document["identity"] == {**BINDINGS.to_dict(), "language_basis": "declared"}


def test_heuristic_theme_decisions_concrete_texts_return_rule_basis(scenario: Scenario) -> None:
    by_comment = {
        decision.comment_id: decision
        for decisions in scenario.heuristic.values()
        for decision in decisions
    }

    assert by_comment["a1"] == HeuristicDecision(
        "a1", "documentation", "keyword_hits", "documentation"
    )
    assert by_comment["a2"] == HeuristicDecision(
        "a2", "reliability", "issue_marker_fallback", "reliability"
    )
    assert by_comment["a3"] == HeuristicDecision(
        "a3", OTHER_OR_UNCERTAIN, "default_theme", "general_feedback"
    )
    assert by_comment["a4"] == HeuristicDecision("a4", None, "noise_filtered", None)
    assert by_comment["b1"] == HeuristicDecision("b1", None, "noise_filtered", None)
    assert by_comment["b4"] == HeuristicDecision("b4", None, "empty_text", None)
    assert by_comment["c1"] == HeuristicDecision(
        "c1", "workflow", "request_marker_fallback", "workflow"
    )


def test_heuristic_theme_decisions_theme_outside_options_raises(scenario: Scenario) -> None:
    narrowed = replace(
        THEME_QUESTION,
        options=tuple(option for option in THEME_QUESTION.options if option[0] != "documentation"),
    )

    with pytest.raises(DecisionEvaluationError) as raised:
        heuristic_theme_decisions(scenario.corpus[0].thread, VOCABULARY, narrowed)

    assert raised.value.details["reason"] == "heuristic_theme_outside_options"
    assert raised.value.details["theme"] == "documentation"


def test_build_evaluation_document_heuristic_comparator_counts_identical_items(
    document: dict[str, Any],
) -> None:
    heuristic = document["comparators"]["heuristic"]
    development = heuristic["partitions"]["development"]

    assert heuristic["basis_counts"] == {
        "keyword_hits": 1,
        "issue_marker_fallback": 1,
        "request_marker_fallback": 1,
        "default_theme": 1,
        "noise_filtered": 1,
        "empty_text": 1,
    }
    assert development["items"] == 5
    assert development["typed"] == {
        "decided": 4,
        "abstained": 1,
        "labels": {
            "status": "available",
            "reason": None,
            "labeled": 3,
            "labeled_abstained": 1,
            "labeled_decided": 2,
            "correct": 1,
            "incorrect": 1,
            "error_rate": 0.5,
        },
    }
    assert development["heuristic"] == {
        "decided": 2,
        "abstained": 3,
        "labels": {
            "status": "available",
            "reason": None,
            "labeled": 3,
            "labeled_abstained": 1,
            "labeled_decided": 2,
            "correct": 2,
            "incorrect": 0,
            "error_rate": 0.0,
        },
    }
    assert development["agreement"] == {"both_decided": 2, "agreed": 1, "rate": 0.5}
    assert heuristic["partitions"]["evaluation"]["agreement"] == {
        "both_decided": 1,
        "agreed": 1,
        "rate": 1.0,
    }
    assert heuristic["partitions"]["calibration"]["agreement"]["rate"] is None
    assert [entry["question_id"] for entry in heuristic["unsupported_questions"]] == [
        "objective_relevance",
        "evidence_type",
        "request_intent",
        "actionability",
        "promotional_framing",
    ]


def test_build_evaluation_document_static_blocks_record_generative_and_option_stability(
    document: dict[str, Any],
) -> None:
    generative = document["comparators"]["generative"]
    stability = document["option_stability"]

    assert generative["name"] == GENERATIVE_COMPARATOR_NAME
    assert generative["comparison"] == "not_performed"
    assert generative["shape_support"] == generative_shape_support(QUESTION_SET)
    assert stability["status"] == "vacuous"
    assert stability["probes"] == []
    assert [layout["question_id"] for layout in stability["layouts"]] == [
        "evidence_type",
        "request_intent",
        "theme",
    ]
    assert stability["layouts"][2]["options"] == list(THEME_QUESTION.labels)


def test_evaluation_bytes_identical_inputs_produce_identical_bytes() -> None:
    first = evaluation_bytes(build_scenario().document())
    second = evaluation_bytes(build_scenario().document())

    assert first == second
    assert hashlib.sha256(first).hexdigest() == hashlib.sha256(second).hexdigest()
    assert first.endswith(b"}\n")


def test_build_evaluation_document_decision_count_mismatch_raises(scenario: Scenario) -> None:
    shortened = replace(scenario, decisions=scenario.decisions[:2])

    with pytest.raises(DecisionEvaluationError) as raised:
        shortened.document()

    assert raised.value.details["reason"] == "reconciliation_failed"
    assert raised.value.details["field"] == "decisions"


def test_build_evaluation_document_replay_digest_mismatch_raises(scenario: Scenario) -> None:
    stale = replace(scenario.replays[0], decision_sha256=scenario.decisions[1].decision_sha256)
    tampered = replace(scenario, replays=(stale, *scenario.replays[1:]))

    with pytest.raises(DecisionEvaluationError) as raised:
        tampered.document()

    assert raised.value.details["reason"] == "reconciliation_failed"
    assert raised.value.details["field"] == "replays"


def test_reconcile_population_without_matching_audit_raises(document: dict[str, Any]) -> None:
    audit = {**document["audit"], "rejected": document["audit"]["rejected"][:1]}

    with pytest.raises(DecisionEvaluationError) as raised:
        assessment._reconcile(document["partitions"], audit)

    assert raised.value.details == {
        "reason": "reconciliation_failed",
        "partition": "development",
        "identity": "rejected_audit",
    }


class RecordingHandler:
    def __init__(self, response: httpx.Response | None) -> None:
        self.response = response
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.response is None:
            raise httpx.ConnectError("connection refused", request=request)
        return self.response


def observe(handler: RecordingHandler, **runtime: Any) -> dict[str, Any]:
    config = RuntimeConfig(**{"base_url": LOOPBACK, "model": CONFIGURED_MODEL, **runtime})
    return observe_generative_availability(config, transport=httpx.MockTransport(handler))


def listing(*model_ids: str) -> httpx.Response:
    return httpx.Response(200, json={"object": "list", "data": [{"id": m} for m in model_ids]})


def test_observe_generative_availability_disabled_makes_no_request() -> None:
    handler = RecordingHandler(listing(CONFIGURED_MODEL))

    observation = observe(handler, enabled=False)

    assert handler.requests == []
    assert observation["status"] == "not_observed"
    assert observation["reason"] == "disabled_by_configuration"


def test_observe_generative_availability_non_loopback_makes_no_request() -> None:
    handler = RecordingHandler(listing(CONFIGURED_MODEL))

    observation = observe(handler, base_url="http://localhost:11434")

    assert handler.requests == []
    assert observation["status"] == "not_observed"
    assert observation["reason"] == "endpoint_not_literal_loopback"


def test_observe_generative_availability_portless_endpoint_is_endpoint_invalid() -> None:
    handler = RecordingHandler(listing(CONFIGURED_MODEL))

    observation = observe(handler, base_url="http://127.0.0.1")

    assert handler.requests == []
    assert observation["status"] == "not_observed"
    assert observation["reason"] == "endpoint_invalid"


def test_observe_generative_availability_connect_error_is_runtime_unreachable() -> None:
    handler = RecordingHandler(None)

    observation = observe(handler)

    assert len(handler.requests) == 1
    assert observation["reason"] == "runtime_unreachable"
    assert observation["error_type"] == "ConnectError"
    assert observation["served_models"] is None


def test_observe_generative_availability_non_200_records_status_code() -> None:
    handler = RecordingHandler(httpx.Response(503, text="loading"))

    observation = observe(handler)

    assert (observation["reason"], observation["status_code"]) == ("listing_http_status", 503)


def test_observe_generative_availability_model_missing_lists_served_models() -> None:
    handler = RecordingHandler(listing("qwen3:8b", "llama3:8b"))

    observation = observe(handler)

    assert observation["status"] == "unavailable"
    assert observation["reason"] == "configured_model_not_served"
    assert observation["served_models"] == ["llama3:8b", "qwen3:8b"]


def test_observe_generative_availability_model_served_is_available() -> None:
    handler = RecordingHandler(listing("qwen3:8b", CONFIGURED_MODEL))

    observation = observe(handler)

    assert [(r.method, str(r.url)) for r in handler.requests] == [("GET", f"{LOOPBACK}/v1/models")]
    assert observation["status"] == "available"
    assert observation["reason"] is None
    assert observation["served_models"] == [CONFIGURED_MODEL, "qwen3:8b"]


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, json={"data": [{"name": CONFIGURED_MODEL}]}),
        httpx.Response(200, json={"data": {"id": CONFIGURED_MODEL}}),
        httpx.Response(200, content=b"not json"),
    ],
)
def test_observe_generative_availability_malformed_listing_is_listing_invalid(
    response: httpx.Response,
) -> None:
    observation = observe(RecordingHandler(response))

    assert observation["status"] == "unavailable"
    assert observation["reason"] == "listing_invalid"
    assert observation["served_models"] is None


def test_paired_generative_metrics_preserve_full_populations(scenario: Scenario) -> None:
    from threadsense.decisions.generative import GenerativeCaptureResult

    baseline = scenario.document()
    rows: tuple[dict[str, Any], ...] = tuple(
        {
            "thread_id": thread.entry.thread_id,
            "comment_id": comment.comment_id,
            "partition": thread.entry.partition.value,
            "status": "decided",
            "label": next(label for label in THEME_QUESTION.labels if label != OTHER_OR_UNCERTAIN),
            "raw_theme": next(
                label for label in THEME_QUESTION.labels if label != OTHER_OR_UNCERTAIN
            ),
            "reason": None,
        }
        for thread, saved in zip(scenario.corpus, scenario.decisions, strict=True)
        for comment, item in zip(thread.comments, saved.items, strict=True)
        if item.status is ItemStatus.OK
        and comment.language in scenario.manifest.supported_languages
        and comment.chars <= 300
    )
    result = GenerativeCaptureResult(
        {"status": "available", "reason": None},
        {"capture_sha256": "a" * 64},
        rows,
        (),
    )
    document = scenario.document(result)
    assert document["partitions"] == baseline["partitions"]
    assert document["comparators"]["heuristic"] == baseline["comparators"]["heuristic"]
    comparison = document["comparators"]["generative"]
    assert comparison["comparison"] == "performed"
    for partition in Partition:
        block = comparison["partitions"][partition.value]
        expected = [row for row in rows if row["partition"] == partition.value]
        assert block["items"] == len(expected)
        assert block["ids"] == [ref(row["thread_id"], row["comment_id"]) for row in expected]
        assert block["generative"]["decided"] == len(expected)
        for comparator in ("typed", "heuristic", "generative"):
            metrics = block[comparator]
            assert metrics["decided"] + metrics["abstained"] == len(expected)
            labels = metrics["labels"]
            assert labels["correct"] + labels["incorrect"] == labels["labeled_decided"]
    with pytest.raises(DecisionEvaluationError, match="aligned"):
        scenario.document(replace(result, rows=rows[:-1]))
