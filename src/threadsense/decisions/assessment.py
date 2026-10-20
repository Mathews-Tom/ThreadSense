"""Deterministic evaluation document for frozen typed decision artifacts.

Every metric names its denominator and is null when that denominator is zero; missing labels
are reported as unavailable, never as zero error. Decisions are read from the reported
distributions (score argmax, noul threshold), never from the expected ordinal, and answer
confidence stays an uncalibrated quantity. The document carries no timestamps, run ids,
endpoints or local paths, so identical inputs reproduce identical bytes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import combinations
from typing import Any

from threadsense.decisions.artifact import decision_bytes
from threadsense.decisions.comparators import (
    GENERATIVE_COMPARATOR_NAME,
    HEURISTIC_BASES,
    HEURISTIC_COMPARATOR_NAME,
    HEURISTIC_QUESTION_ID,
    generative_shape_support,
)
from threadsense.decisions.contracts import (
    CALIBRATION,
    MODE,
    OTHER_OR_UNCERTAIN,
    POLICY,
    ChoiceQuestion,
    ItemStatus,
    Question,
    QuestionSet,
    QuestionType,
)
from threadsense.decisions.evaluation_contracts import (
    ABSTENTION_RULE,
    EVALUATION_ARTIFACT_KIND,
    EVALUATION_SCHEMA_VERSION,
    LANGUAGE_EXCLUSION_REASON,
    NOUL_DECISION_RULE,
    NOUL_TRUE_THRESHOLD,
    SCORE_DECISION_RULE,
    CaseCategory,
    CorpusComment,
    CorpusThread,
    EvaluationManifest,
    ExpectedValue,
    HeuristicDecision,
    Label,
    LabelSet,
    Partition,
    PartitionIntegrity,
    ReplayReport,
    SavedAnswer,
    SavedDecision,
    SavedItem,
)
from threadsense.decisions.generative import GenerativeCaptureResult
from threadsense.errors import DecisionEvaluationError

RELEVANCE_QUESTION_ID = "objective_relevance"
_OUTCOMES = ("correct", "incorrect", "abstained", "not_covered", "excluded")
_DECIDED = ("correct", "incorrect")
_COMPARATOR_LABEL_KEYS = (
    "status",
    "reason",
    "labeled",
    "labeled_abstained",
    "labeled_decided",
    "correct",
    "incorrect",
    "error_rate",
)
_CLAIMS_NOTE = (
    "Metrics describe only the listed inputs against their declared labels; they are not "
    "representative accuracy and confidence values are uncalibrated."
)
_LANGUAGE_EXCLUSION_RULE = (
    "ok items whose declared language is not supported are excluded from the eligible population"
)
_CONFIDENCE_DEFINITION = "answer_confidence; uncalibrated; not a probability of correctness"
_RANKING_METHOD = (
    "pairwise order agreement of expected ordinal levels against labeled levels; ties count 0.5"
)
_HEURISTIC_MAPPING = (
    "heuristic default theme -> other_or_uncertain; marker fallbacks keep their fallback theme; "
    "empty or noise-filtered comments make no decision"
)
_STABILITY_REASON = (
    "each question set has one fixed option layout and no permuted-layout probe was run; "
    "stability across option order is not measured"
)


def evaluation_bytes(document: dict[str, Any]) -> bytes:
    return decision_bytes(document)


def _fail(message: str, reason: str, **details: Any) -> DecisionEvaluationError:
    return DecisionEvaluationError(message, {"reason": reason, **details})


def _misaligned(field: str, **details: Any) -> DecisionEvaluationError:
    return _fail(
        "assessment inputs are not aligned with the corpus",
        "reconciliation_failed",
        field=field,
        **details,
    )


def _rate(numerator: float, denominator: int) -> float | None:
    return None if denominator == 0 else round(numerator / denominator, 6)


def _mean(values: Sequence[float]) -> float | None:
    return _rate(sum(values), len(values))


@dataclass(frozen=True)
class _Item:
    partition: Partition
    thread_id: str
    position: tuple[int, int]
    comment: CorpusComment
    saved: SavedItem
    supported: bool
    heuristic: HeuristicDecision

    @property
    def comment_id(self) -> str:
        return self.comment.comment_id

    @property
    def excluded(self) -> bool:
        return self.saved.status is ItemStatus.OK and not self.supported

    @property
    def eligible(self) -> bool:
        return self.saved.status is ItemStatus.OK and self.supported

    def ref(self) -> dict[str, Any]:
        return {"thread_id": self.thread_id, "comment_id": self.comment_id}


@dataclass(frozen=True)
class _Scored:
    item: _Item
    label: Label
    outcome: str
    actual: ExpectedValue | None
    answer: SavedAnswer | None


@dataclass(frozen=True)
class _Context:
    question_set: QuestionSet
    items: tuple[_Item, ...]
    typed: tuple[_Scored, ...]

    def partition_items(self, partition: Partition) -> list[_Item]:
        return [item for item in self.items if item.partition is partition]

    def scored(self, partition: Partition, question_id: str) -> list[_Scored]:
        return [
            entry
            for entry in self.typed
            if entry.item.partition is partition and entry.label.question_id == question_id
        ]


def _answer(item: _Item, question_id: str) -> SavedAnswer:
    answer = item.saved.answer(question_id)
    if answer is None:
        raise _misaligned(
            "answers",
            thread_id=item.thread_id,
            comment_id=item.comment_id,
            question_id=question_id,
        )
    return answer


def _score_level(answer: SavedAnswer) -> int:
    if not answer.probabilities:
        raise _misaligned("probabilities", question_id=answer.question_id)
    best = 0
    for index, (_level, probability) in enumerate(answer.probabilities):
        if probability > answer.probabilities[best][1]:
            best = index
    return best


def _decision(answer: SavedAnswer) -> ExpectedValue:
    if answer.question_type is QuestionType.SCORE:
        return _score_level(answer)
    if answer.question_type is QuestionType.NOUL:
        if answer.noul is None:
            raise _misaligned("noul", question_id=answer.question_id)
        return answer.noul >= NOUL_TRUE_THRESHOLD
    if answer.choice is None:
        raise _misaligned("choice", question_id=answer.question_id)
    return answer.choice


def _expected_ordinal(answer: SavedAnswer) -> float:
    if answer.score is None:
        raise _misaligned("score", question_id=answer.question_id)
    return answer.score


def _labeled_level(label: Label) -> int:
    if isinstance(label.expected, bool) or not isinstance(label.expected, int):
        raise _misaligned("labels", question_id=label.question_id, expected=label.expected)
    return label.expected


def _decided_outcome(expected: ExpectedValue, actual: ExpectedValue) -> str:
    if actual == OTHER_OR_UNCERTAIN:
        return "abstained"
    if type(expected) is type(actual) and expected == actual:
        return "correct"
    return "incorrect"


def _typed_outcome(item: _Item, label: Label) -> _Scored:
    if item.saved.status is not ItemStatus.OK:
        return _Scored(item, label, "not_covered", None, None)
    if not item.supported:
        return _Scored(item, label, "excluded", None, None)
    answer = _answer(item, label.question_id)
    actual = _decision(answer)
    return _Scored(item, label, _decided_outcome(label.expected, actual), actual, answer)


def _heuristic_outcome(item: _Item, label: Label) -> _Scored:
    actual = item.heuristic.label
    outcome = "abstained" if actual is None else _decided_outcome(label.expected, actual)
    return _Scored(item, label, outcome, actual, None)


def _check_thread(
    position: int, thread: CorpusThread, decision: SavedDecision, replay: ReplayReport
) -> None:
    thread_id = thread.entry.thread_id
    if decision.snapshot_sha256 != thread.snapshot_sha256 or decision.thread_id != thread_id:
        raise _misaligned("decisions", position=position, thread_id=thread_id)
    if replay.decision_sha256 != decision.decision_sha256:
        raise _misaligned("replays", position=position, thread_id=thread_id)
    saved = [(item.comment_id, item.comment_text_sha256) for item in decision.items]
    corpus = [(comment.comment_id, comment.text_sha256) for comment in thread.comments]
    if saved != corpus:
        raise _misaligned("decision_items", position=position, thread_id=thread_id)


def _thread_heuristic(
    thread: CorpusThread, heuristic: Mapping[str, Sequence[HeuristicDecision]]
) -> Sequence[HeuristicDecision]:
    thread_id = thread.entry.thread_id
    if thread_id not in heuristic:
        raise _misaligned("heuristic", thread_id=thread_id)
    decisions = heuristic[thread_id]
    if [entry.comment_id for entry in decisions] != [c.comment_id for c in thread.comments]:
        raise _misaligned("heuristic", thread_id=thread_id)
    return decisions


def _build_items(
    manifest: EvaluationManifest,
    corpus: Sequence[CorpusThread],
    decisions: Sequence[SavedDecision],
    heuristic: Mapping[str, Sequence[HeuristicDecision]],
) -> tuple[_Item, ...]:
    supported = set(manifest.supported_languages)
    items: list[_Item] = []
    for position, (thread, decision) in enumerate(zip(corpus, decisions, strict=True)):
        thread_heuristic = _thread_heuristic(thread, heuristic)
        for comment, saved, guess in zip(
            thread.comments, decision.items, thread_heuristic, strict=True
        ):
            items.append(
                _Item(
                    partition=thread.entry.partition,
                    thread_id=thread.entry.thread_id,
                    position=(position, comment.index),
                    comment=comment,
                    saved=saved,
                    supported=comment.language in supported,
                    heuristic=guess,
                )
            )
    return tuple(items)


def _check_alignment(
    manifest: EvaluationManifest,
    corpus: Sequence[CorpusThread],
    decisions: Sequence[SavedDecision],
    replays: Sequence[ReplayReport],
) -> None:
    manifest_ids = [entry.thread_id for entry in manifest.threads]
    if [thread.entry.thread_id for thread in corpus] != manifest_ids:
        raise _misaligned("corpus", expected=manifest_ids)
    if len(decisions) != len(corpus):
        raise _misaligned("decisions", expected=len(corpus), actual=len(decisions))
    if len(replays) != len(corpus):
        raise _misaligned("replays", expected=len(corpus), actual=len(replays))
    for position, (thread, decision, replay) in enumerate(
        zip(corpus, decisions, replays, strict=True)
    ):
        _check_thread(position, thread, decision, replay)


def _ordered_labels(
    labels: LabelSet, items: Sequence[_Item], question_set: QuestionSet
) -> list[tuple[_Item, Label]]:
    by_key = {(item.thread_id, item.comment_id): item for item in items}
    question_order = {qid: index for index, qid in enumerate(question_set.question_ids())}
    keyed: list[tuple[tuple[int, int, int], _Item, Label]] = []
    for label in labels.labels:
        item = by_key.get((label.thread_id, label.comment_id))
        if item is None or label.question_id not in question_order:
            raise _misaligned(
                "labels",
                thread_id=label.thread_id,
                comment_id=label.comment_id,
                question_id=label.question_id,
            )
        keyed.append(((*item.position, question_order[label.question_id]), item, label))
    keyed.sort(key=lambda entry: entry[0])
    return [(item, label) for _key, item, label in keyed]


def _outcome_counts(scored: Sequence[_Scored]) -> dict[str, int]:
    return {outcome: sum(1 for s in scored if s.outcome == outcome) for outcome in _OUTCOMES}


def _label_block(scored: Sequence[_Scored]) -> dict[str, Any]:
    counts = _outcome_counts(scored)
    decided = counts["correct"] + counts["incorrect"]
    return {
        "status": "available" if scored else "unavailable",
        "reason": None if scored else "no_labels_for_question",
        "labeled": len(scored),
        "labeled_not_covered": counts["not_covered"],
        "labeled_excluded": counts["excluded"],
        "labeled_abstained": counts["abstained"],
        "labeled_decided": decided,
        "correct": counts["correct"],
        "incorrect": counts["incorrect"],
        "error_rate": _rate(counts["incorrect"], decided),
        "incorrect_items": [
            {**s.item.ref(), "expected": s.label.expected, "actual": s.actual}
            for s in scored
            if s.outcome == "incorrect"
        ],
    }


def _comparator_labels(scored: Sequence[_Scored]) -> dict[str, Any]:
    block = _label_block(scored)
    return {key: block[key] for key in _COMPARATOR_LABEL_KEYS}


def _confidences(scored: Sequence[_Scored], outcome: str) -> list[float]:
    return [
        s.answer.answer_confidence for s in scored if s.outcome == outcome and s.answer is not None
    ]


def _confidence_block(scored: Sequence[_Scored], decided: Sequence[SavedAnswer]) -> dict[str, Any]:
    return {
        "definition": _CONFIDENCE_DEFINITION,
        "mean_correct": _mean(_confidences(scored, "correct")),
        "mean_incorrect": _mean(_confidences(scored, "incorrect")),
        "mean_decided": _mean([answer.answer_confidence for answer in decided]),
    }


def _decided_scored(scored: Sequence[_Scored]) -> list[tuple[int, float]]:
    """(labeled level, expected ordinal) for labeled items that were decided."""
    pairs: list[tuple[int, float]] = []
    for entry in scored:
        if entry.outcome in _DECIDED and entry.answer is not None:
            pairs.append((_labeled_level(entry.label), _expected_ordinal(entry.answer)))
    return pairs


def _score_diagnostics(scored: Sequence[_Scored]) -> dict[str, Any]:
    decided = _decided_scored(scored)
    return {
        "basis": "expected ordinal level versus labeled level",
        "mean_absolute_error": _mean([abs(ordinal - level) for level, ordinal in decided]),
    }


def _pair_credit(first: tuple[int, float], second: tuple[int, float]) -> float:
    higher, lower = (first, second) if first[0] > second[0] else (second, first)
    if higher[1] > lower[1]:
        return 1.0
    return 0.5 if higher[1] == lower[1] else 0.0


def _ranking(scored: Sequence[_Scored]) -> dict[str, Any]:
    credits = [
        _pair_credit(first, second)
        for first, second in combinations(_decided_scored(scored), 2)
        if first[0] != second[0]
    ]
    return {
        "status": "available" if credits else "unavailable",
        "reason": None if credits else "no_comparable_labeled_pairs",
        "method": _RANKING_METHOD,
        "comparable_pairs": len(credits),
        "agreement": _mean(credits),
    }


def _relevant_evidence_misses(scored: Sequence[_Scored]) -> dict[str, Any]:
    relevant = [s for s in scored if _labeled_level(s.label) >= 1]
    return {
        "status": "available" if scored else "unavailable",
        "reason": None if scored else "no_labels_for_question",
        "definition": "labeled level >= 1",
        "labeled_relevant": len(relevant),
        "predicted_not_relevant": [
            s.item.ref() for s in relevant if s.outcome in _DECIDED and s.actual == 0
        ],
        "not_covered": [s.item.ref() for s in relevant if s.outcome == "not_covered"],
        "excluded": [s.item.ref() for s in relevant if s.outcome == "excluded"],
    }


def _question_metrics(
    question: Question, items: Sequence[_Item], scored: Sequence[_Scored]
) -> dict[str, Any]:
    answers = [_answer(item, question.question_id) for item in items if item.eligible]
    decided = [answer for answer in answers if _decision(answer) != OTHER_OR_UNCERTAIN]
    abstained = len(answers) - len(decided)
    metrics: dict[str, Any] = {
        "question_type": question.question_type.value,
        "population": len(items),
        "eligible": len(answers),
        "abstained": abstained,
        "decided": len(decided),
        "decision_rate": _rate(len(decided), len(items)),
        "labels": _label_block(scored),
        "confidence": _confidence_block(scored, decided),
    }
    if question.question_type is QuestionType.SCORE:
        metrics["score_diagnostics"] = _score_diagnostics(scored)
        metrics["ranking"] = _ranking(scored)
    if question.question_id == RELEVANCE_QUESTION_ID:
        metrics["relevant_evidence_misses"] = _relevant_evidence_misses(scored)
    return metrics


def _population(items: Sequence[_Item], threads: int) -> dict[str, int]:
    statuses = [item.saved.status for item in items]
    ok = statuses.count(ItemStatus.OK)
    excluded = sum(1 for item in items if item.excluded)
    return {
        "threads": threads,
        "comments": len(items),
        "ok": ok,
        "refused": statuses.count(ItemStatus.REFUSED),
        "unavailable": statuses.count(ItemStatus.UNAVAILABLE),
        "excluded_language": excluded,
        "eligible": sum(1 for item in items if item.eligible),
    }


def _partition_block(
    context: _Context, partition: Partition, corpus: Sequence[CorpusThread]
) -> dict[str, Any]:
    items = context.partition_items(partition)
    threads = sum(1 for thread in corpus if thread.entry.partition is partition)
    return {
        "population": _population(items, threads),
        "questions": {
            question.question_id: _question_metrics(
                question, items, context.scored(partition, question.question_id)
            )
            for question in context.question_set.questions
        },
    }


def _audit(context: _Context) -> dict[str, list[dict[str, Any]]]:
    rejected: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    abstained: list[dict[str, Any]] = []
    for item in context.items:
        base = {"partition": item.partition.value, **item.ref()}
        if item.saved.status is not ItemStatus.OK:
            reasons = list(item.saved.reasons)
            rejected.append({**base, "status": item.saved.status.value, "reasons": reasons})
        elif item.excluded:
            language = item.comment.language
            excluded.append({**base, "language": language, "reason": LANGUAGE_EXCLUSION_REASON})
        else:
            abstained.extend(
                {**base, "question_id": question_id, "reason": "chose other_or_uncertain"}
                for question_id in context.question_set.question_ids()
                if _decision(_answer(item, question_id)) == OTHER_OR_UNCERTAIN
            )
    return {"rejected": rejected, "excluded": excluded, "abstained": abstained}


def _identity_failure(partition: str, identity: str) -> DecisionEvaluationError:
    return _fail(
        "assessment counts do not reconcile",
        "reconciliation_failed",
        partition=partition,
        identity=identity,
    )


def _reconcile_questions(name: str, block: dict[str, Any], abstained: list[str]) -> None:
    population = block["population"]
    for question_id, metrics in block["questions"].items():
        labels = metrics["labels"]
        checks = {
            "question_population": metrics["population"] == population["comments"],
            "question_eligible": metrics["eligible"] == population["eligible"],
            "question_decided": metrics["decided"] + metrics["abstained"] == metrics["eligible"],
            "abstained_audit": abstained.count(question_id) == metrics["abstained"],
            "labeled": labels["labeled"]
            == labels["labeled_not_covered"]
            + labels["labeled_excluded"]
            + labels["labeled_abstained"]
            + labels["labeled_decided"],
            "labeled_decided": labels["labeled_decided"] == labels["correct"] + labels["incorrect"],
        }
        for identity, holds in checks.items():
            if not holds:
                raise _identity_failure(name, f"{question_id}.{identity}")


def _reconcile(partitions: Mapping[str, dict[str, Any]], audit: Mapping[str, Any]) -> None:
    for name, block in partitions.items():
        population = block["population"]
        rejected = [e for e in audit["rejected"] if e["partition"] == name]
        excluded = [e for e in audit["excluded"] if e["partition"] == name]
        abstained = [e["question_id"] for e in audit["abstained"] if e["partition"] == name]
        checks = {
            "comments": population["comments"]
            == population["ok"] + population["refused"] + population["unavailable"],
            "eligible": population["eligible"]
            == population["ok"] - population["excluded_language"],
            "rejected_audit": len(rejected) == population["refused"] + population["unavailable"],
            "excluded_audit": len(excluded) == population["excluded_language"],
        }
        for identity, holds in checks.items():
            if not holds:
                raise _identity_failure(name, identity)
        _reconcile_questions(name, block, abstained)


def _engineering_cases(context: _Context) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for case in CaseCategory:
        scored = [s for s in context.typed if s.label.case is case]
        if not scored:
            continue
        outcomes = _outcome_counts(scored)
        items = [
            {
                "partition": s.item.partition.value,
                **s.item.ref(),
                "question_id": s.label.question_id,
                "expected": s.label.expected,
                "actual": s.actual,
                "outcome": s.outcome,
            }
            for s in scored
        ]
        cases.append(
            {"case": case.value, "labels": len(scored), "outcomes": outcomes, "items": items}
        )
    return cases


def _heuristic_decided(label: str | None) -> bool:
    return label is not None and label != OTHER_OR_UNCERTAIN


def _heuristic_partition(context: _Context, partition: Partition) -> dict[str, Any]:
    items = [item for item in context.partition_items(partition) if item.eligible]
    typed_actual = [_decision(_answer(item, HEURISTIC_QUESTION_ID)) for item in items]
    typed_decided = [actual != OTHER_OR_UNCERTAIN for actual in typed_actual]
    guesses = [item.heuristic.label for item in items]
    both = [
        (actual, guess)
        for actual, guess, decided in zip(typed_actual, guesses, typed_decided, strict=True)
        if decided and _heuristic_decided(guess)
    ]
    agreed = sum(1 for actual, guess in both if actual == guess)
    typed = [s for s in context.scored(partition, HEURISTIC_QUESTION_ID) if s.item.eligible]
    heuristic = [_heuristic_outcome(s.item, s.label) for s in typed]
    guessed = sum(1 for guess in guesses if _heuristic_decided(guess))
    return {
        "items": len(items),
        "typed": {
            "decided": sum(typed_decided),
            "abstained": len(items) - sum(typed_decided),
            "labels": _comparator_labels(typed),
        },
        "heuristic": {
            "decided": guessed,
            "abstained": len(items) - guessed,
            "labels": _comparator_labels(heuristic),
        },
        "agreement": {
            "both_decided": len(both),
            "agreed": agreed,
            "rate": _rate(agreed, len(both)),
        },
    }


def _heuristic_comparator(context: _Context) -> dict[str, Any]:
    if not isinstance(context.question_set.by_id().get(HEURISTIC_QUESTION_ID), ChoiceQuestion):
        raise _misaligned("question_set", question_id=HEURISTIC_QUESTION_ID)
    eligible = [item for item in context.items if item.eligible]
    return {
        "name": HEURISTIC_COMPARATOR_NAME,
        "question_id": HEURISTIC_QUESTION_ID,
        "mapping": _HEURISTIC_MAPPING,
        "population": "identical items: typed-eligible comments (ok and supported language)",
        "basis_counts": {
            basis: sum(1 for item in eligible if item.heuristic.basis == basis)
            for basis in HEURISTIC_BASES
        },
        "partitions": {
            partition.value: _heuristic_partition(context, partition) for partition in Partition
        },
        "unsupported_questions": [
            {"question_id": question_id, "reason": "no_existing_heuristic_counterpart"}
            for question_id in context.question_set.question_ids()
            if question_id != HEURISTIC_QUESTION_ID
        ],
    }


def _generative_comparator(
    context: _Context, result: GenerativeCaptureResult | None
) -> dict[str, Any]:
    rows = () if result is None else result.rows
    by_key = {(row["thread_id"], row["comment_id"]): row for row in rows}
    if len(by_key) != len(rows):
        raise _misaligned("generative_rows")
    expected_keys = {
        (item.thread_id, item.comment_id)
        for item in context.items
        if item.eligible and item.comment.chars <= 300
    }
    performed = result is not None and result.capture is not None
    if set(by_key) != (expected_keys if performed else set()):
        raise _misaligned("generative_population")
    for item in context.items:
        row = by_key.get((item.thread_id, item.comment_id))
        if row is not None and (
            row["partition"] != item.partition.value
            or row["status"] not in ("decided", "abstained")
            or (row["status"] == "abstained") != (row["label"] == OTHER_OR_UNCERTAIN)
        ):
            raise _misaligned("generative_rows")
    partitions: dict[str, Any] = {}
    for partition in Partition:
        population = context.partition_items(partition)
        items = [item for item in population if (item.thread_id, item.comment_id) in by_key]
        keys = {(item.thread_id, item.comment_id) for item in items}
        typed = [
            entry
            for entry in context.scored(partition, HEURISTIC_QUESTION_ID)
            if (entry.item.thread_id, entry.item.comment_id) in keys
        ]
        actuals: dict[str, list[Any]] = {
            "typed": [_decision(_answer(item, HEURISTIC_QUESTION_ID)) for item in items],
            "heuristic": [item.heuristic.label for item in items],
            "generative": [by_key[(item.thread_id, item.comment_id)]["label"] for item in items],
        }
        metrics: dict[str, Any] = {}
        for name, values in actuals.items():
            values_by_key = dict(
                zip([(item.thread_id, item.comment_id) for item in items], values, strict=True)
            )
            scored = [
                _Scored(
                    entry.item,
                    entry.label,
                    "abstained"
                    if values_by_key[(entry.item.thread_id, entry.item.comment_id)] is None
                    else _decided_outcome(
                        entry.label.expected,
                        values_by_key[(entry.item.thread_id, entry.item.comment_id)],
                    ),
                    values_by_key[(entry.item.thread_id, entry.item.comment_id)],
                    None,
                )
                for entry in typed
            ]
            decided = sum(_heuristic_decided(value) for value in values)
            metrics[name] = {
                "decided": decided,
                "abstained": len(items) - decided,
                "abstentions": [
                    item.ref()
                    for item, value in zip(items, values, strict=True)
                    if not _heuristic_decided(value)
                ],
                "labels": _comparator_labels(scored),
            }
        agreements: dict[str, Any] = {}
        for first, second in combinations(actuals, 2):
            pairs = [
                (a, b)
                for a, b in zip(actuals[first], actuals[second], strict=True)
                if _heuristic_decided(a) and _heuristic_decided(b)
            ]
            agreed = sum(a == b for a, b in pairs)
            agreements[f"{first}_{second}"] = {
                "both_decided": len(pairs),
                "agreed": agreed,
                "rate": _rate(agreed, len(pairs)),
            }
        partitions[partition.value] = {
            "items": len(items),
            "ids": [item.ref() for item in items],
            "excluded": len(population) - len(items),
            "exclusions": [
                {
                    **item.ref(),
                    "reason": "typed_ineligible_or_unsupported_language"
                    if not item.eligible
                    else "body_exceeds_300_characters"
                    if item.comment.chars > 300
                    else "comparison_not_available",
                }
                for item in population
                if (item.thread_id, item.comment_id) not in keys
            ],
            **metrics,
            "agreement": agreements,
        }
    return {
        "name": GENERATIVE_COMPARATOR_NAME,
        "comparison": "performed" if performed else "not_performed",
        "reason": None if performed else "no_frozen_generative_capture",
        "availability": "observed per run; see the evaluation run record",
        "capture_sha256": result.capture["capture_sha256"]
        if result is not None and result.capture is not None
        else None,
        "question_id": HEURISTIC_QUESTION_ID,
        "population": (
            "identical typed-eligible supported-language comments with bodies <=300 characters"
        ),
        "partitions": partitions,
        "shape_support": generative_shape_support(context.question_set),
    }


def _option_stability(question_set: QuestionSet) -> dict[str, Any]:
    return {
        "status": "vacuous",
        "reason": _STABILITY_REASON,
        "layouts": [
            {"question_id": question.question_id, "options": list(question.labels)}
            for question in question_set.questions
            if isinstance(question, ChoiceQuestion)
        ],
        "probes": [],
    }


def _labels_summary(labels: LabelSet) -> dict[str, Any]:
    by_case = {
        case.value: sum(1 for label in labels.labels if label.case is case) for case in CaseCategory
    }
    return {
        "sha256": labels.sha256,
        "origin": labels.origin.to_dict(),
        "count": len(labels.labels),
        "by_case": {case: count for case, count in by_case.items() if count},
    }


def _dataset_block(
    manifest: EvaluationManifest, labels: LabelSet, corpus: Sequence[CorpusThread]
) -> dict[str, Any]:
    partition_of = {thread.entry.thread_id: thread.entry.partition for thread in corpus}
    return {
        "manifest_sha256": manifest.sha256,
        "dataset_id": manifest.dataset_id,
        "description": manifest.description,
        "objective": manifest.objective.value,
        "domain": manifest.domain,
        "supported_languages": list(manifest.supported_languages),
        "sampling": manifest.sampling.to_dict(),
        "labels": _labels_summary(labels),
        "threads": [
            {
                "thread_id": thread.entry.thread_id,
                "partition": thread.entry.partition.value,
                "snapshot_sha256": thread.snapshot_sha256,
                "thread_text_sha256": thread.thread_text_sha256,
                "comment_count": len(thread.comments),
                "source_language": thread.entry.source_language,
                "comment_languages": dict(thread.entry.comment_languages),
            }
            for thread in corpus
        ],
        "issue_groups": [
            {
                "group_id": group.group_id,
                "partition": partition_of[group.members[0][0]].value,
                "members": [
                    {"thread_id": thread_id, "comment_id": comment_id}
                    for thread_id, comment_id in group.members
                ],
                "note": group.note,
            }
            for group in manifest.issue_groups
        ],
    }


def _annotations(
    corpus: Sequence[CorpusThread],
    decisions: Sequence[SavedDecision],
    replays: Sequence[ReplayReport],
) -> list[dict[str, Any]]:
    return [
        {
            "thread_id": thread.entry.thread_id,
            "snapshot_sha256": thread.snapshot_sha256,
            "decision_sha256": decision.decision_sha256,
            "requirement": decision.requirement,
            "coverage": decision.coverage.to_dict(),
            "replay": replay.to_dict(),
        }
        for thread, decision, replay in zip(corpus, decisions, replays, strict=True)
    ]


def _header(manifest: EvaluationManifest, labels: LabelSet) -> dict[str, Any]:
    return {
        "artifact_kind": EVALUATION_ARTIFACT_KIND,
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "mode": MODE,
        "calibration": CALIBRATION,
        "policy": POLICY,
        "claims": {
            "result_scope": "engineering_assessment",
            "population_accuracy": "not_claimed",
            "label_origin": labels.origin.kind.value,
            "independent_adjudication": labels.origin.independent,
            "calibration_fitted": False,
            "note": _CLAIMS_NOTE,
        },
    }


def _decision_rules() -> dict[str, str]:
    return {
        "score": SCORE_DECISION_RULE,
        "noul": NOUL_DECISION_RULE,
        "abstention": ABSTENTION_RULE,
        "language_exclusion": _LANGUAGE_EXCLUSION_RULE,
    }


def build_evaluation_document(
    *,
    manifest: EvaluationManifest,
    labels: LabelSet,
    corpus: Sequence[CorpusThread],
    integrity: PartitionIntegrity,
    decisions: Sequence[SavedDecision],
    replays: Sequence[ReplayReport],
    generative: GenerativeCaptureResult | None = None,
    question_set: QuestionSet,
    heuristic: Mapping[str, Sequence[HeuristicDecision]],
) -> dict[str, Any]:
    """Assess saved decisions; `decisions[i]` and `replays[i]` belong to `corpus[i]`."""
    _check_alignment(manifest, corpus, decisions, replays)
    items = _build_items(manifest, corpus, decisions, heuristic)
    typed = tuple(
        _typed_outcome(item, label) for item, label in _ordered_labels(labels, items, question_set)
    )
    context = _Context(question_set=question_set, items=items, typed=typed)
    partitions = {
        partition.value: _partition_block(context, partition, corpus) for partition in Partition
    }
    audit = _audit(context)
    _reconcile(partitions, audit)
    return {
        **_header(manifest, labels),
        "dataset": _dataset_block(manifest, labels, corpus),
        "identity": {**manifest.bindings.to_dict(), "language_basis": "declared"},
        "decision_rules": _decision_rules(),
        "partition_integrity": integrity.to_dict(),
        "annotations": _annotations(corpus, decisions, replays),
        "partitions": partitions,
        "audit": audit,
        "engineering_cases": _engineering_cases(context),
        "comparators": {
            "heuristic": _heuristic_comparator(context),
            "generative": _generative_comparator(context, generative),
        },
        "option_stability": _option_stability(question_set),
    }
