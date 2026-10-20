"""Offline assessment of frozen typed decision artifacts against a frozen dataset manifest.

Evaluation reads authored inputs and saved decision documents only. It never contacts the
typed-decision service and never re-annotates anything. The one network observation is a
loopback model listing that records generative comparator availability in the run record,
which is execution metadata outside the hashed evaluation document.
"""

from __future__ import annotations

import hashlib
import re
import secrets
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from threadsense.config import RuntimeConfig
from threadsense.decisions.artifact import (
    check_output_location,
    decision_bytes,
    publish_immutable,
    refuse_alias,
    require_run_id,
)
from threadsense.decisions.assessment import build_evaluation_document, evaluation_bytes
from threadsense.decisions.comparators import (
    heuristic_theme_decisions,
    observe_generative_availability,
)
from threadsense.decisions.contracts import (
    PINNED_CONTEXT_POLICY,
    PINNED_INFERENCE_SETTINGS,
    ChoiceQuestion,
    QuestionSet,
    RuntimeManifest,
)
from threadsense.decisions.evaluation_contracts import (
    EVALUATION_RUN_ARTIFACT_KIND,
    EVALUATION_SCHEMA_VERSION,
    CorpusThread,
    EvaluationManifest,
    HeuristicDecision,
    IdentityBindings,
    Partition,
    SavedDecision,
)
from threadsense.decisions.evaluation_manifest import (
    check_partition_integrity,
    load_corpus,
    load_evaluation_manifest,
    load_label_set,
    validate_labels_against_corpus,
)
from threadsense.decisions.questions import build_question_set
from threadsense.decisions.saved import load_saved_decision, replay_saved_decision
from threadsense.domains.loader import load_domain_vocabulary
from threadsense.errors import DecisionArtifactError, DecisionEvaluationError

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_EVALUATIONS_DIRNAME = "evaluations"
_HEURISTIC_QUESTION_ID = "theme"
_OBSERVATION_SCOPE = "evaluator wall time only; no model-server memory is observed by the evaluator"


@dataclass(frozen=True)
class EvaluationJob:
    manifest_path: Path
    annotation_paths: tuple[Path, ...]
    output_root: Path
    protected_roots: tuple[Path, ...]


@dataclass(frozen=True)
class EvaluationOutcome:
    evaluation_path: Path
    run_path: Path
    evaluation_sha256: str
    run_id: str
    manifest_sha256: str
    label_origin: str
    independent_adjudication: bool
    # Partition name -> the evaluation document's population counts for that partition.
    partitions: dict[str, dict[str, Any]]
    generative_status: str
    generative_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "evaluation_path": str(self.evaluation_path),
            "run_path": str(self.run_path),
            "evaluation_sha256": self.evaluation_sha256,
            "run_id": self.run_id,
            "manifest_sha256": self.manifest_sha256,
            "label_origin": self.label_origin,
            "independent_adjudication": self.independent_adjudication,
            "partitions": self.partitions,
            "generative_comparator": {
                "status": self.generative_status,
                "reason": self.generative_reason,
            },
        }


@dataclass(frozen=True)
class EvaluationPaths:
    evaluation_path: Path
    run_path: Path


def evaluation_paths(
    output_root: Path, manifest_sha256: str, evaluation_sha256: str, run_id: str
) -> EvaluationPaths:
    named_hashes = (("manifest_sha256", manifest_sha256), ("evaluation_sha256", evaluation_sha256))
    for label, value in named_hashes:
        if _SHA256_PATTERN.fullmatch(value) is None:
            raise DecisionArtifactError(
                f"{label} must be 64 lowercase hex characters",
                details={"reason": "artifact_name_invalid", "field": label},
            )
    require_run_id(run_id)
    evaluations_root = output_root / _EVALUATIONS_DIRNAME
    manifest_root = evaluations_root / manifest_sha256
    # Only generated descendants of the canonicalized output root must be real directories,
    # so existing links cannot redirect publication.
    for directory in (evaluations_root, manifest_root, manifest_root / "runs"):
        if directory.is_symlink():
            raise DecisionArtifactError(
                "evaluation artifact directories must not be symbolic links",
                details={"reason": "artifact_directory_symlink", "path": str(directory)},
            )
    return EvaluationPaths(
        evaluation_path=manifest_root / f"{evaluation_sha256}.json",
        run_path=manifest_root / "runs" / f"{run_id}.json",
    )


def _current_bindings(
    runtime_manifest: RuntimeManifest, question_set: QuestionSet
) -> IdentityBindings:
    return IdentityBindings(
        runtime_manifest_sha256=runtime_manifest.sha256(),
        checkpoint_revision=runtime_manifest.checkpoint.revision,
        question_set_sha256=question_set.sha256(),
        settings_sha256=PINNED_INFERENCE_SETTINGS.sha256(),
        context_policy_sha256=PINNED_CONTEXT_POLICY.sha256(),
        language=PINNED_INFERENCE_SETTINGS.lang,
    )


def _first_difference(
    expected: IdentityBindings, observed: IdentityBindings
) -> tuple[str, str, str] | None:
    observed_fields = observed.to_dict()
    for field, value in expected.to_dict().items():
        if observed_fields[field] != value:
            return field, value, observed_fields[field]
    return None


def _require_fresh_bindings(manifest: EvaluationManifest, current: IdentityBindings) -> None:
    difference = _first_difference(manifest.bindings, current)
    if difference is not None:
        field, bound, now = difference
        raise DecisionEvaluationError(
            f"manifest binding {field} differs from the current pinned identity",
            details={"reason": "stale_binding", "field": field, "bound": bound, "current": now},
        )


def _require_decision_identity(saved: SavedDecision, manifest: EvaluationManifest) -> None:
    difference = _first_difference(manifest.bindings, saved.bindings())
    if difference is not None:
        field, bound, observed = difference
        raise DecisionEvaluationError(
            f"annotation {field} differs from the manifest binding",
            details={
                "reason": "annotation_identity_mismatch",
                "decision_sha256": saved.decision_sha256,
                "field": field,
                "bound": bound,
                "observed": observed,
            },
        )
    for field, bound, observed in (
        ("objective", manifest.objective.value, saved.objective),
        ("domain", manifest.domain, saved.domain),
    ):
        if bound != observed:
            raise DecisionEvaluationError(
                f"annotation {field} differs from the manifest",
                details={
                    "reason": "annotation_identity_mismatch",
                    "decision_sha256": saved.decision_sha256,
                    "field": field,
                    "bound": bound,
                    "observed": observed,
                },
            )
    _require_checkpoint_verified(saved)


def _require_checkpoint_verified(saved: SavedDecision) -> None:
    """Refuse an annotation whose ok answers do not rest on the pinned checkpoint files."""
    if saved.coverage.ok > 0 and not saved.checkpoint_verified:
        raise DecisionEvaluationError(
            "annotation answers were not produced on the verified pinned checkpoint",
            details={
                "reason": "annotation_identity_mismatch",
                "decision_sha256": saved.decision_sha256,
                "field": "checkpoint_verification",
                "ok_items": saved.coverage.ok,
            },
        )


def _require_corpus_match(saved: SavedDecision, thread: CorpusThread) -> None:
    saved_comments = [(item.comment_id, item.comment_text_sha256) for item in saved.items]
    corpus_comments = [(comment.comment_id, comment.text_sha256) for comment in thread.comments]
    if saved.thread_id != thread.entry.thread_id or saved_comments != corpus_comments:
        raise DecisionEvaluationError(
            "annotation items differ from the frozen corpus thread",
            details={
                "reason": "annotation_corpus_mismatch",
                "decision_sha256": saved.decision_sha256,
                "thread_id": thread.entry.thread_id,
            },
        )


def _index_annotations(
    annotation_paths: Sequence[Path], corpus: Sequence[CorpusThread]
) -> dict[str, SavedDecision]:
    known = {thread.snapshot_sha256 for thread in corpus}
    by_snapshot: dict[str, SavedDecision] = {}
    for path in annotation_paths:
        saved = load_saved_decision(path)
        if saved.snapshot_sha256 not in known:
            raise DecisionEvaluationError(
                "annotation snapshot is not listed in the manifest",
                details={
                    "reason": "annotation_unbound",
                    "path": str(path),
                    "snapshot_sha256": saved.snapshot_sha256,
                },
            )
        if saved.snapshot_sha256 in by_snapshot:
            raise DecisionEvaluationError(
                "more than one annotation was supplied for one snapshot",
                details={
                    "reason": "annotation_duplicate",
                    "path": str(path),
                    "snapshot_sha256": saved.snapshot_sha256,
                },
            )
        by_snapshot[saved.snapshot_sha256] = saved
    return by_snapshot


def _bind_annotations(
    annotation_paths: Sequence[Path],
    manifest: EvaluationManifest,
    corpus: Sequence[CorpusThread],
) -> tuple[SavedDecision, ...]:
    """Return exactly one verified saved decision per corpus thread, in corpus order."""
    by_snapshot = _index_annotations(annotation_paths, corpus)
    missing = [thread for thread in corpus if thread.snapshot_sha256 not in by_snapshot]
    if missing:
        raise DecisionEvaluationError(
            "a manifest thread has no annotation",
            details={
                "reason": "annotation_missing",
                "thread_ids": [thread.entry.thread_id for thread in missing],
                "snapshot_sha256": [thread.snapshot_sha256 for thread in missing],
            },
        )
    for thread in corpus:
        saved = by_snapshot[thread.snapshot_sha256]
        _require_decision_identity(saved, manifest)
        _require_corpus_match(saved, thread)
    return tuple(by_snapshot[thread.snapshot_sha256] for thread in corpus)


def _heuristic_question(question_set: QuestionSet) -> ChoiceQuestion:
    question = question_set.by_id().get(_HEURISTIC_QUESTION_ID)
    if not isinstance(question, ChoiceQuestion):
        raise DecisionEvaluationError(
            "the question set has no theme choice question for the heuristic comparator",
            details={
                "reason": "heuristic_question_unavailable",
                "question_id": _HEURISTIC_QUESTION_ID,
            },
        )
    return question


def _input_paths(job: EvaluationJob, manifest: EvaluationManifest) -> tuple[Path, ...]:
    base = job.manifest_path.parent
    return (
        job.manifest_path,
        base / manifest.labels.path,
        *(base / thread.file.path for thread in manifest.threads),
        *job.annotation_paths,
    )


def _default_run_id(started_at_utc: float) -> str:
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(started_at_utc))
    return f"{stamp}-{secrets.token_hex(6)}"


def _run_record(
    *,
    run_id: str,
    evaluation_sha256: str,
    evaluation_relpath: str,
    manifest_sha256: str,
    started_at_utc: float,
    completed_at_utc: float,
    availability: dict[str, Any],
) -> dict[str, Any]:
    return {
        "artifact_kind": EVALUATION_RUN_ARTIFACT_KIND,
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "run_id": run_id,
        "evaluation_sha256": evaluation_sha256,
        "evaluation_path": evaluation_relpath,
        "manifest_sha256": manifest_sha256,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "wall_seconds": completed_at_utc - started_at_utc,
        "generative_comparator": availability,
        "observation_scope": _OBSERVATION_SCOPE,
    }


def _assess(
    job: EvaluationJob, runtime_manifest: RuntimeManifest
) -> tuple[EvaluationManifest, dict[str, Any], str, bool]:
    """Load, verify and assess every input; return the manifest, document and label origin."""
    manifest = load_evaluation_manifest(job.manifest_path)
    labels = load_label_set(manifest, job.manifest_path.parent)
    corpus = load_corpus(manifest, job.manifest_path.parent)
    vocabulary = load_domain_vocabulary(manifest.domain)
    question_set = build_question_set(manifest.objective, vocabulary)
    _require_fresh_bindings(manifest, _current_bindings(runtime_manifest, question_set))
    validate_labels_against_corpus(labels, corpus, question_set)
    integrity = check_partition_integrity(manifest, corpus)
    decisions = _bind_annotations(job.annotation_paths, manifest, corpus)
    replays = tuple(
        replay_saved_decision(saved, question_set, PINNED_INFERENCE_SETTINGS) for saved in decisions
    )
    theme = _heuristic_question(question_set)
    heuristic: dict[str, tuple[HeuristicDecision, ...]] = {
        thread.entry.thread_id: heuristic_theme_decisions(thread.thread, vocabulary, theme)
        for thread in corpus
    }
    document = build_evaluation_document(
        manifest=manifest,
        labels=labels,
        corpus=corpus,
        integrity=integrity,
        decisions=decisions,
        replays=replays,
        question_set=question_set,
        heuristic=heuristic,
    )
    return manifest, document, labels.origin.kind.value, labels.origin.independent


def evaluate_frozen_assessment(
    job: EvaluationJob,
    *,
    runtime_manifest: RuntimeManifest,
    runtime_config: RuntimeConfig,
    comparator_transport: httpx.BaseTransport | None = None,
    wall_clock: Callable[[], float] = time.time,
    run_id: str | None = None,
) -> EvaluationOutcome:
    if run_id is not None:
        require_run_id(run_id)
    output_root = check_output_location(job.output_root, job.manifest_path, job.protected_roots)
    started_at_utc = wall_clock()
    manifest, document, label_origin, independent = _assess(job, runtime_manifest)
    content = evaluation_bytes(document)
    evaluation_sha256 = hashlib.sha256(content).hexdigest()
    resolved_run_id = run_id if run_id is not None else _default_run_id(started_at_utc)
    paths = evaluation_paths(output_root, manifest.sha256, evaluation_sha256, resolved_run_id)
    for input_path in _input_paths(job, manifest):
        refuse_alias(paths.evaluation_path, input_path)
        refuse_alias(paths.run_path, input_path)
    publish_immutable(paths.evaluation_path, content, reuse_identical=True)
    availability = observe_generative_availability(runtime_config, transport=comparator_transport)
    completed_at_utc = wall_clock()
    record = _run_record(
        run_id=resolved_run_id,
        evaluation_sha256=evaluation_sha256,
        evaluation_relpath=paths.evaluation_path.relative_to(output_root).as_posix(),
        manifest_sha256=manifest.sha256,
        started_at_utc=started_at_utc,
        completed_at_utc=completed_at_utc,
        availability=availability,
    )
    publish_immutable(paths.run_path, decision_bytes(record), reuse_identical=False)
    return EvaluationOutcome(
        evaluation_path=paths.evaluation_path,
        run_path=paths.run_path,
        evaluation_sha256=evaluation_sha256,
        run_id=resolved_run_id,
        manifest_sha256=manifest.sha256,
        label_origin=label_origin,
        independent_adjudication=independent,
        partitions={
            partition.value: document["partitions"][partition.value]["population"]
            for partition in Partition
        },
        generative_status=availability["status"],
        generative_reason=availability["reason"],
    )
