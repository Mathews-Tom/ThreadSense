"""Frozen evaluation dataset loading, label validation and partition integrity.

The shipped dataset under tests/fixtures/decisions/evaluation/ is engineering-authored;
these tests exercise its bindings and refusals, never model quality.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from threadsense.contracts.analysis_contract import ObjectiveType
from threadsense.decisions.evaluation_contracts import (
    CaseCategory,
    LabelOriginKind,
    Partition,
)
from threadsense.decisions.evaluation_manifest import (
    check_partition_integrity,
    load_corpus,
    load_evaluation_manifest,
    load_label_set,
    validate_labels_against_corpus,
)
from threadsense.decisions.questions import build_question_set
from threadsense.domains.loader import load_domain_vocabulary
from threadsense.errors import DecisionEvaluationError

DATASET_DIR = Path("tests/fixtures/decisions/evaluation")
MANIFEST_PATH = DATASET_DIR / "manifest.json"
ANALYSIS_FIXTURE = Path("tests/fixtures/analysis/canonical_feedback_thread.json")

Payload = dict[str, Any]


def _copy_dataset(tmp_path: Path) -> Path:
    dataset = tmp_path / "decisions" / "evaluation"
    shutil.copytree(DATASET_DIR, dataset)
    (tmp_path / "analysis").mkdir()
    shutil.copy(ANALYSIS_FIXTURE, tmp_path / "analysis" / ANALYSIS_FIXTURE.name)
    return dataset / "manifest.json"


def _read(path: Path) -> Payload:
    payload: Payload = json.loads(path.read_text(encoding="utf-8"))
    return payload


def _write(path: Path, payload: Payload) -> str:
    raw = (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _edit_manifest(manifest_path: Path, edit: Callable[[Payload], None]) -> None:
    payload = _read(manifest_path)
    edit(payload)
    _write(manifest_path, payload)


def _thread_entry(manifest: Payload, thread_id: str) -> Payload:
    entries = [entry for entry in manifest["threads"] if entry["thread_id"] == thread_id]
    assert len(entries) == 1
    entry: Payload = entries[0]
    return entry


def _edit_thread(manifest_path: Path, thread_id: str, edit: Callable[[Payload], None]) -> None:
    """Edit one bound thread file and rebind its digest in the manifest."""
    manifest = _read(manifest_path)
    entry = _thread_entry(manifest, thread_id)
    thread_path = manifest_path.parent / entry["path"]
    payload = _read(thread_path)
    edit(payload)
    entry["sha256"] = _write(thread_path, payload)
    _write(manifest_path, manifest)


def _edit_labels(manifest_path: Path, edit: Callable[[Payload], None]) -> None:
    """Edit the labels file and rebind its digest in the manifest."""
    labels_path = manifest_path.parent / "labels.json"
    payload = _read(labels_path)
    edit(payload)
    digest = _write(labels_path, payload)
    _edit_manifest(manifest_path, lambda manifest: manifest["labels"].update(sha256=digest))


def _refusal(action: Callable[[], object]) -> dict[str, Any]:
    with pytest.raises(DecisionEvaluationError) as caught:
        action()
    assert caught.value.code == "decision_evaluation_error"
    return caught.value.details


def _load_all(manifest_path: Path) -> None:
    manifest = load_evaluation_manifest(manifest_path)
    load_label_set(manifest, manifest_path.parent)
    load_corpus(manifest, manifest_path.parent)


def _validate_labels(manifest_path: Path) -> None:
    manifest = load_evaluation_manifest(manifest_path)
    labels = load_label_set(manifest, manifest_path.parent)
    corpus = load_corpus(manifest, manifest_path.parent)
    question_set = build_question_set(manifest.objective, load_domain_vocabulary(manifest.domain))
    validate_labels_against_corpus(labels, corpus, question_set)


def _integrity(manifest_path: Path) -> None:
    manifest = load_evaluation_manifest(manifest_path)
    check_partition_integrity(manifest, load_corpus(manifest, manifest_path.parent))


def test_load_evaluation_manifest_shipped_dataset_binds_threads_and_partitions() -> None:
    manifest = load_evaluation_manifest(MANIFEST_PATH)

    assert manifest.sha256 == hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest()
    assert manifest.dataset_id == "threadsense-engineering-cases-v1"
    assert manifest.objective is ObjectiveType.FRICTION_ANALYSIS
    assert manifest.domain == "developer_tools"
    assert manifest.supported_languages == ("en",)
    assert manifest.bindings.language == "en"
    assert manifest.bindings.checkpoint_revision == "7b928d828b7b0e022f929d9bd2e44165aa270148"
    partitions = {entry.thread_id: entry.partition for entry in manifest.threads}
    assert len(partitions) == 8
    assigned = list(partitions.values())
    assert assigned.count(Partition.DEVELOPMENT) == 2
    assert assigned.count(Partition.CALIBRATION) == 2
    assert assigned.count(Partition.EVALUATION) == 4
    assert partitions["reddit:analysis123"] is Partition.EVALUATION
    assert [group.group_id for group in manifest.issue_groups] == [
        "browser-comment-expansion-crash",
        "extension-large-file-crash",
    ]


def test_load_corpus_shipped_dataset_records_languages_lengths_and_digests() -> None:
    manifest = load_evaluation_manifest(MANIFEST_PATH)

    corpus = load_corpus(manifest, MANIFEST_PATH.parent)

    by_id = {item.entry.thread_id: item for item in corpus}
    assert sum(len(item.comments) for item in corpus) == 40
    canonical = by_id["reddit:analysis123"]
    assert canonical.snapshot_sha256 == hashlib.sha256(ANALYSIS_FIXTURE.read_bytes()).hexdigest()
    languages = {
        comment.comment_id: comment.language
        for comment in by_id["fixture:eng-unsupported-language"].comments
    }
    assert languages == {
        "fixture:ul-1": "es",
        "fixture:ul-2": "de",
        "fixture:ul-3": "ja",
        "fixture:ul-4": "en",
    }
    long_context = by_id["fixture:eng-long-context"]
    chars = {comment.comment_id: comment.chars for comment in long_context.comments}
    assert chars["fixture:lc-3"] == 2000
    assert chars["fixture:lc-4"] == 2001
    assert chars["fixture:lc-1"] > 400
    assert len(long_context.thread.title) > 300
    assert long_context.thread.body is not None and len(long_context.thread.body) > 600
    repetition = by_id["fixture:eng-repetition"].comments
    assert len({comment.normalized_text_sha256 for comment in repetition[:4]}) == 1
    assert len({comment.text_sha256 for comment in repetition[:4]}) == 3


def test_load_label_set_shipped_labels_disclose_engineering_origin() -> None:
    manifest = load_evaluation_manifest(MANIFEST_PATH)

    labels = load_label_set(manifest, MANIFEST_PATH.parent)

    assert labels.sha256 == manifest.labels.sha256
    assert labels.origin.kind is LabelOriginKind.ENGINEERING_AUTHORED
    assert labels.origin.independent is False
    assert "not independent human adjudication" in labels.origin.disclosure
    assert {label.case for label in labels.labels} == set(CaseCategory)
    labelled = {(label.comment_id, label.question_id) for label in labels.labels}
    assert ("fixture:lc-4", "theme") in labelled
    assert ("fixture:ul-3", "theme") in labelled


def test_validate_labels_against_corpus_shipped_labels_fit_question_set() -> None:
    _validate_labels(MANIFEST_PATH)


def test_check_partition_integrity_shipped_dataset_passes_four_checks() -> None:
    manifest = load_evaluation_manifest(MANIFEST_PATH)
    corpus = load_corpus(manifest, MANIFEST_PATH.parent)

    integrity = check_partition_integrity(manifest, corpus)

    assert [(check.name, check.groups_checked) for check in integrity.checks] == [
        ("thread_partition", 8),
        ("thread_copies", 0),
        ("comment_copies", 0),
        ("issue_groups", 2),
    ]
    assert integrity.to_dict()["status"] == "passed"


def test_load_corpus_edited_thread_bytes_raise_corpus_snapshot_mismatch(tmp_path: Path) -> None:
    manifest_path = _copy_dataset(tmp_path)
    thread_path = manifest_path.parent / "threads" / "negation.json"
    thread_path.write_bytes(thread_path.read_bytes().replace(b"five minutes", b"six minutes"))

    details = _refusal(lambda: _load_all(manifest_path))

    assert details["reason"] == "corpus_snapshot_mismatch"
    assert details["thread_id"] == "fixture:eng-negation"
    assert details["actual"] == hashlib.sha256(thread_path.read_bytes()).hexdigest()
    assert details["expected"] != details["actual"]


def test_load_label_set_edited_labels_raise_labels_digest_mismatch(tmp_path: Path) -> None:
    manifest_path = _copy_dataset(tmp_path)
    labels_path = manifest_path.parent / "labels.json"
    labels_path.write_bytes(labels_path.read_bytes() + b"\n")

    details = _refusal(lambda: _load_all(manifest_path))

    assert details["reason"] == "labels_digest_mismatch"
    assert details["path"] == "labels.json"
    assert details["actual"] == hashlib.sha256(labels_path.read_bytes()).hexdigest()


def test_load_evaluation_manifest_unknown_key_raises_manifest_invalid(tmp_path: Path) -> None:
    manifest_path = _copy_dataset(tmp_path)
    _edit_manifest(manifest_path, lambda manifest: manifest.update(notes="extra"))

    details = _refusal(lambda: load_evaluation_manifest(manifest_path))

    assert details["reason"] == "manifest_invalid"
    assert details["field"] == "manifest"
    assert details["unexpected"] == ["notes"]


def test_load_corpus_unknown_comment_language_key_raises_manifest_invalid(
    tmp_path: Path,
) -> None:
    manifest_path = _copy_dataset(tmp_path)

    def add_language(manifest: Payload) -> None:
        entry = _thread_entry(manifest, "fixture:eng-negation")
        entry["comment_languages"] = {"fixture:not-in-thread": "fr"}

    _edit_manifest(manifest_path, add_language)

    details = _refusal(lambda: _load_all(manifest_path))

    assert details["reason"] == "manifest_invalid"
    assert details["field"] == "threads[0].comment_languages"
    assert details["thread_id"] == "fixture:eng-negation"
    assert details["comment_id"] == "fixture:not-in-thread"


def test_load_evaluation_manifest_future_schema_raises_version_unsupported(
    tmp_path: Path,
) -> None:
    manifest_path = _copy_dataset(tmp_path)
    _edit_manifest(manifest_path, lambda manifest: manifest.update(schema_version=2))

    details = _refusal(lambda: load_evaluation_manifest(manifest_path))

    assert details["reason"] == "manifest_version_unsupported"
    assert details["schema_version"] == 2
    assert details["supported"] == [1]


def test_load_corpus_thread_over_comment_limit_raises_sampling_limit_exceeded(
    tmp_path: Path,
) -> None:
    manifest_path = _copy_dataset(tmp_path)
    _edit_manifest(
        manifest_path, lambda manifest: manifest["sampling"].update(max_comments_per_thread=4)
    )

    details = _refusal(lambda: _load_all(manifest_path))

    assert details["reason"] == "sampling_limit_exceeded"
    assert details["field"] == "sampling.max_comments_per_thread"
    assert details["thread_id"] == "fixture:eng-negation"
    assert (details["limit"], details["actual"]) == (4, 5)


def test_validate_labels_against_corpus_unknown_option_raises_labels_invalid(
    tmp_path: Path,
) -> None:
    manifest_path = _copy_dataset(tmp_path)
    _edit_labels(manifest_path, lambda labels: labels["labels"][1].update(expected="speed"))

    details = _refusal(lambda: _validate_labels(manifest_path))

    assert details["reason"] == "labels_invalid"
    assert details["field"] == "labels[1].expected"
    assert (details["comment_id"], details["question_id"]) == ("fixture:neg-1", "theme")
    assert details["expected"] == "speed"


def test_validate_labels_against_corpus_bool_score_raises_labels_invalid(tmp_path: Path) -> None:
    manifest_path = _copy_dataset(tmp_path)
    _edit_labels(manifest_path, lambda labels: labels["labels"][2].update(expected=True))

    details = _refusal(lambda: _validate_labels(manifest_path))

    assert details["reason"] == "labels_invalid"
    assert details["field"] == "labels[2].expected"
    assert details["question_id"] == "actionability"
    assert details["expected"] is True


def test_load_label_set_duplicate_label_raises_labels_invalid(tmp_path: Path) -> None:
    manifest_path = _copy_dataset(tmp_path)
    _edit_labels(manifest_path, lambda labels: labels["labels"].append(labels["labels"][0]))

    details = _refusal(lambda: _load_all(manifest_path))

    assert details["reason"] == "labels_invalid"
    assert details["thread_id"] == "fixture:eng-negation"
    assert (details["comment_id"], details["question_id"]) == ("fixture:neg-1", "request_intent")


def test_check_partition_integrity_comment_copy_across_partitions_raises_partition_leak(
    tmp_path: Path,
) -> None:
    manifest_path = _copy_dataset(tmp_path)
    negation = _read(manifest_path.parent / "threads" / "negation.json")
    copied_body = negation["thread"]["comments"][2]["body"]

    def copy_comment(payload: Payload) -> None:
        payload["thread"]["comments"][4]["body"] = copied_body.upper()

    _edit_thread(manifest_path, "fixture:eng-repetition", copy_comment)

    details = _refusal(lambda: _integrity(manifest_path))

    assert details["reason"] == "partition_leak"
    assert [violation["check"] for violation in details["violations"]] == ["comment_copies"]
    violation = details["violations"][0]
    assert violation["members"] == [
        {"thread_id": "fixture:eng-negation", "comment_id": "fixture:neg-3"},
        {"thread_id": "fixture:eng-repetition", "comment_id": "fixture:rep-5"},
    ]
    assert violation["partitions"] == ["development", "evaluation"]


def test_check_partition_integrity_split_issue_group_raises_partition_leak(
    tmp_path: Path,
) -> None:
    manifest_path = _copy_dataset(tmp_path)

    def move_canonical(manifest: Payload) -> None:
        _thread_entry(manifest, "reddit:analysis123")["partition"] = "calibration"

    _edit_manifest(manifest_path, move_canonical)

    details = _refusal(lambda: _integrity(manifest_path))

    assert details["reason"] == "partition_leak"
    assert details["violations"] == [
        {
            "check": "issue_groups",
            "group_id": "browser-comment-expansion-crash",
            "members": [
                {"thread_id": "fixture:eng-repetition", "comment_id": "fixture:rep-5"},
                {"thread_id": "reddit:analysis123", "comment_id": "reddit:c5"},
            ],
            "partitions": ["calibration", "evaluation"],
        }
    ]


def test_check_partition_integrity_thread_copy_under_new_id_raises_partition_leak(
    tmp_path: Path,
) -> None:
    manifest_path = _copy_dataset(tmp_path)
    copy_id = "fixture:eng-negation-copy"
    payload = _read(manifest_path.parent / "threads" / "negation.json")
    payload["thread"]["thread_id"] = copy_id
    for comment in payload["thread"]["comments"]:
        comment["thread_id"] = copy_id
    digest = _write(manifest_path.parent / "threads" / "negation_copy.json", payload)

    def add_copy(manifest: Payload) -> None:
        entry = dict(_thread_entry(manifest, "fixture:eng-negation"))
        entry.update(
            thread_id=copy_id,
            path="threads/negation_copy.json",
            sha256=digest,
            partition="evaluation",
        )
        manifest["threads"].append(entry)

    _edit_manifest(manifest_path, add_copy)

    details = _refusal(lambda: _integrity(manifest_path))

    assert details["reason"] == "partition_leak"
    checks = [violation["check"] for violation in details["violations"]]
    assert checks == ["thread_copies"] + ["comment_copies"] * 5
    thread_copy = details["violations"][0]
    assert thread_copy["basis"] == "thread_text_sha256"
    assert thread_copy["members"] == [
        {"thread_id": "fixture:eng-negation"},
        {"thread_id": copy_id},
    ]
    assert thread_copy["partitions"] == ["development", "evaluation"]
