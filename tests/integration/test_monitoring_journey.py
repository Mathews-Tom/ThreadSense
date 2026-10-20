"""Model-free CLI transitions for the integrated journey, not real-inference acceptance.

The separately executed --scenario all acceptance is required to prove the checkpoint.
These tests never create an inference answer or contact a model endpoint.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from threadsense.decisions.evaluate import _bind_annotations
from threadsense.decisions.evaluation_manifest import (
    load_corpus,
    load_evaluation_manifest,
    load_label_set,
)
from threadsense.errors import DecisionEvaluationError


@pytest.fixture
def journey_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    scripts = Path(__file__).resolve().parents[2] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    return importlib.import_module("smoke_journey")


@pytest.fixture
def journey(journey_module: ModuleType, tmp_path: Path) -> Any:
    return journey_module.JourneyScenario(tmp_path, journey_module.ENDPOINT)


def test_saved_watch_snapshots_freeze_before_inference_and_refuse_missing_annotations(
    journey: Any,
) -> None:
    journey.profile()
    journey.baseline()
    journey.freeze()
    manifest = load_evaluation_manifest(journey.manifest)
    corpus = load_corpus(manifest, journey.manifest.parent)
    labels = load_label_set(manifest, journey.manifest.parent)
    assert len(corpus) == 3
    assert {entry.partition.value for entry in manifest.threads} == {
        "development",
        "calibration",
        "evaluation",
    }
    assert labels.origin.kind.value == "engineering_authored"
    revisions = journey.rows("watch_thread_revisions")
    saved_hashes = {row["snapshot_sha256"] for row in revisions}
    assert {entry.file.sha256 for entry in manifest.threads} <= saved_hashes
    assert len(journey.rows("watches")) == 1

    with pytest.raises(DecisionEvaluationError) as missing:
        _bind_annotations((), manifest, corpus)
    assert missing.value.details["reason"] == "annotation_missing"
    assert len(missing.value.details["thread_ids"]) == 3
    assert not list(journey.store_root.rglob("evaluations/*/*.json"))

    original_manifest = journey.manifest.read_bytes()
    stale = json.loads(original_manifest)
    stale["bindings"]["checkpoint_revision"] = "0" * 40
    stale_path = journey.manifest.parent / "stale-manifest.json"
    stale_path.write_text(json.dumps(stale), encoding="utf-8")
    code, refused = journey.cli(
        "decisions",
        "evaluate",
        "--manifest",
        str(stale_path),
        "--annotation",
        str(journey.work_dir / "missing-annotation.json"),
        label="refuse-stale-identity",
    )
    assert code == 1
    assert refused["error"]["details"]["reason"] == "stale_binding"
    assert refused["error"]["details"]["field"] == "checkpoint_revision"
    assert journey.manifest.read_bytes() == original_manifest

    # Tampering with a frozen label is rejected by the real manifest consumer.
    label_path = journey.manifest.parent / manifest.labels.path
    document = json.loads(label_path.read_text(encoding="utf-8"))
    document["labels"][0]["expected"] = "performance"
    label_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(DecisionEvaluationError) as error:
        load_label_set(manifest, journey.manifest.parent)
    assert error.value.details["reason"] == "labels_digest_mismatch"


def test_single_watch_copy_feedback_recovery_replay_and_cohort_transitions(journey: Any) -> None:
    journey.profile()
    journey.baseline()
    journey.new_community()
    journey.feedback()
    original_feedback = journey.rows("watch_feedback_keys")
    journey.publication_recovery()
    journey.issue_history()
    after_updates = journey.rows("watch_issue_updates")
    assert len(after_updates) == 4
    assert len(journey.rows("watch_correction_events")) == 1
    journey.replay()
    assert journey.rows("watch_feedback_keys") == original_feedback
    assert journey.rows("watch_issue_updates") == after_updates
    recovered = [row for row in journey.rows("watch_runs") if row["status"] == "abandoned"]
    assert len(recovered) == 1
    assert recovered[0]["recovered_by_run_id"]
    assert all(row["status"] == "written" for row in journey.rows("watch_digests"))
    journey.edit_observation()
    edited = [row for row in journey.rows("watch_comment_texts") if row["change"] == "edited"]
    assert any(row["comment_id"] == "reddit:ia2" for row in edited)
    journey.coverage()
    assert any(row["outcome"] == "partial" for row in journey.rows("watch_sweeps"))
    journey.cohort()
    journey.scheduler()
    assert len(journey.rows("watches")) == 1
    assert len(journey.rows("watch_revisions")) == 2
    assert len(journey.rows("watch_alert_cohorts")) == 2
    assert journey.observations["semantic_activation_performed"] is False
    assert not any(item["step"] == "annotations" for item in journey.evidence)


def test_actual_policy_missing_evidence_and_invalid_annotation_endpoint_are_local_refusals(
    journey: Any,
) -> None:
    journey.profile()
    journey.baseline()
    revision = journey.rows("watch_thread_revisions")[0]
    snapshot = journey.monitoring / revision["snapshot_path"]
    code, annotation = journey.cli(
        "decisions",
        "annotate",
        "--input",
        str(snapshot),
        "--endpoint",
        "https://example.invalid",
        "--requirement",
        "required",
        "--output-dir",
        str(journey.work_dir / "refused-annotations"),
        "--domain",
        "developer_tools",
        "--objective",
        "friction_analysis",
        label="invalid-endpoint-no-request",
    )
    assert code == 1
    assert annotation["error"]["details"]["reason"] == "endpoint_invalid"
    assert not list((journey.work_dir / "refused-annotations").rglob("*.json"))
    missing = journey.work_dir / "missing" / ("0" * 64 + ".json")
    code, policy = journey.cli(
        "decisions",
        "policy",
        "check",
        "--evaluation",
        str(missing),
        label="missing-evidence-no-request",
    )
    assert code == 0
    assert policy["eligibility"] == "not_eligible"
    assert policy["activation_permitted"] is False
    assert "evidence_missing" in policy["reasons"]
    result = json.loads(Path(policy["policy_check_path"]).read_text(encoding="utf-8"))
    assert result["activation"]["performed"] is False
    assert result["evidence"] is None
    assert len(journey.rows("watches")) == 1


def test_unknown_feedback_target_refuses_without_writing_history(journey: Any) -> None:
    journey.profile()
    journey.baseline()
    before = journey.rows("watch_feedback_keys")
    code, payload = journey.cli(
        "feedback",
        "record",
        "--watch",
        "export-issues",
        "--kind",
        "preference",
        "--target-kind",
        "finding",
        "--target",
        "ent-" + "0" * 32,
        "--value",
        "irrelevant",
        "--key",
        "unknown",
        label="unknown-feedback",
    )
    assert code == 1
    assert payload["error"]["details"]["reason"] == "unknown_target"
    assert journey.rows("watch_feedback_keys") == before == []
    assert journey.rows("watch_preference_events") == []
    assert journey.rows("watch_correction_events") == []
