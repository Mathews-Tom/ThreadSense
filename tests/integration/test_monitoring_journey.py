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


@pytest.fixture
def collected_journey(journey: Any) -> Any:
    """Save snapshots without draining alerts unrelated to focused refusal checks."""
    journey.profile()
    journey.watch("run", label="initial", phase="initial")
    return journey


def test_saved_watch_snapshots_freeze_before_inference_and_refuse_missing_annotations(
    collected_journey: Any,
) -> None:
    journey = collected_journey
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


def test_single_watch_copy_feedback_recovery_replay_and_cohort_transitions(
    journey: Any, journey_module: ModuleType
) -> None:
    journey.profile()
    journey.baseline()
    # Freeze an older unchanged sweep with an explicit finite operational envelope.
    # This is not an assumption that the first default-budget call must backlog.
    config = journey.work_dir / "older-target-budget.toml"
    config.write_text("[alerts]\nderivations = 1\n", encoding="utf-8")
    older = journey.watch(
        "run", "--config", str(config), label="older-target-start", phase="initial"
    )["alerts"]
    progress = journey.alert_progress()
    while older["target_id"] is None:
        assert older["status"] == "backlog", older
        older = journey.watch("alerts", "--config", str(config), label="older-target-freeze")
        after = journey.alert_progress()
        assert after != progress, older
        progress = after
    assert older["status"] == "backlog", older
    assert not older["new_alert_ids"]
    older_target = journey.frozen_target(older["target_id"])
    assert not next(
        row
        for row in journey.rows("watch_alert2_progress")
        if row["target_id"] == older["target_id"]
    )["finished"]
    journey.new_community()
    community = journey.alert_journeys[-1]
    terminal = community["terminal_result"]
    assert community["complete"]
    assert community["requested_sweep_id"] != older_target["sweep_id"]
    assert community["completed_target_ids"][0] == older_target["target_id"]
    assert community["completed_target_ids"][-1] == terminal["target_id"]
    assert [item["target_id"] for item in community["completed_reports"]] == community[
        "completed_target_ids"
    ]
    assert all(
        item[collection]["complete"]
        for item in community["completed_reports"]
        for collection in ("changes", "evidence")
    )
    assert terminal["target_id"] != older_target["target_id"]
    assert journey.frozen_target(older_target["target_id"]) == older_target
    report = json.loads(Path(terminal["json_path"]).read_text(encoding="utf-8"))
    assert report["window"]["sweep_run_id"] == community["requested_sweep_id"]
    newest_target = journey.frozen_target(terminal["target_id"])
    assert report["source_highwater"] == newest_target["highwater"]
    assert report["basis_sha256"] == newest_target["basis_sha256"]
    assert report["basis"] == json.loads(newest_target["target_json"])["basis"]
    emitted: list[str] = []
    for invocation in community["invocations"]:
        raw = json.loads(Path(invocation["payload_path"]).read_text(encoding="utf-8"))
        result = raw if raw.get("artifact_type") == "watch_alert_result" else raw["alerts"]
        assert invocation["result"] == result
        emitted.extend(result["new_alert_ids"])
        command = json.loads(Path(invocation["cli_receipt_path"]).read_text(encoding="utf-8"))
        assert command["fresh_interpreter"] is True
        assert "--cursor" not in command["argv"]
    assert emitted == community["new_alert_ids"]
    assert emitted and len(set(emitted)) == len(emitted)
    durable = {row["alert_id"]: row for row in journey.rows("alert_v2_event_identities")}
    assert set(emitted) <= set(durable)
    assert {row["alert_id"] for row in community["new_alert_provenance"]} == set(emitted)
    for collection in ("changes", "evidence"):
        reading = community[collection]
        assert reading["complete"] and reading["items"]
        assert reading["emitted_count"] == reading["expected_count"] == len(reading["items"])
        cursor: dict[str, Any] | None = None
        for entry in reading["pages"]:
            page = json.loads(Path(entry["path"]).read_text(encoding="utf-8"))
            assert page["request_cursor"] == cursor
            assert page["result"]["status"] in ("backlog", "complete")
            assert page["result"]["integrity"] == "accessed_page_leaves_only"
            cursor = page["result"]["cursor"]
        assert cursor is None
    assert len(community["invocations"]) >= 2
    journey.feedback()
    original_feedback = journey.rows("watch_feedback_keys")
    journey.publication_recovery()
    journey.issue_history()
    after_updates = journey.rows("watch_issue_updates")
    assert len(after_updates) == 4
    alert_identities = journey.rows("alert_v2_event_identities")
    assert alert_identities
    assert len(journey.rows("watch_correction_events")) == 1
    journey.replay()
    assert journey.rows("alert_v2_event_identities") == alert_identities
    replay = journey.alert_journeys[-2]
    restarted = journey.alert_journeys[-1]
    assert not replay["new_alert_ids"] and not restarted["new_alert_ids"]
    assert replay["alert_ids"] == restarted["alert_ids"]
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
    targets = [json.loads(row["target_json"]) for row in journey.rows("watch_alert2_targets")]
    assert len({target["cohort_id"] for target in targets}) == 2
    assert {target["basis"]["profile_revision"] for target in targets} == {1, 2}
    assert all(
        len(
            {
                target["cohort_id"]
                for target in targets
                if target["basis"]["profile_revision"] == revision
            }
        )
        == 1
        for revision in (1, 2)
    )
    assert journey.observations["semantic_activation_performed"] is False
    assert not any(item["step"] == "annotations" for item in journey.evidence)

    # A real indivisible-unit refusal remains visible; the journey must not raise
    # the operator's envelope or silently retry it as an empty success.
    refused_config = journey.work_dir / "refused-alert-budget.toml"
    refused_config.write_text("[alerts]\ninput_bytes = 1\n", encoding="utf-8")
    history = journey.stable_history()
    refused = journey.watch(
        "alerts", "--config", str(refused_config), label="journey-resource-refused"
    )
    assert refused["status"] == "resource_refused"
    assert refused["resource"]["required"] > refused["resource"]["configured"]
    calls = journey.calls
    with pytest.raises(journey_module.SmokeFailure, match="resource_refused") as failure:
        journey.complete_alerts(refused, label="expected-resource-refusal")
    assert journey.calls == calls
    assert all(
        str(refused["resource"][field]) in str(failure.value)
        for field in ("unit", "reason", "required", "configured")
    )
    assert journey.stable_history() == history


def test_actual_policy_missing_evidence_and_invalid_annotation_endpoint_are_local_refusals(
    collected_journey: Any,
) -> None:
    journey = collected_journey
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


def test_unknown_feedback_target_refuses_without_writing_history(
    collected_journey: Any,
) -> None:
    journey = collected_journey
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
