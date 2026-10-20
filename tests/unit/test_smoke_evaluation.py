"""Pure helpers and registration of the evaluation smoke scenario."""

from __future__ import annotations

import importlib.util
import json
import math
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
BASE_BINDINGS = {
    "runtime_manifest_sha256": "a" * 64,
    "checkpoint_revision": "b" * 40,
    "question_set_sha256": "c" * 64,
    "settings_sha256": "d" * 64,
    "context_policy_sha256": "e" * 64,
    "language": "en",
    "dataset_manifest_sha256": "f" * 64,
    "labels_sha256": "1" * 64,
}


@pytest.fixture
def smoke_modules(monkeypatch: pytest.MonkeyPatch) -> dict[str, ModuleType]:
    modules: dict[str, ModuleType] = {}
    for name in (
        "smoke_annotations",
        "smoke_evaluation",
        "smoke_issues",
        "smoke_feedback",
        "smoke_monitoring",
    ):
        spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        modules[name] = module
    return modules


def held_out_metrics(
    error_rate: float, decision_rate: float, labeled_decided: int
) -> dict[str, Any]:
    return {
        "decision_rate": decision_rate,
        "labels": {"error_rate": error_rate, "labeled_decided": labeled_decided},
    }


def leaf_values(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        leaves: dict[str, Any] = {}
        for key, item in value.items():
            leaves.update(leaf_values(item, f"{prefix}{key}."))
        return leaves
    return {prefix.rstrip("."): value}


def changed_fields(base: dict[str, Any], edited: dict[str, Any]) -> list[str]:
    before, after = leaf_values(base), leaf_values(edited)
    assert sorted(before) == sorted(after)
    return [key for key in before if before[key] != after[key]]


def items_scenario(evaluation: ModuleType, tmp_path: Path) -> Any:
    """An evaluation scenario holding one staged thread with short, at-budget and long bodies."""
    scenario = evaluation.EvaluationScenario(tmp_path, "http://127.0.0.1:8123")
    budget = evaluation.CLIENT_COMMENT_BUDGET
    scenario.comments = {
        "thread": {"short": "a", "boundary": "b" * budget, "long": "c" * (budget + 1)}
    }
    scenario.boundary = [["thread", "boundary"]]
    return scenario


def decision_items(statuses: dict[str, tuple[str, list[str]]]) -> dict[str, Any]:
    items = [
        {"comment_id": comment_id, "status": status, "reasons": reasons}
        for comment_id, (status, reasons) in statuses.items()
    ]
    refused = sum(1 for item in items if item["status"] == "refused")
    return {
        "items": items,
        "coverage": {
            "total": len(items),
            "ok": len(items) - refused,
            "refused": refused,
            "unavailable": 0,
        },
    }


def test_check_real_items_boundary_refused_with_server_reasons_returns_refusal(
    smoke_modules: dict[str, ModuleType], tmp_path: Path
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]
    scenario = items_scenario(evaluation, tmp_path)
    server_reasons = ["server_dropped_state_tokens", "server_reported_truncation"]
    decision = decision_items(
        {
            "short": ("ok", []),
            "boundary": ("refused", server_reasons),
            "long": ("refused", [evaluation.OVERSIZED_REASON]),
        }
    )

    # Act
    refused = scenario.check_real_items("thread", decision)

    # Assert
    assert refused == {"boundary": server_reasons, "long": [evaluation.OVERSIZED_REASON]}


def test_check_real_items_below_budget_refused_with_server_reasons_raises_smoke_failure(
    smoke_modules: dict[str, ModuleType], tmp_path: Path
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]
    scenario = items_scenario(evaluation, tmp_path)
    decision = decision_items(
        {
            "short": ("refused", ["server_reported_truncation"]),
            "boundary": ("ok", []),
            "long": ("refused", [evaluation.OVERSIZED_REASON]),
        }
    )

    # Act / Assert
    with pytest.raises(
        smoke_modules["smoke_annotations"].SmokeFailure, match="^short: within-budget"
    ):
        scenario.check_real_items("thread", decision)


@pytest.mark.parametrize("reasons", [["server_unknown_reason"], []])
def test_check_real_items_boundary_refused_without_server_reasons_raises_smoke_failure(
    smoke_modules: dict[str, ModuleType], tmp_path: Path, reasons: list[str]
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]
    scenario = items_scenario(evaluation, tmp_path)
    decision = decision_items(
        {
            "short": ("ok", []),
            "boundary": ("refused", reasons),
            "long": ("refused", [evaluation.OVERSIZED_REASON]),
        }
    )

    # Act / Assert
    with pytest.raises(
        smoke_modules["smoke_annotations"].SmokeFailure, match="^boundary: at-budget"
    ):
        scenario.check_real_items("thread", decision)


def test_check_real_items_oversized_refused_with_server_reason_raises_smoke_failure(
    smoke_modules: dict[str, ModuleType], tmp_path: Path
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]
    scenario = items_scenario(evaluation, tmp_path)
    decision = decision_items(
        {
            "short": ("ok", []),
            "boundary": ("ok", []),
            "long": ("refused", ["server_reported_truncation"]),
        }
    )

    # Act / Assert
    with pytest.raises(smoke_modules["smoke_annotations"].SmokeFailure, match="^long: oversized"):
        scenario.check_real_items("thread", decision)


def test_check_real_items_all_ok_returns_empty_refusals(
    smoke_modules: dict[str, ModuleType], tmp_path: Path
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]
    scenario = items_scenario(evaluation, tmp_path)
    budget = evaluation.CLIENT_COMMENT_BUDGET
    scenario.comments = {"thread": {"short": "a", "boundary": "b" * budget}}
    decision = decision_items({"short": ("ok", []), "boundary": ("ok", [])})

    # Act
    refused = scenario.check_real_items("thread", decision)

    # Assert
    assert refused == {}


def test_server_refusal_reasons_cover_every_usage_coverage_reason(
    smoke_modules: dict[str, ModuleType],
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]

    # Act
    reasons = evaluation.SERVER_REFUSAL_REASONS

    # Assert
    assert reasons == {
        "server_dropped_state_tokens",
        "server_reported_truncation",
        "server_truncated_questions",
        "server_reported_option_usage",
    }


def test_constructed_labels_same_values_replace_origin_with_constructed_fixture(
    smoke_modules: dict[str, ModuleType],
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]
    labels = {
        "artifact_kind": "typed_decision_evaluation_labels",
        "schema_version": 1,
        "origin": {"kind": "engineering_authored"},
        "labels": [{"thread_id": "t", "comment_id": "c", "question_id": "theme"}],
    }

    # Act
    constructed = evaluation.constructed_labels(labels)

    # Assert
    assert constructed["labels"] == labels["labels"]
    assert constructed["origin"]["kind"] == "constructed_test_fixture"
    assert "constructed test fixture" in constructed["origin"]["disclosure"].lower()
    assert labels["origin"] == {"kind": "engineering_authored"}


def test_activation_policy_test_only_uses_predeclared_fixture_tolerances(
    smoke_modules: dict[str, ModuleType],
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]

    # Act
    policy = evaluation.activation_policy(
        "test_only", "theme", BASE_BINDINGS, evaluation.FIXTURE_TOLERANCES
    )

    # Assert
    assert policy["scope"] == "test_only"
    assert policy["tolerances"] == {
        "max_error_rate": 1.0,
        "min_decision_rate": 0.0,
        "min_labeled_decided": 1,
    }
    assert policy["bindings"] == BASE_BINDINGS
    assert "test" in policy["disclosure"].lower()


def test_invalidation_cases_policy_edits_change_exactly_one_field(
    smoke_modules: dict[str, ModuleType],
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]
    base = evaluation.activation_policy(
        "test_only", "theme", BASE_BINDINGS, evaluation.FIXTURE_TOLERANCES
    )

    # Act
    cases = evaluation.invalidation_cases(base, base, held_out_metrics(0.25, 0.5, 4))

    # Assert
    edited = [case for case in cases if case.evidence == "constructed"]
    assert edited
    for case in edited:
        assert changed_fields(base, case.policy) == [case.target], case.name
        assert case.expected is not None and len(case.expected) == 1, case.name


def test_invalidation_cases_satisfied_tolerances_are_not_applicable_with_fixed_values(
    smoke_modules: dict[str, ModuleType],
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]
    base = evaluation.activation_policy(
        "test_only", "theme", BASE_BINDINGS, evaluation.FIXTURE_TOLERANCES
    )

    # Act
    cases = {
        case.name: case
        for case in evaluation.invalidation_cases(base, base, held_out_metrics(0.0, 1.0, 3))
    }

    # Assert
    assert cases["max_error_rate_zero"].expected is None
    assert cases["max_error_rate_zero"].observed_value == 0.0
    assert cases["max_error_rate_zero"].policy["tolerances"]["max_error_rate"] == 0.0
    assert cases["min_decision_rate_one"].expected is None
    assert cases["min_decision_rate_one"].policy["tolerances"]["min_decision_rate"] == 1.0
    unreachable = cases["min_labeled_decided_unreachable"]
    assert unreachable.expected == ("insufficient_labeled_items",)
    assert unreachable.policy["tolerances"]["min_labeled_decided"] == 1_000_000
    assert math.isnan(cases["nonfinite_max_error_rate"].policy["tolerances"]["max_error_rate"])


def test_write_json_file_nonfinite_tolerance_writes_literal_token(
    smoke_modules: dict[str, ModuleType], tmp_path: Path
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]

    # Act
    path = evaluation.write_json_file(tmp_path / "policy.json", {"max_error_rate": math.nan})

    # Assert
    assert '"max_error_rate": NaN' in path.read_text(encoding="utf-8")


def test_relocate_sibling_directory_rebases_relative_path(
    smoke_modules: dict[str, ModuleType], tmp_path: Path
) -> None:
    # Arrange
    evaluation = smoke_modules["smoke_evaluation"]
    source = tmp_path / "decisions" / "evaluation"
    target = tmp_path / "decisions" / "constructed"

    # Act
    thread = evaluation.relocate("threads/negation.json", source, target)
    shared = evaluation.relocate("../../analysis/thread.json", source, target)

    # Assert
    assert thread == "../evaluation/threads/negation.json"
    assert shared == "../../analysis/thread.json"


def test_main_evaluation_without_endpoint_exits_with_usage_error(
    smoke_modules: dict[str, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Arrange
    monitoring = smoke_modules["smoke_monitoring"]
    argv = ["smoke_monitoring.py", "--scenario", "evaluation", "--work-dir", str(tmp_path / "w")]
    monkeypatch.setattr(sys, "argv", argv)

    # Act
    with pytest.raises(SystemExit) as caught:
        monitoring.main()

    # Assert
    assert caught.value.code == 2
    assert not (tmp_path / "w").exists()


def test_main_evaluation_failure_writes_redacted_failed_evidence_with_observations(
    smoke_modules: dict[str, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Arrange
    annotations = smoke_modules["smoke_annotations"]
    evaluation = smoke_modules["smoke_evaluation"]
    monitoring = smoke_modules["smoke_monitoring"]
    private_path = "/tmp/private-smoke-workspace/evaluation.json"

    def fail_run(scenario: Any) -> None:
        raise annotations.SmokeFailure(f"policy check failed reading {private_path}")

    monkeypatch.setattr(evaluation.EvaluationScenario, "run", fail_run)
    work_dir = tmp_path / "smoke"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "smoke_monitoring.py",
            *("--scenario", "evaluation", "--work-dir", str(work_dir)),
            *("--endpoint", "http://127.0.0.1:8123"),
        ],
    )

    # Act
    exit_code = monitoring.main()

    # Assert
    assert exit_code == 1
    evidence_text = (work_dir / "evaluation-evidence.json").read_text(encoding="utf-8")
    evidence = json.loads(evidence_text)
    assert evidence["status"] == "failed"
    assert evidence["steps"] == []
    assert evidence["observations"]["fixture_tolerances"] == evaluation.FIXTURE_TOLERANCES
    assert "<local-path>" in evidence["error"]
    assert private_path not in evidence_text
    output = capsys.readouterr().out
    assert "scenario=evaluation status=failed" in output
    assert private_path not in output


@pytest.mark.parametrize("capture_mode", ["capture", "no_capture", "legacy"])
def test_real_evaluation_replays_only_frozen_evidence(
    smoke_modules: dict[str, ModuleType],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capture_mode: str,
) -> None:
    scenario, payload, run, document, path = evaluation_consumer(
        smoke_modules["smoke_evaluation"], tmp_path, monkeypatch, capture_mode
    )
    calls: list[tuple[str, ...]] = []

    def configured(*args: str) -> tuple[int, dict[str, Any]]:
        calls.append(args)
        if len(calls) == 1:
            assert "--generative-capture" not in args
            return 0, payload
        # An available runtime must never become a fresh acquisition on replay.
        assert capture_mode == "capture"
        assert args[args.index("--generative-capture") + 1] == str(
            scenario.engineering_capture_path
        )
        return 0, {**payload, "run_id": "second"}

    def disabled(*args: str) -> tuple[int, dict[str, Any]]:
        calls.append(args)
        assert capture_mode != "capture"
        assert "--generative-capture" not in args
        return 0, {**payload, "run_id": "second"}

    monkeypatch.setattr(scenario, "cli_with_configured_runtime", configured)
    monkeypatch.setattr(scenario, "cli", disabled)
    scenario.real_evaluation()
    assert (
        scenario.observations["generative_comparator_availability"] == run["generative_comparator"]
    )
    assert scenario.engineering_document is document
    assert scenario.engineering_path == path
    scenario.evaluation_replay()
    assert len(calls) == 2
    replay = scenario.evidence[-1]["observed"]
    assert replay["acquisition"] == "none"
    assert replay["replay_mode"] == (
        "frozen_capture" if capture_mode == "capture" else "typed_heuristic_only"
    )
    assert replay["first_run_id"] != replay["second_run_id"]


def evaluation_consumer(
    evaluation: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capture_mode: str,
) -> tuple[Any, dict[str, Any], dict[str, Any], dict[str, Any], Path]:
    """Keep the smoke consumer real; replace only CLI and unrelated audit reconciliation."""
    scenario = evaluation.EvaluationScenario(tmp_path, "http://127.0.0.1:8123")
    root = tmp_path / "annotations"
    path = root / "evaluations" / ("a" * 64) / ("b" * 64 + ".json")
    capture = path.parent / "generative-captures" / "capture.json"
    generative: dict[str, Any] = {"status": "unavailable", "reason": "model_not_served"}
    cli_generative = dict(generative)
    if capture_mode == "capture":
        capture.parent.mkdir(parents=True)
        capture.write_bytes(b'{"frozen":true}\n')
        digest = evaluation.file_sha256(capture)
        generative.update(
            status="available",
            reason=None,
            capture_sha256=digest,
            capture_path=capture.relative_to(root).as_posix(),
        )
        cli_generative = {**generative, "capture_path": str(capture)}
    elif capture_mode != "legacy":
        generative.update(capture_sha256=None, capture_path=None)
        cli_generative = dict(generative)
    payload = {
        "generative_comparator": cli_generative,
        "evaluation_path": str(path),
        "evaluation_sha256": "b" * 64,
        "run_id": "first",
        "manifest_sha256": "a" * 64,
        "label_origin": "engineering_authored",
        "independent_adjudication": False,
        "partitions": {},
    }
    run = {"generative_comparator": generative, "wall_seconds": 0.1}
    document = {
        "comparators": {
            "generative": {
                "comparison": "performed" if capture_mode == "capture" else "not_performed"
            }
        },
        "claims": {"population_accuracy": False},
    }
    monkeypatch.setattr(scenario, "checked_evaluation", lambda *args: (path, document, run))
    monkeypatch.setattr(
        scenario, "reconcile_evaluation", lambda document: {"rejected": [], "excluded": []}
    )
    monkeypatch.setattr(scenario, "cli_with_configured_runtime", lambda *args: (0, payload))
    return scenario, payload, run, document, path


@pytest.mark.parametrize("field", ["status", "reason", "capture_sha256", "capture_path"])
def test_real_evaluation_rejects_cli_run_provenance_mismatch(
    smoke_modules: dict[str, ModuleType],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    evaluation = smoke_modules["smoke_evaluation"]
    scenario, payload, _, _, _ = evaluation_consumer(evaluation, tmp_path, monkeypatch, "capture")
    payload["generative_comparator"][field] = "wrong"
    with pytest.raises(smoke_modules["smoke_annotations"].SmokeFailure, match="disagree"):
        scenario.real_evaluation()
    assert scenario.engineering_payload == {}


@pytest.mark.parametrize("capture_mode", ["capture", "no_capture"])
def test_real_evaluation_rejects_comparison_without_matching_capture_state(
    smoke_modules: dict[str, ModuleType],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capture_mode: str,
) -> None:
    evaluation = smoke_modules["smoke_evaluation"]
    scenario, _, _, document, _ = evaluation_consumer(
        evaluation, tmp_path, monkeypatch, capture_mode
    )
    document["comparators"]["generative"]["comparison"] = (
        "not_performed" if capture_mode == "capture" else "performed"
    )
    with pytest.raises(smoke_modules["smoke_annotations"].SmokeFailure, match="generative"):
        scenario.real_evaluation()


@pytest.mark.parametrize("field", ["evaluation_sha256", "evaluation_path"])
def test_evaluation_replay_rejects_changed_frozen_identity(
    smoke_modules: dict[str, ModuleType],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    scenario, payload, _, _, _ = evaluation_consumer(
        smoke_modules["smoke_evaluation"], tmp_path, monkeypatch, "no_capture"
    )
    scenario.real_evaluation()
    replay = {**payload, "run_id": "second", field: "changed"}
    monkeypatch.setattr(scenario, "cli", lambda *args: (0, replay))

    def forbid_fresh_acquisition(*args: str) -> Any:
        pytest.fail("replay attempted fresh configured acquisition")

    monkeypatch.setattr(scenario, "cli_with_configured_runtime", forbid_fresh_acquisition)
    with pytest.raises(smoke_modules["smoke_annotations"].SmokeFailure, match="replay"):
        scenario.evaluation_replay()
    assert scenario.evidence[-1]["step"] == "real_evaluation"
