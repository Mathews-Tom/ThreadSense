"""Shipped `threadsense decisions evaluate` over annotations produced by the shipped CLI.

Every saved decision is produced by `threadsense decisions annotate` through a real loopback
socket. The service side returns a synthesized batch response built for the exact request the
client sends; that is an engineering transport proving wiring, replay and assessment, not
evidence about any model. Labels are the shipped engineering-authored fixture labels.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

import httpx
import pytest
from recorded_laya import (
    HEALTH_PAYLOAD,
    RecordedLayaServer,
    RecordedResponse,
    json_response,
    serve_recorded_laya,
)

import threadsense.cli
from threadsense.cli import main
from threadsense.contracts import ObjectiveType
from threadsense.decisions.artifact import decision_bytes
from threadsense.decisions.context import build_comment_states, plan_batches
from threadsense.decisions.contracts import (
    PINNED_CONTEXT_POLICY,
    PINNED_INFERENCE_SETTINGS,
    ChoiceQuestion,
    CommentState,
    NoulQuestion,
    Question,
    QuestionSet,
    RuntimeManifest,
    ScoreQuestion,
)
from threadsense.decisions.evaluate import evaluate_frozen_assessment
from threadsense.decisions.questions import build_question_set
from threadsense.decisions.runtime import parse_runtime_manifest
from threadsense.decisions.saved import load_saved_decision
from threadsense.domains.loader import load_domain_vocabulary
from threadsense.models.canonical import canonical_thread_from_payload

REPO_ROOT = Path(__file__).resolve().parents[2]
DATASET_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "decisions" / "evaluation"
DOMAIN = "developer_tools"
OBJECTIVE = ObjectiveType.FRICTION_ANALYSIS
CLIENT_COMMENT_BUDGET = 2000
SMALL_CHECKPOINT_FILES = {
    "config.json": b'{"model_type": "evaluation"}\n',
    "tokenizer/tokenizer.json": b'{"version": "evaluation"}\n',
}
PROXY_VARIABLES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
)
PARTITIONS = ("development", "calibration", "evaluation")

ManifestEdit = Callable[[dict[str, Any]], None]


# --- builders ---------------------------------------------------------------------------------


def write_small_checkpoint(directory: Path) -> Path:
    for relative, content in SMALL_CHECKPOINT_FILES.items():
        target = directory / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return directory


def small_manifest() -> RuntimeManifest:
    """The packaged manifest, pinned to the small files of the temp checkpoint."""
    packaged = json.loads(
        resources.files("threadsense.decisions").joinpath("laya_runtime_manifest.json").read_bytes()
    )
    packaged["checkpoint"]["files"] = [
        {"path": relative, "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()}
        for relative, content in SMALL_CHECKPOINT_FILES.items()
    ]
    return parse_runtime_manifest(packaged)


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_bytes())
    assert isinstance(payload, dict)
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def write_dataset_copy(
    target: Path, runtime_manifest_sha256: str, edit: ManifestEdit | None = None
) -> Path:
    """Copy the shipped engineering dataset under ``target`` for the test runtime manifest.

    The shipped manifest binds the real pinned runtime manifest sha256, while this harness pins
    a small temporary checkpoint whose runtime manifest sha256 differs. The copy therefore
    rewrites `bindings.runtime_manifest_sha256` only. Thread files and labels are copied
    byte-for-byte, so every file sha256 binding stays valid; thread paths are rewritten into
    `threads/` because one shipped path points outside the dataset directory. ``edit`` lets a
    test break exactly one further manifest field.
    """
    manifest = read_json(DATASET_FIXTURE / "manifest.json")
    manifest["bindings"]["runtime_manifest_sha256"] = runtime_manifest_sha256
    labels_target = target / manifest["labels"]["path"]
    labels_target.parent.mkdir(parents=True, exist_ok=True)
    labels_target.write_bytes((DATASET_FIXTURE / manifest["labels"]["path"]).read_bytes())
    copied: set[str] = set()
    for entry in manifest["threads"]:
        relative = f"threads/{Path(entry['path']).name}"
        assert relative not in copied
        copied.add(relative)
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((DATASET_FIXTURE / entry["path"]).read_bytes())
        entry["path"] = relative
    if edit is not None:
        edit(manifest)
    return write_json(target / "manifest.json", manifest)


# --- synthesized service answers --------------------------------------------------------------


def _distribution(seed: str, labels: Sequence[str]) -> tuple[dict[str, float], str]:
    """A deterministic, strictly peaked distribution rounded like the service (4 decimals)."""
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    pick = digest[0] % len(labels)
    weights = [1 + digest[index + 1] % 16 for index in range(len(labels))]
    weights[pick] += 32
    total = sum(weights)
    rounded = [round(weight / total, 4) for weight in weights]
    rounded[pick] = round(rounded[pick] + (1 - sum(rounded)), 4)
    return dict(zip(labels, rounded, strict=True)), labels[pick]


def _concentration(probabilities: dict[str, float]) -> float:
    entropy = -sum(value * math.log(value) for value in probabilities.values() if value > 0)
    return round(max(0.0, 1 - entropy / math.log(len(probabilities))), 4)


def _synthetic_answer(question: Question, seed: str) -> dict[str, Any]:
    if isinstance(question, ChoiceQuestion):
        probabilities, choice = _distribution(seed, question.labels)
        return {
            "type": "choice",
            "choice": choice,
            "probabilities": probabilities,
            "confidence": _concentration(probabilities),
            "answer_confidence": probabilities[choice],
        }
    if isinstance(question, ScoreQuestion):
        keys = [str(index) for index in range(len(question.levels))]
        probabilities, top = _distribution(seed, keys)
        expected = sum(index * probabilities[key] for index, key in enumerate(keys))
        return {
            "type": "score",
            "score": round(expected, 4),
            "legend": dict(zip(keys, question.levels, strict=True)),
            "probabilities": probabilities,
            "confidence": _concentration(probabilities),
            "answer_confidence": probabilities[top],
        }
    assert isinstance(question, NoulQuestion)
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    noul = round(digest[0] / 255, 4)
    certainty = round(max(noul, 1 - noul), 4)
    return {"type": "noul", "noul": noul, "confidence": certainty, "answer_confidence": certainty}


def _synthetic_result(state: CommentState, question_set: QuestionSet, repo: Path) -> dict[str, Any]:
    settings = PINNED_INFERENCE_SETTINGS
    state_tokens = max(1, state.state_chars // 4)
    return {
        "model": "laya-rl-agent",
        "answers": {
            question.question_id: _synthetic_answer(
                question, f"{state.comment_id}|{question.question_id}"
            )
            for question in question_set.questions
        },
        "usage": {
            "input_tokens": 600 + state_tokens,
            "output_tokens": 0,
            "state_tokens": state_tokens,
            "state_tokens_dropped": 0,
            "truncated": False,
            "truncated_questions": [],
        },
        "routing": {
            "model": settings.model,
            "repo": str(repo),
            "reason": f"explicit model={settings.model!r}",
            "detection": None,
            "workflow": None,
        },
    }


def synthesize_batch_response(input_path: Path, objective: ObjectiveType, repo: Path) -> bytes:
    """Build a valid Laya batch response for the exact request `annotate` sends for a thread.

    This is an engineering transport, not model evidence: every answer is a deterministic
    function of the comment id and question id, shaped like the recorded wire format
    (4-decimal probabilities, choice = argmax, score = expected level with its legend, noul
    confidence = max(P, 1 - P)) so the shipped validators accept it.
    """
    thread = canonical_thread_from_payload(read_json(input_path))
    question_set = build_question_set(objective, load_domain_vocabulary(DOMAIN))
    states = build_comment_states(thread, PINNED_CONTEXT_POLICY)
    plans = plan_batches(states, question_set, PINNED_INFERENCE_SETTINGS)
    assert len(plans) <= 1, "each fixture thread fits one batch"
    by_index = {state.index: state for state in states}
    results = [
        _synthetic_result(by_index[index], question_set, repo)
        for plan in plans
        for index in plan.item_indexes
    ]
    total_usage = {
        "input_tokens": sum(result["usage"]["input_tokens"] for result in results),
        "output_tokens": 0,
    }
    return json.dumps({"results": results, "total_usage": total_usage}).encode("utf-8")


# --- harness ----------------------------------------------------------------------------------


@dataclass
class EvaluateHarness:
    server: RecordedLayaServer
    checkpoint: Path
    manifest: RuntimeManifest
    store_root: Path
    work_root: Path
    manifest_path: Path
    capsys: pytest.CaptureFixture[str]

    def run_cli(self, *argv: str) -> tuple[int, dict[str, Any]]:
        self.capsys.readouterr()
        exit_code = main(["--output-format", "json", *argv])
        payload = json.loads(self.capsys.readouterr().out)
        assert isinstance(payload, dict)
        return exit_code, payload

    def dataset(self, name: str, edit: ManifestEdit | None = None) -> Path:
        return write_dataset_copy(self.work_root / name, self.manifest.sha256(), edit)

    def thread_files(self) -> dict[str, Path]:
        manifest = read_json(self.manifest_path)
        return {
            entry["thread_id"]: self.manifest_path.parent / entry["path"]
            for entry in manifest["threads"]
        }

    def annotate(self, input_path: Path, objective: ObjectiveType = OBJECTIVE) -> Path:
        body = synthesize_batch_response(input_path, objective, self.checkpoint)
        self.server.batch_response = RecordedResponse(200, "application/json", body)
        exit_code, payload = self.run_cli(
            "decisions",
            "annotate",
            "--input",
            str(input_path),
            "--endpoint",
            self.server.endpoint,
            "--requirement",
            "optional",
            "--objective",
            objective.value,
            "--domain",
            DOMAIN,
            "--output-dir",
            str(self.work_root / "annotations"),
        )
        assert exit_code == 0, payload
        return Path(payload["decision_path"])

    def annotate_all(self, health_down: frozenset[str] = frozenset()) -> dict[str, Path]:
        """Annotate every manifest thread; threads in ``health_down`` see a 503 health check."""
        decisions: dict[str, Path] = {}
        for thread_id, path in self.thread_files().items():
            status = 503 if thread_id in health_down else 200
            self.server.health_response = json_response(HEALTH_PAYLOAD, status=status)
            decisions[thread_id] = self.annotate(path)
        self.server.health_response = json_response(HEALTH_PAYLOAD)
        return decisions

    def evaluate(
        self,
        annotations: Sequence[Path],
        *,
        manifest_path: Path | None = None,
        output_dir: Path | None = None,
        generative_capture: Path | None = None,
    ) -> tuple[int, dict[str, Any]]:
        argv = [
            "decisions",
            "evaluate",
            "--manifest",
            str(manifest_path if manifest_path is not None else self.manifest_path),
        ]
        for annotation in annotations:
            argv += ["--annotation", str(annotation)]
        if output_dir is not None:
            argv += ["--output-dir", str(output_dir)]
        if generative_capture is not None:
            argv += ["--generative-capture", str(generative_capture)]
        return self.run_cli(*argv)


@pytest.fixture
def harness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[EvaluateHarness]:
    checkpoint = write_small_checkpoint(tmp_path / "checkpoint")
    manifest = small_manifest()
    store_root = tmp_path / "store"
    work_root = tmp_path / "work"
    manifest_path = write_dataset_copy(work_root / "dataset", manifest.sha256())

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(store_root))
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "false")
    monkeypatch.delenv("THREADSENSE_STORAGE_ANNOTATIONS_DIR", raising=False)
    for name in PROXY_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(threadsense.cli, "load_pinned_runtime_manifest", lambda: manifest)

    empty_batch = RecordedResponse(200, "application/json", b"{}")
    with serve_recorded_laya(json_response(HEALTH_PAYLOAD), empty_batch) as server:
        yield EvaluateHarness(
            server=server,
            checkpoint=checkpoint,
            manifest=manifest,
            store_root=store_root,
            work_root=work_root,
            manifest_path=manifest_path,
            capsys=capsys,
        )


def corpus_comments(harness: EvaluateHarness) -> list[tuple[str, str, str]]:
    """(thread_id, comment_id, body) for every comment of the copied dataset."""
    comments: list[tuple[str, str, str]] = []
    for thread_id, path in harness.thread_files().items():
        thread = canonical_thread_from_payload(read_json(path))
        comments.extend(
            (thread_id, comment.comment_id, comment.body) for comment in thread.comments
        )
    return comments


def thread_partitions(manifest_path: Path) -> dict[str, str]:
    manifest = read_json(manifest_path)
    return {entry["thread_id"]: entry["partition"] for entry in manifest["threads"]}


def assert_error(payload: dict[str, Any], code: str, reason: str) -> dict[str, Any]:
    assert payload["status"] == "error"
    assert payload["error"]["code"] == code
    details = payload["error"]["details"]
    assert isinstance(details, dict)
    assert details["reason"] == reason
    return details


# --- 1. full assessment -----------------------------------------------------------------------


def test_decisions_evaluate_all_threads_publishes_ready_content_addressed_evaluation(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = harness.annotate_all()

    # Act
    exit_code, payload = harness.evaluate(list(annotations.values()))

    # Assert
    assert exit_code == 0, payload
    assert payload["artifact_type"] == "decision_evaluation"
    assert payload["status"] == "ready"
    assert payload["mode"] == "shadow"
    evaluation_path = Path(payload["evaluation_path"])
    content = evaluation_path.read_bytes()
    assert hashlib.sha256(content).hexdigest() == payload["evaluation_sha256"]
    assert evaluation_path.stem == payload["evaluation_sha256"]
    assert evaluation_path.parent.name == payload["manifest_sha256"]
    default_root = harness.store_root / "annotations" / "evaluations"
    assert evaluation_path.resolve().is_relative_to(default_root.resolve())
    manifest_sha256 = hashlib.sha256(harness.manifest_path.read_bytes()).hexdigest()
    assert payload["manifest_sha256"] == manifest_sha256


def test_decisions_evaluate_all_threads_partitions_reconcile_with_corpus(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = harness.annotate_all()
    partitions = thread_partitions(harness.manifest_path)
    comments = corpus_comments(harness)

    # Act
    exit_code, payload = harness.evaluate(list(annotations.values()))

    # Assert
    assert exit_code == 0, payload
    document = read_json(Path(payload["evaluation_path"]))
    assert list(document["partitions"]) == list(PARTITIONS)
    for name in PARTITIONS:
        population = document["partitions"][name]["population"]
        assert payload["partitions"][name] == population
        expected_comments = sum(
            1 for thread_id, _comment_id, _body in comments if partitions[thread_id] == name
        )
        assert population["comments"] == expected_comments
        assert population["comments"] == (
            population["ok"] + population["refused"] + population["unavailable"]
        )
        assert population["eligible"] == population["ok"] - population["excluded_language"]
        assert population["threads"] == sum(1 for value in partitions.values() if value == name)
    assert document["partition_integrity"]["status"] == "passed"
    assert [annotation["replay"]["status"] for annotation in document["annotations"]] == [
        "reproduced"
    ] * len(annotations)


def test_decisions_evaluate_engineering_labels_disclose_origin_without_population_claim(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = harness.annotate_all()

    # Act
    exit_code, payload = harness.evaluate(list(annotations.values()))

    # Assert
    assert exit_code == 0, payload
    assert payload["label_origin"] == "engineering_authored"
    assert payload["independent_adjudication"] is False
    document = read_json(Path(payload["evaluation_path"]))
    assert document["claims"]["label_origin"] == "engineering_authored"
    assert document["claims"]["independent_adjudication"] is False
    assert document["claims"]["population_accuracy"] == "not_claimed"
    assert document["calibration"] == "uncalibrated"
    assert document["option_stability"]["status"] == "vacuous"
    assert document["option_stability"]["probes"] == []
    assert document["comparators"]["generative"]["comparison"] == "not_performed"


def test_decisions_evaluate_client_budget_comment_is_rejected_not_scored(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = harness.annotate_all()
    oversized = [
        (thread_id, comment_id)
        for thread_id, comment_id, body in corpus_comments(harness)
        if len(body) > CLIENT_COMMENT_BUDGET
    ]
    assert len(oversized) == 1
    assert [
        len(body)
        for thread_id, comment_id, body in corpus_comments(harness)
        if (thread_id, comment_id) == oversized[0]
    ] == [CLIENT_COMMENT_BUDGET + 1]

    # Act
    exit_code, payload = harness.evaluate(list(annotations.values()))

    # Assert
    assert exit_code == 0, payload
    document = read_json(Path(payload["evaluation_path"]))
    rejected = {
        (entry["thread_id"], entry["comment_id"]): entry for entry in document["audit"]["rejected"]
    }
    assert list(rejected) == oversized
    assert rejected[oversized[0]]["status"] == "refused"
    assert rejected[oversized[0]]["reasons"] == ["comment_exceeds_client_char_budget"]
    for case in document["engineering_cases"]:
        for item in case["items"]:
            if (item["thread_id"], item["comment_id"]) == oversized[0]:
                assert item["outcome"] == "not_covered"
                assert item["actual"] is None


def test_decisions_evaluate_declared_non_english_comments_are_excluded(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = harness.annotate_all()
    manifest = read_json(harness.manifest_path)
    supported = set(manifest["supported_languages"])
    declared = sorted(
        (entry["thread_id"], comment_id, language)
        for entry in manifest["threads"]
        for comment_id, language in entry["comment_languages"].items()
        if language not in supported
    )
    assert declared

    # Act
    exit_code, payload = harness.evaluate(list(annotations.values()))

    # Assert
    assert exit_code == 0, payload
    document = read_json(Path(payload["evaluation_path"]))
    excluded = sorted(
        (entry["thread_id"], entry["comment_id"], entry["language"])
        for entry in document["audit"]["excluded"]
    )
    assert excluded == declared
    assert {entry["reason"] for entry in document["audit"]["excluded"]} == {"language_unsupported"}
    assert sum(
        document["partitions"][name]["population"]["excluded_language"] for name in PARTITIONS
    ) == len(declared)


def test_decisions_evaluate_runtime_disabled_records_generative_comparator_not_observed(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = harness.annotate_all()

    # Act
    exit_code, payload = harness.evaluate(list(annotations.values()))

    # Assert
    assert exit_code == 0, payload
    assert payload["generative_comparator"] == {
        "status": "not_observed",
        "reason": "disabled_by_configuration",
        "capture_path": None,
        "capture_sha256": None,
    }
    record = read_json(Path(payload["run_path"]))
    assert record["artifact_kind"] == "typed_decision_evaluation_run"
    assert record["run_id"] == payload["run_id"]
    assert record["evaluation_sha256"] == payload["evaluation_sha256"]
    output_root = harness.store_root / "annotations"
    assert (output_root / record["evaluation_path"]).resolve() == Path(
        payload["evaluation_path"]
    ).resolve()
    assert record["generative_comparator"]["enabled"] is False
    assert record["generative_comparator"]["status"] == "not_observed"
    assert record["generative_comparator"]["reason"] == "disabled_by_configuration"
    assert record["generative_comparator"]["served_models"] is None
    assert record["wall_seconds"] >= 0


# --- 2. replay determinism --------------------------------------------------------------------


def test_decisions_evaluate_repeat_run_reuses_evaluation_with_new_run_record(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = list(harness.annotate_all().values())
    first_code, first = harness.evaluate(annotations)

    # Act
    second_code, second = harness.evaluate(annotations)

    # Assert
    assert (first_code, second_code) == (0, 0)
    assert second["evaluation_sha256"] == first["evaluation_sha256"]
    assert second["evaluation_path"] == first["evaluation_path"]
    assert second["run_id"] != first["run_id"]
    assert second["run_path"] != first["run_path"]
    assert Path(first["run_path"]).is_file()
    assert Path(second["run_path"]).is_file()


# --- 3. refusals ------------------------------------------------------------------------------


def test_decisions_evaluate_stale_question_binding_refuses_with_stale_binding(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = list(harness.annotate_all().values())

    def stale_question_set(manifest: dict[str, Any]) -> None:
        manifest["bindings"]["question_set_sha256"] = "0" * 64

    manifest_path = harness.dataset("stale", stale_question_set)

    # Act
    exit_code, payload = harness.evaluate(annotations, manifest_path=manifest_path)

    # Assert
    assert exit_code == 1
    details = assert_error(payload, "decision_evaluation_error", "stale_binding")
    assert details["field"] == "question_set_sha256"
    assert details["bound"] == "0" * 64


def test_decisions_evaluate_missing_annotation_refuses_with_annotation_missing(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = harness.annotate_all()
    omitted = next(iter(annotations))
    supplied = [path for thread_id, path in annotations.items() if thread_id != omitted]

    # Act
    exit_code, payload = harness.evaluate(supplied)

    # Assert
    assert exit_code == 1
    details = assert_error(payload, "decision_evaluation_error", "annotation_missing")
    assert details["thread_ids"] == [omitted]


def test_decisions_evaluate_duplicate_annotation_refuses_with_annotation_duplicate(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = list(harness.annotate_all().values())

    # Act
    exit_code, payload = harness.evaluate([*annotations, annotations[0]])

    # Assert
    assert exit_code == 1
    assert_error(payload, "decision_evaluation_error", "annotation_duplicate")


def test_decisions_evaluate_unlisted_snapshot_refuses_with_annotation_unbound(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = harness.annotate_all()
    listed = next(iter(harness.thread_files().values()))
    # Same canonical content, different bytes: a snapshot the manifest does not bind.
    unlisted = harness.work_root / "unlisted" / listed.name
    unlisted.parent.mkdir(parents=True)
    unlisted.write_text(json.dumps(read_json(listed), indent=4) + "\n", encoding="utf-8")
    assert unlisted.read_bytes() != listed.read_bytes()
    extra = harness.annotate(unlisted)

    # Act
    exit_code, payload = harness.evaluate([*annotations.values(), extra])

    # Assert
    assert exit_code == 1
    details = assert_error(payload, "decision_evaluation_error", "annotation_unbound")
    assert details["snapshot_sha256"] == hashlib.sha256(unlisted.read_bytes()).hexdigest()


def test_decisions_evaluate_other_objective_annotation_refuses_with_identity_mismatch(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = harness.annotate_all()
    replaced_thread, replaced_input = next(iter(harness.thread_files().items()))
    annotations[replaced_thread] = harness.annotate(
        replaced_input, objective=ObjectiveType.FEATURE_DEMAND
    )

    # Act
    exit_code, payload = harness.evaluate(list(annotations.values()))

    # Assert
    assert exit_code == 1
    details = assert_error(payload, "decision_evaluation_error", "annotation_identity_mismatch")
    assert details["field"] == "question_set_sha256"


def test_decisions_evaluate_unverified_checkpoint_with_ok_items_refuses_with_identity_mismatch(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = harness.annotate_all()
    edited_thread, original = next(iter(annotations.items()))
    document = read_json(original)
    assert document["coverage"]["ok"] > 0
    observed_file = document["backend"]["checkpoint_verification"]["files"][0]
    observed_file["sha256"] = "0" * 64 if observed_file["sha256"] != "0" * 64 else "1" * 64
    edited_bytes = decision_bytes(document)
    edited_sha256 = hashlib.sha256(edited_bytes).hexdigest()
    edited = original.parent / f"{edited_sha256}.json"
    edited.write_bytes(edited_bytes)
    annotations[edited_thread] = edited

    # Act
    exit_code, payload = harness.evaluate(list(annotations.values()))

    # Assert
    assert exit_code == 1
    details = assert_error(payload, "decision_evaluation_error", "annotation_identity_mismatch")
    assert details["field"] == "checkpoint_verification"
    assert details["decision_sha256"] == edited_sha256


def test_decisions_evaluate_partition_crossing_issue_group_refuses_with_partition_leak(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = list(harness.annotate_all().values())
    manifest = read_json(harness.manifest_path)
    crossing = next(
        group
        for group in manifest["issue_groups"]
        if len({member["thread_id"] for member in group["members"]}) > 1
    )
    moved_thread = crossing["members"][0]["thread_id"]

    def move_thread(edited: dict[str, Any]) -> None:
        # Group members share one partition; moving one member's thread splits the group.
        for entry in edited["threads"]:
            if entry["thread_id"] == moved_thread:
                entry["partition"] = next(name for name in PARTITIONS if name != entry["partition"])

    manifest_path = harness.dataset("leak", move_thread)

    # Act
    exit_code, payload = harness.evaluate(annotations, manifest_path=manifest_path)

    # Assert
    assert exit_code == 1
    details = assert_error(payload, "decision_evaluation_error", "partition_leak")
    assert details["violations"]


def test_decisions_evaluate_output_inside_protected_storage_refuses(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    annotations = list(harness.annotate_all().values())
    protected = harness.store_root / "normalized" / "evaluations"

    # Act
    exit_code, payload = harness.evaluate(annotations, output_dir=protected)

    # Assert
    assert exit_code == 1
    assert_error(payload, "decision_input_error", "output_inside_protected_storage")
    assert not protected.exists()


def test_public_evaluate_acquires_paired_theme_and_replays_offline(
    harness: EvaluateHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    annotations = tuple(harness.annotate_all().values())
    original_evaluate = evaluate_frozen_assessment
    requests: list[httpx.Request] = []
    model = "engineering-served-model"
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("THREADSENSE_RUNTIME_MODEL", model)

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": model}]})
        prompt = json.loads(request.content)["messages"][-1]["content"]
        ids = re.findall(r"^\[id=([^\]]+)\]", prompt, re.MULTILINE)
        assert ids
        classifications = [
            {"comment_id": cid, "theme": "general_feedback", "confidence": 0.8}
            for cid in reversed(ids)
        ]
        return httpx.Response(
            200,
            json={
                "id": "engineering-reply",
                "object": "chat.completion",
                "model": model,
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": json.dumps({"classifications": classifications}),
                        },
                    }
                ],
            },
        )

    def evaluate_with_transport(*args: Any, **kwargs: Any) -> Any:
        return original_evaluate(*args, **kwargs, comparator_transport=httpx.MockTransport(respond))

    monkeypatch.setattr(threadsense.cli, "evaluate_frozen_assessment", evaluate_with_transport)
    code, acquired = harness.evaluate(annotations)
    assert code == 0, acquired
    assert acquired["generative_comparator"]["status"] == "available"
    capture_path = Path(acquired["generative_comparator"]["capture_path"])
    assert capture_path.stem == hashlib.sha256(capture_path.read_bytes()).hexdigest()
    document = read_json(Path(acquired["evaluation_path"]))
    comparison = document["comparators"]["generative"]
    assert comparison["comparison"] == "performed"
    comments = {(tid, cid): body for tid, cid, body in corpus_comments(harness)}
    eligible = {
        (decision.thread_id, item.comment_id)
        for path in annotations
        for decision in [load_saved_decision(path)]
        for item in decision.items
        if item.status.value == "ok"
    }
    excluded_language = {
        (entry["thread_id"], entry["comment_id"]) for entry in document["audit"]["excluded"]
    }
    expected = {key for key in eligible - excluded_language if len(comments[key]) <= 300}
    actual = {
        (entry["thread_id"], entry["comment_id"])
        for block in comparison["partitions"].values()
        for entry in block["ids"]
    }
    assert actual == expected
    assert actual
    for block in comparison["partitions"].values():
        for name in ("typed", "heuristic", "generative"):
            assert block[name]["decided"] + block[name]["abstained"] == block["items"]
        assert block["generative"]["decided"] == 0
        assert block["generative"]["labels"]["error_rate"] is None
    assert sum(request.method == "GET" for request in requests) == 1
    frozen = Path(acquired["evaluation_path"]).read_bytes()
    acquired_availability = read_json(Path(acquired["run_path"]))["generative_comparator"]
    request_count = len(requests)
    network_attempts = 0

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        nonlocal network_attempts
        network_attempts += 1
        pytest.fail("capture replay attempted network")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    code, replay = harness.evaluate(annotations, generative_capture=capture_path)
    assert code == 0, replay
    assert network_attempts == 0
    assert len(requests) == request_count
    assert replay["generative_comparator"]["reason"] == "replayed_frozen_capture"
    replay_availability = read_json(Path(replay["run_path"]))["generative_comparator"]
    assert replay_availability["status"] == "available"
    assert replay_availability["reason"] == "replayed_frozen_capture"
    assert replay_availability["method"] is None
    assert replay_availability["served_models"] is None
    captured_availability = replay_availability["captured_availability"]
    assert captured_availability["status"] == acquired_availability["status"]
    assert captured_availability["reason"] == acquired_availability["reason"]
    assert captured_availability["method"] == acquired_availability["method"]
    assert captured_availability["served_models"] == [model]
    assert Path(replay["evaluation_path"]).read_bytes() == frozen
    assert replay["evaluation_sha256"] == acquired["evaluation_sha256"]
    tampered = harness.work_root / ("0" * 64 + ".json")
    tampered.write_bytes(capture_path.read_bytes())
    code, refused = harness.evaluate(annotations, generative_capture=tampered)
    assert code == 1
    assert_error(refused, "decision_evaluation_error", "generative_capture_invalid")


@pytest.mark.parametrize("confidence", [None, 10**400], ids=["invalid-task", "overflow-confidence"])
def test_public_evaluate_failed_generation_preserves_typed_evidence(
    harness: EvaluateHarness,
    monkeypatch: pytest.MonkeyPatch,
    confidence: int | None,
) -> None:
    annotations = tuple(harness.annotate_all().values())
    code, baseline = harness.evaluate(annotations)
    assert code == 0, baseline
    baseline_bytes = Path(baseline["evaluation_path"]).read_bytes()
    original_evaluate = evaluate_frozen_assessment
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("THREADSENSE_RUNTIME_MODEL", "engineering-served-model")

    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": "engineering-served-model"}]})
        classifications = []
        if confidence is not None:
            prompt = json.loads(request.content)["messages"][-1]["content"]
            ids = re.findall(r"^\[id=([^\]]+)\]", prompt, re.MULTILINE)
            assert ids
            classifications = [
                {"comment_id": cid, "theme": "general_feedback", "confidence": confidence}
                for cid in ids
            ]
        content = (
            json.dumps({"classifications": classifications}) if confidence is not None else "{}"
        )
        return httpx.Response(
            200,
            json={
                "id": "engineering-reply",
                "object": "chat.completion",
                "model": "engineering-served-model",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": content},
                    }
                ],
            },
        )

    def evaluate_with_transport(*args: Any, **kwargs: Any) -> Any:
        return original_evaluate(*args, **kwargs, comparator_transport=httpx.MockTransport(respond))

    monkeypatch.setattr(threadsense.cli, "evaluate_frozen_assessment", evaluate_with_transport)
    code, failed = harness.evaluate(annotations)
    assert code == 0, failed
    assert failed["generative_comparator"]["status"] == "unavailable"
    assert failed["generative_comparator"]["reason"] == "inference_failed"
    assert failed["generative_comparator"]["capture_path"] is None
    assert failed["generative_comparator"]["capture_sha256"] is None
    assert Path(failed["evaluation_path"]).read_bytes() == baseline_bytes
    assert failed["evaluation_sha256"] == baseline["evaluation_sha256"]


# --- 4. unavailable service -------------------------------------------------------------------


def test_decisions_evaluate_health_unavailable_annotations_count_labels_not_covered(
    harness: EvaluateHarness,
) -> None:
    # Arrange
    partitions = thread_partitions(harness.manifest_path)
    down = frozenset(thread for thread, name in partitions.items() if name == "evaluation")
    annotations = harness.annotate_all(health_down=down)
    labels = read_json(harness.manifest_path.parent / "labels.json")["labels"]

    # Act
    exit_code, payload = harness.evaluate(list(annotations.values()))

    # Assert
    assert exit_code == 0, payload
    document = read_json(Path(payload["evaluation_path"]))
    population = document["partitions"]["evaluation"]["population"]
    assert population["ok"] == 0
    assert population["eligible"] == 0
    rejected = [
        entry for entry in document["audit"]["rejected"] if entry["partition"] == "evaluation"
    ]
    assert len(rejected) == population["comments"]
    for entry in rejected:
        assert entry["reasons"] in (
            ["runtime_health_unavailable"],
            ["comment_exceeds_client_char_budget"],
        )
    for question_id, metrics in document["partitions"]["evaluation"]["questions"].items():
        labeled = sum(
            1
            for label in labels
            if label["question_id"] == question_id
            and partitions[label["thread_id"]] == "evaluation"
        )
        assert metrics["decided"] == 0
        assert metrics["labels"]["labeled"] == labeled
        assert metrics["labels"]["labeled_not_covered"] == labeled
        assert metrics["labels"]["labeled_decided"] == 0
        assert metrics["labels"]["error_rate"] is None
        assert metrics["labels"]["status"] == ("available" if labeled else "unavailable")
    for case in document["engineering_cases"]:
        for item in case["items"]:
            if item["partition"] == "evaluation":
                assert item["outcome"] == "not_covered"
