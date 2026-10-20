"""Typed-decision evaluation and activation-policy smoke exercised through the real CLI.

Driven by `scripts/smoke_monitoring.py --scenario evaluation --endpoint URL`. The endpoint is
a locally running `laya.serve` with the pinned checkpoint already loaded; this module never
starts, stops or configures it and never downloads anything. Every product step runs
`python -m threadsense --output-format json ...` in a fresh interpreter with a scrubbed
environment and isolated storage under the work directory. Only loopback endpoints are used.

The dataset is the committed engineering evaluation fixture staged into the work directory. Its
labels are engineering-authored, not independent adjudication, so the supported real policy
outcome is `not_eligible`. The test-only eligible path uses a constructed copy of the same label
values whose origin is disclosed as a constructed test fixture, checked against the
engineering-only fixture tolerances declared below before any model call. That path exercises
the checker mechanics and supports no quality claim. No policy is installed anywhere and no
consumer is activated.
"""

from __future__ import annotations

import copy
import json
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

from smoke_annotations import (
    ANNOTATE_TIMEOUT_SECONDS,
    CHECKPOINT_BASIS,
    DEFAULT_TIMEOUT_SECONDS,
    DOMAIN,
    LABEL_NO_QUALITY_CLAIM,
    OBJECTIVE,
    AnnotationsScenario,
    check,
    closed_loopback_port,
    decode_payload,
    file_sha256,
    load_json_object,
    run_process,
    source_root,
)

from threadsense.decisions.contracts import UsageCoverage
from threadsense.decisions.runtime import load_pinned_runtime_manifest

FIXTURES_RELPATH = Path("tests/fixtures")
DATASET_RELPATH = FIXTURES_RELPATH / "decisions" / "evaluation"
ANALYSIS_THREAD_RELPATH = FIXTURES_RELPATH / "analysis" / "canonical_feedback_thread.json"
FIXTURE_PROVENANCE = (
    f"{DATASET_RELPATH} and {ANALYSIS_THREAD_RELPATH}, committed engineering evaluation "
    "fixture staged into the work directory"
)
SCENARIO_SCOPE = (
    "real local checkpoint through the shipped CLI; source data is a committed fixture, "
    "no live source; evaluation and policy checks run offline"
)
LABEL_ORIGIN_DISCLOSURE = (
    "engineering-authored labels: not independent adjudication, not representative of any "
    "population and not ground truth"
)
LABEL_RESOURCE = (
    "single point observations of wall time and resident set size: "
    "not quality, not peak, not comparative"
)
GENERATIVE_NOTE = (
    "availability observed through the configured loopback model listing only; no generative "
    "comparison was performed and no accuracy is attributed to the generative comparator"
)
REAL_OUTCOME_NOTE = (
    "not_eligible is the supported real outcome: the engineering labels are not independent "
    "adjudication and no domain tolerance has been supplied"
)

CLIENT_COMMENT_BUDGET = 2000
OVERSIZED_REASON = "comment_exceeds_client_char_budget"
UNAVAILABLE_REASON = "runtime_health_unavailable"
# Every server-reported usage refusal reason, read from UsageCoverage.refusal_reasons with each
# reporting field set. Only a boundary comment (body exactly CLIENT_COMMENT_BUDGET characters)
# may be refused with a non-empty subset of these; it is then observed, never scored.
SERVER_REFUSAL_REASONS = frozenset(
    UsageCoverage(
        input_tokens=0,
        output_tokens=0,
        state_tokens=0,
        state_tokens_dropped=1,
        truncated=True,
        truncated_questions=("question",),
        options_reported=True,
    ).refusal_reasons()
)
BOUNDARY_NOTE = (
    "the client character budget is necessary but not sufficient: the pinned service may "
    "refuse an at-budget comment with server usage reasons; observed, unscored, audited as "
    "rejected; not a quality claim"
)
PARTITIONS = ("development", "calibration", "evaluation")
HELD_OUT_PARTITION = "evaluation"
AUDIT_KINDS = ("rejected", "excluded", "abstained")

POLICY_KIND = "typed_decision_activation_policy"
POLICY_CHECK_KIND = "typed_decision_policy_check"
POLICY_CHECKS_DIRNAME = "policy-checks"
POLICY_CONSUMER = "semantic_reranking"
# Predeclared before any model call: the policy question must have held-out labels in the
# staged dataset, the invalidation question must have none. Neither is chosen from outcomes.
POLICY_QUESTION = "theme"
MISSING_LABELS_QUESTION = "actionability"
IDENTITY_KEYS = (
    "runtime_manifest_sha256",
    "checkpoint_revision",
    "question_set_sha256",
    "settings_sha256",
    "context_policy_sha256",
    "language",
)
BINDING_KEYS = (*IDENTITY_KEYS, "dataset_manifest_sha256", "labels_sha256")

# Engineering-only fixture tolerances, declared before any model call and never derived from
# observed outcomes. They exercise the checker mechanics; they are not quality thresholds.
FIXTURE_TOLERANCES: dict[str, float | int] = {
    "max_error_rate": 1.0,
    "min_decision_rate": 0.0,
    "min_labeled_decided": 1,
}
FIXTURE_TOLERANCE_DISCLOSURE = (
    "max_error_rate=1.0, min_decision_rate=0.0, min_labeled_decided=1 are engineering-only "
    "fixture tolerances declared before any model call to exercise the policy checker "
    "mechanics; they are not quality thresholds and were not derived from observed outcomes"
)
# Fixed one-field invalidation values, also declared before any model call.
INVALID_MAX_ERROR_RATE = 0.0
INVALID_MIN_DECISION_RATE = 1.0
INVALID_MIN_LABELED_DECIDED = 1_000_000
OUT_OF_RANGE_MIN_DECISION_RATE = 1.5
STALE_SHA256 = "0" * 64
STALE_REVISION = "0" * 40
STALE_LANGUAGE = "de"

CONSTRUCTED_DATASET_SUFFIX = "-constructed-test-only"
CONSTRUCTED_ORIGIN = {
    "kind": "constructed_test_fixture",
    "authored_by": "ThreadSense evaluation smoke, copied from the engineering fixture labels",
    "method": (
        "Every label value is copied unchanged from the engineering-authored fixture labels; "
        "only this origin block is replaced to construct test-only evidence."
    ),
    "disclosure": (
        "Constructed test fixture built for tests to exercise the activation policy checker. "
        "The label values are the engineering-authored fixture expectations: not independent "
        "adjudication, not representative of any population and not ground truth."
    ),
}
TEST_ONLY_POLICY_DISCLOSURE = (
    "Test-only policy over a constructed test fixture, built for tests to exercise the "
    "activation policy checker. It can never permit activation and its tolerances are "
    "engineering-only fixture values, not quality thresholds."
)
RUNTIME_POLICY_DISCLOSURE = (
    "Runtime-scope policy over the engineering-labelled evaluation, checked to show that labels "
    "without independent adjudication cannot back activation. Its tolerances are "
    "engineering-only fixture values, not quality thresholds."
)

OUTPUT_ROOTS = frozenset(
    {"fixtures", "annotations", "annotations-unavailable", "store", "policies"}
)
EVIDENCE_DEPENDENT_CHECKS = (
    "runtime_identity",
    "policy_bindings",
    "label_provenance",
    "evidence_integrity",
    "held_out_coverage",
    "reconciliation",
)
COVERAGE_UNSUPPORTED_ALLOWED = frozenset({"coverage_unsupported", "held_out_labels_missing"})
STEP_NAMES = (
    "stage_dataset",
    "real_annotations",
    "annotation_replay_deterministic",
    "server_memory_observation",
    "real_evaluation",
    "evaluation_replay",
    "evaluation_refusals",
    "real_policy_not_eligible",
    "test_only_eligible",
    "paired_invalidations",
    "unsupported_coverage",
    "no_policy_installed_inputs_unchanged",
)

ManifestEdit = Callable[[dict[str, Any]], None]


# --- pure helpers -----------------------------------------------------------------------------


def relocate(path: str, source_dir: Path, target_dir: Path) -> str:
    """`path` relative to `source_dir`, re-expressed relative to `target_dir`."""
    absolute = (source_dir / path).resolve()
    return Path(os.path.relpath(absolute, target_dir.resolve())).as_posix()


def constructed_labels(labels: dict[str, Any]) -> dict[str, Any]:
    """The same label values under a disclosed constructed-test-fixture origin."""
    return {**copy.deepcopy(labels), "origin": dict(CONSTRUCTED_ORIGIN)}


def evidence_bindings(document: dict[str, Any]) -> dict[str, str]:
    identity = document["identity"]
    bindings: dict[str, str] = {key: identity[key] for key in IDENTITY_KEYS}
    bindings["dataset_manifest_sha256"] = document["dataset"]["manifest_sha256"]
    bindings["labels_sha256"] = document["dataset"]["labels"]["sha256"]
    return bindings


def activation_policy(
    scope: str,
    question_id: str,
    bindings: dict[str, str],
    tolerances: dict[str, float | int],
) -> dict[str, Any]:
    disclosure = TEST_ONLY_POLICY_DISCLOSURE if scope == "test_only" else RUNTIME_POLICY_DISCLOSURE
    return {
        "artifact_kind": POLICY_KIND,
        "schema_version": 1,
        "policy_id": f"smoke-evaluation-{scope}-{question_id}",
        "scope": scope,
        "disclosure": disclosure,
        "decision": {"question_id": question_id, "consumer": POLICY_CONSUMER},
        "bindings": {key: bindings[key] for key in BINDING_KEYS},
        "tolerances": dict(tolerances),
    }


def policy_with(policy: dict[str, Any], path: tuple[str, ...], value: Any) -> dict[str, Any]:
    """A deep copy of `policy` with exactly the field at `path` replaced."""
    edited = copy.deepcopy(policy)
    target = edited
    for key in path[:-1]:
        target = target[key]
    check(path[-1] in target, f"policy has no field {'.'.join(path)}")
    target[path[-1]] = value
    return edited


@dataclass(frozen=True)
class InvalidationCase:
    """One field of the eligible policy or evidence changed; `expected` None: not applicable."""

    name: str
    target: str
    policy: dict[str, Any]
    evidence: str
    expected: tuple[str, ...] | None
    observed_value: Any = None


def binding_cases(base: dict[str, Any]) -> list[InvalidationCase]:
    changes = (
        ("stale_checkpoint_revision", "checkpoint_revision", STALE_REVISION),
        ("stale_question_set", "question_set_sha256", STALE_SHA256),
        ("stale_language", "language", STALE_LANGUAGE),
        ("stale_dataset_manifest", "dataset_manifest_sha256", STALE_SHA256),
        ("stale_labels", "labels_sha256", STALE_SHA256),
        ("unbound_settings", "settings_sha256", ""),
    )
    codes = {
        "checkpoint_revision": "stale_checkpoint_binding",
        "question_set_sha256": "stale_question_binding",
        "language": "stale_language_binding",
        "dataset_manifest_sha256": "stale_dataset_binding",
        "labels_sha256": "stale_dataset_binding",
    }
    cases: list[InvalidationCase] = []
    for name, key, value in changes:
        check(base["bindings"][key] != value, f"{name}: bound value already equals {value!r}")
        code = codes[key] if value else "unbound_binding"
        edited = policy_with(base, ("bindings", key), value)
        cases.append(InvalidationCase(name, f"bindings.{key}", edited, "constructed", (code,)))
    return cases


def tolerance_cases(base: dict[str, Any], held_out: dict[str, Any]) -> list[InvalidationCase]:
    """Fixed invalidation values; a value the observed metric already satisfies is vacuous."""
    error_rate = held_out["labels"]["error_rate"]
    decision_rate = held_out["decision_rate"]
    labeled_decided = held_out["labels"]["labeled_decided"]

    rows: tuple[tuple[str, str, Any, tuple[str, ...] | None, Any], ...] = (
        ("nonfinite_max_error_rate", "max_error_rate", float("nan"), ("nonfinite_value",), None),
        (
            "min_decision_rate_out_of_range",
            "min_decision_rate",
            OUT_OF_RANGE_MIN_DECISION_RATE,
            ("tolerance_invalid",),
            None,
        ),
        (
            "max_error_rate_zero",
            "max_error_rate",
            INVALID_MAX_ERROR_RATE,
            ("error_above_tolerance",) if error_rate > INVALID_MAX_ERROR_RATE else None,
            error_rate,
        ),
        (
            "min_decision_rate_one",
            "min_decision_rate",
            INVALID_MIN_DECISION_RATE,
            ("coverage_below_tolerance",) if decision_rate < INVALID_MIN_DECISION_RATE else None,
            decision_rate,
        ),
        (
            "min_labeled_decided_unreachable",
            "min_labeled_decided",
            INVALID_MIN_LABELED_DECIDED,
            ("insufficient_labeled_items",)
            if labeled_decided < INVALID_MIN_LABELED_DECIDED
            else None,
            labeled_decided,
        ),
    )
    return [
        InvalidationCase(
            name,
            f"tolerances.{key}",
            policy_with(base, ("tolerances", key), value),
            "constructed",
            expected,
            observed,
        )
        for name, key, value, expected, observed in rows
    ]


def evidence_cases(
    base: dict[str, Any], engineering_policy: dict[str, Any]
) -> list[InvalidationCase]:
    return [
        InvalidationCase(
            "scope_runtime",
            "scope",
            policy_with(base, ("scope",), "runtime"),
            "constructed",
            ("held_out_labels_not_independent",),
        ),
        InvalidationCase(
            "question_without_held_out_labels",
            "decision.question_id",
            policy_with(base, ("decision", "question_id"), MISSING_LABELS_QUESTION),
            "constructed",
            ("held_out_labels_missing",),
        ),
        InvalidationCase("evidence_missing", "evidence", base, "missing", ("evidence_missing",)),
        InvalidationCase(
            "engineering_evidence_test_only_policy",
            "evidence",
            engineering_policy,
            "engineering",
            ("test_fixture_origin_mismatch",),
        ),
    ]


def invalidation_cases(
    base: dict[str, Any], engineering_policy: dict[str, Any], held_out: dict[str, Any]
) -> list[InvalidationCase]:
    return [
        *binding_cases(base),
        *tolerance_cases(base, held_out),
        *evidence_cases(base, engineering_policy),
    ]


def audit_refs(entries: list[dict[str, Any]]) -> list[list[str]]:
    return sorted([entry["thread_id"], entry["comment_id"]] for entry in entries)


def held_out_counts(metrics: dict[str, Any]) -> dict[str, Any]:
    labels = metrics["labels"]
    return {
        "population": metrics["population"],
        "eligible": metrics["eligible"],
        "abstained": metrics["abstained"],
        "decided": metrics["decided"],
        "decision_rate": metrics["decision_rate"],
        "labeled": labels["labeled"],
        "labeled_decided": labels["labeled_decided"],
        "error_rate": labels["error_rate"],
    }


def write_json_file(path: Path, payload: dict[str, Any]) -> Path:
    """Pretty JSON; a non-finite float is written as its literal token (for example `NaN`)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


# --- scenario ---------------------------------------------------------------------------------


class EvaluationScenario(AnnotationsScenario):
    """Real annotations, offline evaluation and the activation policy checker, end to end."""

    def __init__(self, work_dir: Path, endpoint: str) -> None:
        super().__init__(work_dir, endpoint)
        self.observations = {
            "endpoint": endpoint,
            "fixture_provenance": FIXTURE_PROVENANCE,
            "scenario_scope": SCENARIO_SCOPE,
            "label_disclosure": LABEL_ORIGIN_DISCLOSURE,
            "fixture_tolerances": dict(FIXTURE_TOLERANCES),
            "fixture_tolerance_disclosure": FIXTURE_TOLERANCE_DISCLOSURE,
            "resource_label": LABEL_RESOURCE,
            "step_wall_seconds": {},
        }
        self.fixtures_root = work_dir / "fixtures"
        self.staged_dataset = self.fixtures_root / "decisions" / "evaluation"
        self.staged_manifest = self.staged_dataset / "manifest.json"
        self.unavailable_dir = work_dir / "annotations-unavailable"
        self.policies_dir = work_dir / "policies"
        self.git_status_before = ""
        self.pinned_manifest_sha256 = ""
        self.pinned_checkpoint_files: list[dict[str, Any]] = []
        self.source_hashes: dict[str, str] = {}
        self.staged_hashes: dict[str, str] = {}
        self.thread_inputs: dict[str, Path] = {}
        self.partitions: dict[str, str] = {}
        self.comments: dict[str, dict[str, str]] = {}
        self.declared_excluded: list[list[str]] = []
        self.oversized: list[list[str]] = []
        self.boundary: list[list[str]] = []
        self.refused_items: dict[tuple[str, str], list[str]] = {}
        self.annotations: dict[str, Path] = {}
        self.annotation_payloads: dict[str, dict[str, Any]] = {}
        self.annotation_hashes: dict[str, str] = {}
        self.engineering_path = Path()
        self.engineering_payload: dict[str, Any] = {}
        self.engineering_document: dict[str, Any] = {}
        self.constructed_manifest = Path()
        self.constructed_path = Path()
        self.constructed_document: dict[str, Any] = {}
        self.base_policy: dict[str, Any] = {}

    def run(self) -> None:
        self.git_status_before = self.source_git_status()
        wall: dict[str, float] = self.observations["step_wall_seconds"]
        for name in STEP_NAMES:
            step: Callable[[], None] = getattr(self, name)
            started = time.monotonic()
            step()
            wall[name] = round(time.monotonic() - started, 3)

    # --- CLI boundaries ---

    def cli_with_configured_runtime(self, *args: str) -> tuple[int, dict[str, Any]]:
        """Like `cli`, but the runtime switch comes from the `--config` file, not the env."""
        env = {
            key: value
            for key, value in self.base_env.items()
            if key != "THREADSENSE_RUNTIME_ENABLED"
        }
        env["THREADSENSE_STORAGE_ROOT"] = str(self.store_root)
        completed = run_process(
            [sys.executable, "-m", "threadsense", "--output-format", "json", *args],
            capture_output=True,
            text=True,
            check=False,
            cwd=self.work_dir,
            env=env,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        return completed.returncode, decode_payload(
            completed.stdout, completed.stderr, " ".join(args)
        )

    def annotate_thread(
        self, input_path: Path, endpoint: str, output_dir: Path
    ) -> tuple[int, dict[str, Any]]:
        return self.cli(
            *("decisions", "annotate", "--input", str(input_path)),
            *("--endpoint", endpoint, "--requirement", "optional"),
            *("--output-dir", str(output_dir)),
            *("--domain", DOMAIN, "--objective", OBJECTIVE),
            timeout=ANNOTATE_TIMEOUT_SECONDS,
        )

    def evaluate(
        self, manifest: Path, annotations: list[Path], *, configured_runtime: bool = False
    ) -> tuple[int, dict[str, Any]]:
        args = ["decisions", "evaluate", "--manifest", str(manifest)]
        for annotation in annotations:
            args += ["--annotation", str(annotation)]
        if configured_runtime:
            args += ["--config", str(self.repo_root / "threadsense.toml")]
            return self.cli_with_configured_runtime(*args)
        return self.cli(*args)

    def policy_outcome(
        self, evaluation: Path, policy: Path | None
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Run `decisions policy check`; verify the payload and its published document agree."""
        args = ["decisions", "policy", "check", "--evaluation", str(evaluation)]
        if policy is not None:
            args += ["--policy", str(policy)]
        code, payload = self.cli(*args)
        check(code == 0, f"policy check exited {code}: {payload}")
        check(
            (payload["artifact_type"], payload["mode"]) == ("decision_policy_check", "shadow")
            and payload["status"] == payload["eligibility"],
            f"policy check payload {payload}",
        )
        path = self.published_path(payload["policy_check_path"])
        digest = payload["policy_check_sha256"]
        check(
            file_sha256(path) == digest
            and path.stem == digest
            and path.parent.name == POLICY_CHECKS_DIRNAME,
            f"policy check is not content addressed at {self.relative(path)}",
        )
        default_root = (self.store_root / "annotations").resolve()
        check(path.resolve().is_relative_to(default_root), "policy check outside the output root")
        document = load_json_object(path)
        activation = document["activation"]
        check(
            (document["artifact_kind"], document["mode"]) == (POLICY_CHECK_KIND, "shadow")
            and document["eligibility"] == payload["eligibility"]
            and activation["performed"] is False
            and activation["permitted"] is payload["activation_permitted"],
            f"policy check document disagrees with payload {payload}",
        )
        check(
            [reason["code"] for reason in document["reasons"]] == payload["reasons"],
            "policy check document reasons differ from the payload",
        )
        permitted = payload["eligibility"] == "eligible" and payload["scope"] == "runtime"
        check(payload["activation_permitted"] is permitted, "activation_permitted rule violated")
        return payload, document

    def write_policy(self, name: str, policy: dict[str, Any]) -> Path:
        return write_json_file(self.policies_dir / f"{name}.json", policy)

    def write_manifest_copy(
        self, name: str, edit: ManifestEdit, labels: dict[str, Any] | None
    ) -> Path:
        """Copy the staged manifest under `fixtures/decisions/<name>`; threads stay in place."""
        target = self.fixtures_root / "decisions" / name
        target.mkdir(parents=True)
        manifest = load_json_object(self.staged_manifest)
        for entry in manifest["threads"]:
            entry["path"] = relocate(entry["path"], self.staged_dataset, target)
        if labels is None:
            source = manifest["labels"]["path"]
            manifest["labels"]["path"] = relocate(source, self.staged_dataset, target)
        else:
            labels_path = write_json_file(target / "labels.json", labels)
            manifest["labels"] = {"path": "labels.json", "sha256": file_sha256(labels_path)}
        edit(manifest)
        return write_json_file(target / "manifest.json", manifest)

    def source_git_status(self) -> str:
        completed = run_process(
            ["git", "-C", str(self.repo_root), "status", "--porcelain"],
            capture_output=True,
            text=True,
            check=False,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        check(completed.returncode == 0, f"git status failed: {completed.stderr[-500:]}")
        return completed.stdout

    # --- 1. staging ---

    def dataset_sources(self) -> list[Path]:
        dataset = self.repo_root / DATASET_RELPATH
        return [
            dataset / "manifest.json",
            dataset / "labels.json",
            *sorted((dataset / "threads").glob("*.json")),
            self.repo_root / ANALYSIS_THREAD_RELPATH,
        ]

    def stage_dataset(self) -> None:
        fixtures = self.repo_root / FIXTURES_RELPATH
        for source in self.dataset_sources():
            target = self.fixtures_root / source.relative_to(fixtures)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
            digest = file_sha256(target)
            check(digest == file_sha256(source), f"staged copy of {source.name} differs")
            self.staged_hashes[self.relative(target)] = digest
            self.source_hashes[str(source.relative_to(self.repo_root))] = digest
        self.load_staged_corpus()
        packaged = json.loads(
            resources.files("threadsense.decisions")
            .joinpath("laya_runtime_manifest.json")
            .read_bytes()
        )
        self.pinned_checkpoint_files = packaged["checkpoint"]["files"]
        self.pinned_manifest_sha256 = load_pinned_runtime_manifest().sha256()
        label_counts = self.held_out_label_counts()
        check(
            label_counts.get(POLICY_QUESTION, 0) > 0,
            f"policy question {POLICY_QUESTION} has no held-out labels: {label_counts}",
        )
        check(
            label_counts.get(MISSING_LABELS_QUESTION, 0) == 0,
            f"question {MISSING_LABELS_QUESTION} has held-out labels: {label_counts}",
        )
        self.record(
            "stage_dataset",
            files=self.staged_hashes,
            partitions=self.partitions,
            comment_counts={key: len(value) for key, value in self.comments.items()},
            oversized_comments=self.oversized,
            boundary_comments=self.boundary,
            declared_non_english=self.declared_excluded,
            held_out_label_counts=label_counts,
            policy_question=POLICY_QUESTION,
            missing_labels_question=MISSING_LABELS_QUESTION,
            fixture_tolerances=dict(FIXTURE_TOLERANCES),
            fixture_tolerance_disclosure=FIXTURE_TOLERANCE_DISCLOSURE,
            pinned_manifest_sha256=self.pinned_manifest_sha256,
        )

    def load_staged_corpus(self) -> None:
        manifest = load_json_object(self.staged_manifest)
        labels_path = self.staged_dataset / manifest["labels"]["path"]
        check(file_sha256(labels_path) == manifest["labels"]["sha256"], "staged labels sha256")
        supported = set(manifest["supported_languages"])
        for entry in manifest["threads"]:
            thread_id = entry["thread_id"]
            path = (self.staged_dataset / entry["path"]).resolve()
            check(path.is_relative_to(self.fixtures_root), f"{thread_id}: path leaves fixtures")
            check(file_sha256(path) == entry["sha256"], f"{thread_id}: staged snapshot sha256")
            self.thread_inputs[thread_id] = path
            self.partitions[thread_id] = entry["partition"]
            comments = load_json_object(path)["thread"]["comments"]
            self.comments[thread_id] = {c["comment_id"]: c["body"] for c in comments}
            self.declared_excluded.extend(
                [thread_id, comment_id, language]
                for comment_id, language in sorted(entry["comment_languages"].items())
                if language not in supported
            )
        self.declared_excluded.sort()
        self.oversized = sorted(
            [thread_id, comment_id]
            for thread_id, bodies in self.comments.items()
            for comment_id, body in bodies.items()
            if len(body) > CLIENT_COMMENT_BUDGET
        )
        check(len(self.oversized) == 1, f"expected one oversized comment: {self.oversized}")
        self.boundary = sorted(
            [thread_id, comment_id]
            for thread_id, bodies in self.comments.items()
            for comment_id, body in bodies.items()
            if len(body) == CLIENT_COMMENT_BUDGET
        )
        check(len(self.boundary) == 1, f"expected one at-budget comment: {self.boundary}")
        check(bool(self.declared_excluded), "no declared non-English comments")

    def held_out_label_counts(self) -> dict[str, int]:
        labels = load_json_object(self.staged_dataset / "labels.json")["labels"]
        counts: dict[str, int] = {}
        for label in labels:
            if self.partitions[label["thread_id"]] == HELD_OUT_PARTITION:
                counts[label["question_id"]] = counts.get(label["question_id"], 0) + 1
        return dict(sorted(counts.items()))

    # --- 2-4. real annotations ---

    def real_annotations(self) -> None:
        per_thread: dict[str, Any] = {}
        for thread_id, path in self.thread_inputs.items():
            code, payload = self.annotate_thread(path, self.endpoint, self.annotations_dir)
            check(code == 0, f"{thread_id}: annotate exited {code}: {payload}")
            decision_path = self.published_path(payload["decision_path"])
            check(file_sha256(decision_path) == payload["decision_sha256"], "decision file sha")
            decision = load_json_object(decision_path)
            self.check_verification(thread_id, payload, decision)
            thread_refused = self.check_real_items(thread_id, decision)
            for comment_id, reasons in thread_refused.items():
                self.refused_items[(thread_id, comment_id)] = reasons
            run = load_json_object(self.published_path(payload["run_path"]))
            self.annotations[thread_id] = decision_path
            self.annotation_payloads[thread_id] = payload
            self.annotation_hashes[self.relative(decision_path)] = payload["decision_sha256"]
            per_thread[thread_id] = {
                "decision_path": self.relative(decision_path),
                "decision_sha256": payload["decision_sha256"],
                "run_id": payload["run_id"],
                "coverage": payload["coverage"],
                "refused_comment_ids": thread_refused,
                "total_http_wall_seconds": run["total_http_wall_seconds"],
            }
        refused = sorted([list(key) for key in self.refused_items])
        client_refused = [key for key in refused if key in self.oversized]
        check(client_refused == self.oversized, f"refused {refused} lacks oversized")
        server_refused = [key for key in refused if key not in self.oversized]
        check(
            all(key in self.boundary for key in server_refused),
            f"server refusals {server_refused} outside the at-budget comments {self.boundary}",
        )
        boundary_observed = [
            {
                "thread_id": t,
                "comment_id": c,
                "characters": CLIENT_COMMENT_BUDGET,
                "status": "refused" if (t, c) in self.refused_items else "ok",
                "reasons": self.refused_items.get((t, c), []),
            }
            for t, c in self.boundary
        ]
        self.record(
            "real_annotations",
            threads=per_thread,
            refused=[
                {"thread_id": t, "comment_id": c, "reasons": self.refused_items[(t, c)]}
                for t, c in refused
            ],
            client_refused=client_refused,
            client_refused_reason=OVERSIZED_REASON,
            server_refused=server_refused,
            boundary_comments=boundary_observed,
            boundary_note=BOUNDARY_NOTE,
            checkpoint_basis=CHECKPOINT_BASIS,
            manifest_sha256=self.pinned_manifest_sha256,
            answers_label=LABEL_NO_QUALITY_CLAIM,
            wall_time_label=LABEL_RESOURCE,
        )

    def check_verification(
        self, thread_id: str, payload: dict[str, Any], decision: dict[str, Any]
    ) -> None:
        backend = decision["backend"]
        check(
            payload["manifest_sha256"] == self.pinned_manifest_sha256
            and backend["manifest_sha256"] == self.pinned_manifest_sha256,
            f"{thread_id}: runtime manifest sha256 differs from the pinned manifest",
        )
        verification = backend["checkpoint_verification"]
        check(verification is not None, f"{thread_id}: checkpoint_verification missing")
        check(verification["basis"] == CHECKPOINT_BASIS, f"{thread_id}: basis {verification}")
        check(
            verification["files"] == self.pinned_checkpoint_files,
            f"{thread_id}: verified checkpoint files differ from the pinned manifest",
        )

    def check_real_items(self, thread_id: str, decision: dict[str, Any]) -> dict[str, list[str]]:
        bodies = self.comments[thread_id]
        items = decision["items"]
        check(
            sorted(item["comment_id"] for item in items) == sorted(bodies),
            f"{thread_id}: item comment ids differ from the staged thread",
        )
        refused: dict[str, list[str]] = {}
        for item in items:
            comment_id = item["comment_id"]
            observed = (item["status"], item["reasons"])
            if len(bodies[comment_id]) > CLIENT_COMMENT_BUDGET:
                check(
                    observed == ("refused", [OVERSIZED_REASON]),
                    f"{comment_id}: oversized comment {observed}",
                )
                refused[comment_id] = item["reasons"]
            elif item["status"] == "refused":
                check(
                    [thread_id, comment_id] in self.boundary,
                    f"{comment_id}: within-budget comment below the boundary refused {observed}",
                )
                check(
                    bool(item["reasons"]) and set(item["reasons"]) <= SERVER_REFUSAL_REASONS,
                    f"{comment_id}: at-budget comment refused without server usage reasons "
                    f"{observed}",
                )
                refused[comment_id] = item["reasons"]
            else:
                check(item["status"] == "ok", f"{comment_id}: status {observed}")
        coverage = decision["coverage"]
        check(
            (coverage["total"], coverage["ok"], coverage["refused"], coverage["unavailable"])
            == (len(items), len(items) - len(refused), len(refused), 0),
            f"{thread_id}: coverage {coverage}",
        )
        return refused

    def annotation_replay_deterministic(self) -> None:
        thread_id = next(iter(self.thread_inputs))
        first = self.annotation_payloads[thread_id]
        code, payload = self.annotate_thread(
            self.thread_inputs[thread_id], self.endpoint, self.annotations_dir
        )
        check(code == 0, f"replay exited {code}: {payload}")
        check(payload["decision_sha256"] == first["decision_sha256"], "replay decision sha256")
        check(payload["decision_path"] == first["decision_path"], "replay decision path")
        check(payload["run_id"] != first["run_id"], "replay reused the first run id")
        self.record(
            "annotation_replay_deterministic",
            thread_id=thread_id,
            decision_sha256=payload["decision_sha256"],
            first_run_id=first["run_id"],
            second_run_id=payload["run_id"],
        )

    def server_memory_observation(self) -> None:
        super().server_memory_observation()
        self.evidence[-1]["observed"]["observation_label"] = LABEL_RESOURCE

    # --- 5-7. evaluation ---

    def checked_evaluation(
        self, code: int, payload: dict[str, Any], label_origin: str
    ) -> tuple[Path, dict[str, Any], dict[str, Any]]:
        check(code == 0, f"evaluate exited {code}: {payload}")
        check(
            (payload["artifact_type"], payload["status"], payload["mode"])
            == ("decision_evaluation", "ready", "shadow"),
            f"evaluate payload {payload}",
        )
        path = self.published_path(payload["evaluation_path"])
        digest = payload["evaluation_sha256"]
        check(file_sha256(path) == digest and path.stem == digest, "evaluation not addressed")
        document = load_json_object(path)
        claims = document["claims"]
        check(
            (claims["label_origin"], claims["independent_adjudication"]) == (label_origin, False)
            and claims["population_accuracy"] == "not_claimed"
            and document["mode"] == "shadow",
            f"evaluation claims {claims}",
        )
        run = load_json_object(self.published_path(payload["run_path"]))
        check(run["evaluation_sha256"] == digest, "run record names another evaluation")
        return path, document, run

    def reconcile_partition(self, name: str, document: dict[str, Any]) -> None:
        block = document["partitions"][name]
        population = block["population"]
        audit = document["audit"]
        threads = [thread for thread, partition in self.partitions.items() if partition == name]
        expected = sum(len(self.comments[thread]) for thread in threads)
        check(population["comments"] == expected, f"{name}: comments {population}")
        check(
            population["comments"]
            == population["ok"] + population["refused"] + population["unavailable"]
            and population["eligible"] == population["ok"] - population["excluded_language"],
            f"{name}: population does not reconcile {population}",
        )
        rejected = [entry for entry in audit["rejected"] if entry["partition"] == name]
        excluded = [entry for entry in audit["excluded"] if entry["partition"] == name]
        check(
            len(rejected) == population["refused"] + population["unavailable"]
            and len(excluded) == population["excluded_language"],
            f"{name}: rejected/excluded audit does not reconcile with {population}",
        )
        abstained = [
            entry["question_id"] for entry in audit["abstained"] if entry["partition"] == name
        ]
        for question_id, metrics in block["questions"].items():
            check(
                abstained.count(question_id) == metrics["abstained"]
                and metrics["decided"] + metrics["abstained"] == metrics["eligible"],
                f"{name}/{question_id}: abstained audit does not reconcile",
            )

    def reconcile_evaluation(self, document: dict[str, Any]) -> dict[str, Any]:
        check(list(document["partitions"]) == list(PARTITIONS), "partition names")
        check(document["partition_integrity"]["status"] == "passed", "partition integrity")
        for name in PARTITIONS:
            self.reconcile_partition(name, document)
        replays = [annotation["replay"] for annotation in document["annotations"]]
        check(
            len(replays) == len(self.thread_inputs)
            and all(
                replay["status"] == "reproduced"
                and replay["matched_items"] == replay["replayed_items"]
                for replay in replays
            ),
            f"annotation replay not reproduced: {replays}",
        )
        audit = document["audit"]
        return {
            "rejected": sorted(
                (
                    {
                        "thread_id": entry["thread_id"],
                        "comment_id": entry["comment_id"],
                        "status": entry["status"],
                        "reasons": entry["reasons"],
                    }
                    for entry in audit["rejected"]
                ),
                key=lambda entry: (entry["thread_id"], entry["comment_id"]),
            ),
            "excluded": sorted(
                [entry["thread_id"], entry["comment_id"], entry["language"]]
                for entry in audit["excluded"]
            ),
            "abstained": sorted(
                [entry["partition"], entry["question_id"], entry["thread_id"], entry["comment_id"]]
                for entry in audit["abstained"]
            ),
            "replayed_annotations": len(replays),
        }

    def reconcile_held_out(
        self, policy_document: dict[str, Any], evaluation: dict[str, Any], question_id: str
    ) -> dict[str, Any]:
        """The policy check's held-out view must equal the evaluation partition it read."""
        held_out = policy_document["held_out"]
        check(held_out is not None, "policy check has no held-out block")
        check(
            (held_out["partition"], held_out["question_id"]) == (HELD_OUT_PARTITION, question_id),
            f"held-out block {held_out['partition']}/{held_out['question_id']}",
        )
        metrics = evaluation["partitions"][HELD_OUT_PARTITION]["questions"][question_id]
        observed = {key: held_out[key] for key in ("eligible", "abstained", "decided")}
        observed["decision_rate"] = held_out["decision_rate"]
        observed["labeled_decided"] = held_out["labeled_decided"]
        observed["error_rate"] = held_out["error_rate"]
        counts = held_out_counts(metrics)
        expected = {key: counts[key] for key in observed}
        check(observed == expected, f"held-out metrics {observed} != evaluation {expected}")
        audit = evaluation["audit"]
        in_partition = {
            kind: [entry for entry in audit[kind] if entry["partition"] == HELD_OUT_PARTITION]
            for kind in AUDIT_KINDS
        }
        in_partition["abstained"] = [
            entry for entry in in_partition["abstained"] if entry["question_id"] == question_id
        ]
        expected_refs = {kind: audit_refs(in_partition[kind]) for kind in AUDIT_KINDS}
        observed_refs = {kind: audit_refs(held_out["audit"][kind]) for kind in AUDIT_KINDS}
        check(observed_refs == expected_refs, "held-out audit ids differ from the evaluation")
        return {"metrics": observed, "population": held_out["population"], "audit": observed_refs}

    def real_evaluation(self) -> None:
        annotations = list(self.annotations.values())
        code, payload = self.evaluate(self.staged_manifest, annotations, configured_runtime=True)
        path, document, run = self.checked_evaluation(code, payload, "engineering_authored")
        summary = self.reconcile_evaluation(document)
        rejected = {
            (entry["thread_id"], entry["comment_id"]): entry["reasons"]
            for entry in summary["rejected"]
        }
        check(
            rejected == self.refused_items,
            f"rejected audit {summary['rejected']} != refused {self.refused_items}",
        )
        check(summary["excluded"] == self.declared_excluded, f"excluded {summary['excluded']}")
        generative = run["generative_comparator"]
        check(
            payload["generative_comparator"]
            == {"status": generative["status"], "reason": generative["reason"]},
            "payload and run record disagree on generative availability",
        )
        comparison = document["comparators"]["generative"]
        check(comparison["comparison"] == "not_performed", f"generative {comparison}")
        self.engineering_path, self.engineering_document = path, document
        self.engineering_payload = payload
        self.observations["generative_comparator_availability"] = generative
        self.record(
            "real_evaluation",
            evaluation_path=self.relative(path),
            evaluation_sha256=payload["evaluation_sha256"],
            run_id=payload["run_id"],
            manifest_sha256=payload["manifest_sha256"],
            label_origin=payload["label_origin"],
            independent_adjudication=payload["independent_adjudication"],
            population_accuracy=document["claims"]["population_accuracy"],
            partitions=payload["partitions"],
            audit=summary,
            runtime_switch="read from the source threadsense.toml; not forced by the smoke",
            generative_comparator_availability=generative,
            generative_comparator=comparison,
            generative_note=GENERATIVE_NOTE,
            wall_seconds=run["wall_seconds"],
            wall_time_label=LABEL_RESOURCE,
            label_disclosure=LABEL_ORIGIN_DISCLOSURE,
        )

    def evaluation_replay(self) -> None:
        first = self.engineering_payload
        annotations = list(self.annotations.values())
        code, payload = self.evaluate(self.staged_manifest, annotations, configured_runtime=True)
        check(code == 0, f"evaluation replay exited {code}: {payload}")
        check(payload["evaluation_sha256"] == first["evaluation_sha256"], "replay sha256")
        check(payload["evaluation_path"] == first["evaluation_path"], "replay path")
        check(payload["run_id"] != first["run_id"], "replay reused the run id")
        self.record(
            "evaluation_replay",
            evaluation_sha256=payload["evaluation_sha256"],
            first_run_id=first["run_id"],
            second_run_id=payload["run_id"],
        )

    def evaluation_refusal(self, code: int, payload: dict[str, Any], reason: str) -> dict[str, Any]:
        check(code == 1, f"expected exit 1 for {reason}, got {code}: {payload}")
        error = payload.get("error", {})
        details: dict[str, Any] = error.get("details", {})
        check(
            (payload.get("status"), error.get("code"), details.get("reason"))
            == ("error", "decision_evaluation_error", reason),
            f"expected {reason}: {payload}",
        )
        return details

    def evaluation_refusals(self) -> None:
        def stale_question_set(manifest: dict[str, Any]) -> None:
            manifest["bindings"]["question_set_sha256"] = STALE_SHA256

        stale = self.write_manifest_copy("stale-question-binding", stale_question_set, None)
        annotations = list(self.annotations.values())
        code, payload = self.evaluate(stale, annotations)
        details = self.evaluation_refusal(code, payload, "stale_binding")
        check(details.get("field") == "question_set_sha256", f"stale field {details}")
        omitted = next(iter(self.annotations))
        supplied = [path for thread, path in self.annotations.items() if thread != omitted]
        code, payload = self.evaluate(self.staged_manifest, supplied)
        missing = self.evaluation_refusal(code, payload, "annotation_missing")
        check(missing.get("thread_ids") == [omitted], f"missing thread ids {missing}")
        self.record(
            "evaluation_refusals",
            stale_binding={"field": details["field"], "manifest": self.relative(stale)},
            annotation_missing={"thread_ids": missing["thread_ids"]},
        )

    # --- 8-11. policy checks ---

    def policy_summary(self, payload: dict[str, Any], document: dict[str, Any]) -> dict[str, Any]:
        return {
            "policy_check_path": self.relative(self.published_path(payload["policy_check_path"])),
            "policy_check_sha256": payload["policy_check_sha256"],
            "eligibility": payload["eligibility"],
            "scope": payload["scope"],
            "activation_permitted": payload["activation_permitted"],
            "activation_performed": document["activation"]["performed"],
            "reasons": payload["reasons"],
            "checks": {entry["name"]: entry["status"] for entry in document["checks"]},
        }

    def real_policy_not_eligible(self) -> None:
        payload, document = self.policy_outcome(self.engineering_path, None)
        required = {"policy_not_declared", "held_out_labels_not_independent"}
        check(
            payload["eligibility"] == "not_eligible"
            and required <= set(payload["reasons"])
            and payload["activation_permitted"] is False,
            f"real policy check without a policy {payload}",
        )
        repeat, _ = self.policy_outcome(self.engineering_path, None)
        check(
            (repeat["policy_check_sha256"], repeat["policy_check_path"])
            == (payload["policy_check_sha256"], payload["policy_check_path"]),
            "repeated policy check was not reproduced",
        )
        bindings = evidence_bindings(self.engineering_document)
        runtime_policy = activation_policy("runtime", POLICY_QUESTION, bindings, FIXTURE_TOLERANCES)
        runtime_path = self.write_policy("runtime-engineering", runtime_policy)
        bound, bound_document = self.policy_outcome(self.engineering_path, runtime_path)
        check(
            bound["eligibility"] == "not_eligible"
            and "held_out_labels_not_independent" in bound["reasons"]
            and bound["activation_permitted"] is False,
            f"runtime policy over engineering labels {bound}",
        )
        held_out = self.reconcile_held_out(
            bound_document, self.engineering_document, POLICY_QUESTION
        )
        self.record(
            "real_policy_not_eligible",
            without_policy=self.policy_summary(payload, document),
            without_policy_reproduced=True,
            runtime_policy=self.policy_summary(bound, bound_document),
            runtime_policy_path=self.relative(runtime_path),
            runtime_policy_tolerances=runtime_policy["tolerances"],
            held_out=held_out,
            outcome_note=REAL_OUTCOME_NOTE,
        )

    def constructed_evidence(self) -> tuple[Path, dict[str, Any]]:
        labels = load_json_object(self.staged_dataset / "labels.json")
        constructed = constructed_labels(labels)
        check(constructed["labels"] == labels["labels"], "constructed labels changed a value")

        def rename(manifest: dict[str, Any]) -> None:
            manifest["dataset_id"] = f"{manifest['dataset_id']}{CONSTRUCTED_DATASET_SUFFIX}"

        self.constructed_manifest = self.write_manifest_copy(
            "constructed-test-only", rename, constructed
        )
        annotations = list(self.annotations.values())
        code, payload = self.evaluate(self.constructed_manifest, annotations)
        path, document, _run = self.checked_evaluation(code, payload, "constructed_test_fixture")
        for key in ("identity", "partitions", "audit"):
            check(
                document[key] == self.engineering_document[key],
                f"constructed evidence {key} differs from the engineering evaluation",
            )
        return path, document

    def test_only_eligible(self) -> None:
        path, document = self.constructed_evidence()
        metrics = document["partitions"][HELD_OUT_PARTITION]["questions"][POLICY_QUESTION]
        counts = held_out_counts(metrics)
        check(
            counts["labeled_decided"] >= 1 and counts["error_rate"] is not None,
            f"predeclared question {POLICY_QUESTION} has no decided held-out labels: {counts}",
        )
        self.constructed_path, self.constructed_document = path, document
        bindings = evidence_bindings(document)
        self.base_policy = activation_policy(
            "test_only", POLICY_QUESTION, bindings, FIXTURE_TOLERANCES
        )
        policy_path = self.write_policy("test-only-constructed", self.base_policy)
        payload, policy_document = self.policy_outcome(path, policy_path)
        check(
            (payload["eligibility"], payload["scope"], payload["reasons"])
            == ("eligible", "test_only", [])
            and payload["activation_permitted"] is False
            and policy_document["activation"]["permitted"] is False,
            f"test-only policy check {payload}",
        )
        failing = [entry for entry in policy_document["checks"] if entry["status"] != "passed"]
        check(not failing, f"eligible check has non-passed checks {failing}")
        held_out = self.reconcile_held_out(policy_document, document, POLICY_QUESTION)
        self.record(
            "test_only_eligible",
            evaluation_path=self.relative(path),
            evaluation_sha256=path.stem,
            manifest=self.relative(self.constructed_manifest),
            dataset_id=document["dataset"]["dataset_id"],
            label_origin=document["dataset"]["labels"]["origin"],
            label_values_unchanged=True,
            policy_path=self.relative(policy_path),
            policy=self.base_policy,
            policy_check=self.policy_summary(payload, policy_document),
            held_out=held_out,
            observed_held_out=counts,
            tolerance_disclosure=FIXTURE_TOLERANCE_DISCLOSURE,
        )

    def invalidation_evidence(self, kind: str) -> Path:
        if kind == "constructed":
            return self.constructed_path
        if kind == "engineering":
            return self.engineering_path
        check(kind == "missing", f"unknown invalidation evidence {kind}")
        missing = self.policies_dir / "absent" / f"{STALE_SHA256}.json"
        check(not missing.exists(), "missing evidence path exists")
        return missing

    def run_invalidation(self, case: InvalidationCase) -> dict[str, Any]:
        entry: dict[str, Any] = {"case": case.name, "field": case.target}
        if case.expected is None:
            return {
                **entry,
                "status": "not_applicable",
                "vacuous": True,
                "observed_value": case.observed_value,
            }
        policy_path = self.write_policy(f"invalidation-{case.name}", case.policy)
        evaluation = self.invalidation_evidence(case.evidence)
        payload, document = self.policy_outcome(evaluation, policy_path)
        checks = {item["name"]: item["status"] for item in document["checks"]}
        not_run_violations = (
            [name for name in EVIDENCE_DEPENDENT_CHECKS if checks.get(name) != "not_run"]
            if case.evidence == "missing"
            else []
        )
        return {
            **entry,
            "status": "checked",
            "evidence": case.evidence,
            "expected": list(case.expected),
            "observed": payload["reasons"],
            "eligibility": payload["eligibility"],
            "activation_permitted": payload["activation_permitted"],
            "checks": checks,
            "not_run_violations": not_run_violations,
            "observed_value": case.observed_value,
        }

    def paired_invalidations(self) -> None:
        held_out = self.constructed_document["partitions"][HELD_OUT_PARTITION]["questions"][
            POLICY_QUESTION
        ]
        engineering_policy = activation_policy(
            "test_only",
            POLICY_QUESTION,
            evidence_bindings(self.engineering_document),
            FIXTURE_TOLERANCES,
        )
        cases = invalidation_cases(self.base_policy, engineering_policy, held_out)
        results = [self.run_invalidation(case) for case in cases]
        mismatches = [
            result
            for result in results
            if result["status"] == "checked"
            and (
                result["observed"] != result["expected"]
                or result["eligibility"] != "not_eligible"
                or result["activation_permitted"] is not False
                or result["not_run_violations"]
            )
        ]
        check(not mismatches, f"invalidations did not match: {mismatches}")
        self.record(
            "paired_invalidations",
            base_policy_path=self.relative(self.policies_dir / "test-only-constructed.json"),
            cases=results,
            invalidation_values={
                "max_error_rate": INVALID_MAX_ERROR_RATE,
                "min_decision_rate": INVALID_MIN_DECISION_RATE,
                "min_labeled_decided": INVALID_MIN_LABELED_DECIDED,
                "min_decision_rate_out_of_range": OUT_OF_RANGE_MIN_DECISION_RATE,
            },
            not_applicable_note="a fixed invalidation the observed metric already satisfies is "
            "recorded as vacuous; no threshold is derived from observations",
        )

    def check_unavailable_items(self, thread_id: str, decision: dict[str, Any]) -> dict[str, Any]:
        statuses: dict[str, Any] = {}
        for item in decision["items"]:
            comment_id = item["comment_id"]
            observed = (item["status"], item["reasons"])
            allowed = [("unavailable", [UNAVAILABLE_REASON])]
            if [thread_id, comment_id] in self.oversized:
                allowed.append(("refused", [OVERSIZED_REASON]))
            check(observed in allowed, f"{comment_id}: {observed}")
            check(item["answers"] == [], f"{comment_id}: answers present")
            statuses[comment_id] = {"status": item["status"], "reasons": item["reasons"]}
        return statuses

    def unavailable_annotations(self) -> tuple[dict[str, Path], dict[str, Any]]:
        endpoint = f"http://127.0.0.1:{closed_loopback_port()}"
        held_out = [
            thread for thread, name in self.partitions.items() if name == HELD_OUT_PARTITION
        ]
        paths: dict[str, Path] = {}
        statuses: dict[str, Any] = {}
        for thread_id in held_out:
            code, payload = self.annotate_thread(
                self.thread_inputs[thread_id], endpoint, self.unavailable_dir
            )
            check(code == 0 and payload["satisfied"] is True, f"{thread_id}: {payload}")
            path = self.published_path(payload["decision_path"])
            statuses[thread_id] = self.check_unavailable_items(thread_id, load_json_object(path))
            paths[thread_id] = path
        return paths, statuses

    def unsupported_coverage(self) -> None:
        unavailable, statuses = self.unavailable_annotations()
        mixed = [unavailable.get(thread, path) for thread, path in self.annotations.items()]
        code, payload = self.evaluate(self.constructed_manifest, mixed)
        path, document, _run = self.checked_evaluation(code, payload, "constructed_test_fixture")
        summary = self.reconcile_evaluation(document)
        population = document["partitions"][HELD_OUT_PARTITION]["population"]
        check((population["ok"], population["eligible"]) == (0, 0), f"held out {population}")
        policy = activation_policy(
            "test_only", POLICY_QUESTION, evidence_bindings(document), FIXTURE_TOLERANCES
        )
        check(policy == self.base_policy, "unsupported-coverage evidence has other bindings")
        policy_path = self.write_policy("test-only-unsupported-coverage", policy)
        outcome, policy_document = self.policy_outcome(path, policy_path)
        codes = outcome["reasons"]
        check(
            outcome["eligibility"] == "not_eligible"
            and "coverage_unsupported" in codes
            and set(codes) <= COVERAGE_UNSUPPORTED_ALLOWED
            and outcome["activation_permitted"] is False,
            f"unsupported coverage policy check {outcome}",
        )
        self.record(
            "unsupported_coverage",
            closed_loopback_threads=sorted(unavailable),
            item_statuses=statuses,
            evaluation_path=self.relative(path),
            evaluation_sha256=payload["evaluation_sha256"],
            held_out_population=population,
            audit=summary,
            policy_check=self.policy_summary(outcome, policy_document),
            allowed_codes=sorted(COVERAGE_UNSUPPORTED_ALLOWED),
        )

    # --- 12. nothing installed, inputs unchanged ---

    def no_policy_installed_inputs_unchanged(self) -> None:
        for relative, digest in self.staged_hashes.items():
            check(file_sha256(self.work_dir / relative) == digest, f"staged {relative} changed")
        for relative, digest in self.annotation_hashes.items():
            check(file_sha256(self.work_dir / relative) == digest, f"annotation {relative} changed")
        for relative, digest in self.source_hashes.items():
            check(file_sha256(self.repo_root / relative) == digest, f"source {relative} changed")
        files = sorted(self.relative(path) for path in self.work_dir.rglob("*") if path.is_file())
        stray = [relative for relative in files if Path(relative).parts[0] not in OUTPUT_ROOTS]
        check(not stray, f"files outside the declared output roots: {stray}")
        package = source_root() / "threadsense"
        installed = sorted(
            str(path.relative_to(package))
            for path in package.rglob("*.json")
            if POLICY_KIND in path.read_text(encoding="utf-8")
        )
        check(not installed, f"activation policy files inside the package: {installed}")
        git_status_after = self.source_git_status()
        check(git_status_after == self.git_status_before, "source checkout git status changed")
        checks_dir = self.store_root / "annotations" / POLICY_CHECKS_DIRNAME
        self.record(
            "no_policy_installed_inputs_unchanged",
            staged_files_unchanged=len(self.staged_hashes),
            annotation_files_unchanged=len(self.annotation_hashes),
            source_fixture_files_unchanged=len(self.source_hashes),
            output_roots=sorted(OUTPUT_ROOTS),
            work_dir_files=len(files),
            policy_checks=sorted(path.name for path in checks_dir.glob("*.json")),
            package_policy_files=installed,
            git_status_unchanged=True,
        )
