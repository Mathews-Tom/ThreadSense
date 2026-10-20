"""Typed-decision annotation smoke scenario exercised through the real `threadsense` CLI.

Driven by `scripts/smoke_monitoring.py --scenario annotations --endpoint URL`. The endpoint
is a locally running `laya.serve` with the pinned checkpoint already loaded; this module
never starts or stops it. Every product step runs `python -m threadsense --output-format
json ...` in a fresh interpreter with a scrubbed environment and isolated storage.

The source data is a committed fixture, not live acquisition. The annotations are
agent-authored-fixture outputs of an uncalibrated shadow model: they are not independent
human labels and support no quality claim.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import threadsense

FIXTURE_RELPATH = "tests/fixtures/analysis/canonical_feedback_thread.json"
FIXTURE_PROVENANCE = f"{FIXTURE_RELPATH}, saved canonical fixture with 7 short comments"
SCENARIO_SCOPE = (
    "real local checkpoint through the shipped CLI; "
    "source data is a committed fixture, no live source"
)
LABEL_NO_QUALITY_CLAIM = "agent-authored fixture, not independent human labels; no quality claim"
LABEL_HTTP_WALL = "client-observed HTTP wall time"
LABEL_RSS = "resident set size at observation time after inference; not peak memory"

COMMENT_COUNT = 7
DOMAIN = "developer_tools"
OBJECTIVE = "friction_analysis"
EXPECTED_QUESTION_IDS = (
    "objective_relevance",
    "evidence_type",
    "request_intent",
    "actionability",
    "promotional_framing",
    "theme",
)
ANNOTATE_TIMEOUT_SECONDS = 900
DEFAULT_TIMEOUT_SECONDS = 120
CHECKPOINT_BASIS = "client_file_sha256"

CORE_PROBE = """\
import importlib.util
import json
import sys

import threadsense
import threadsense.cli
import threadsense.decisions.annotate

heavy = ("torch", "transformers", "numpy", "fastapi")
print(
    json.dumps(
        {
            "torch_spec_none": importlib.util.find_spec("torch") is None,
            "transformers_spec_none": importlib.util.find_spec("transformers") is None,
            "heavy_loaded": [name for name in heavy if name in sys.modules],
            "threadsense_file": threadsense.__file__,
        }
    )
)
"""


class SmokeFailure(Exception):
    pass


def run_process(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(argv, **kwargs)
    except (subprocess.TimeoutExpired, OSError) as error:
        raise SmokeFailure(f"process {argv[0]} failed: {error}") from error


def check(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeFailure(message)


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def text_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_sha256(value: Any) -> str:
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return text_sha256(text)


def reject_constant(token: str) -> Any:
    raise ValueError(f"non-standard JSON constant {token}")


def decode_payload(stdout: str, stderr: str, command: str) -> dict[str, Any]:
    try:
        payload: dict[str, Any] = json.loads(stdout, parse_constant=reject_constant)
    except ValueError as error:
        raise SmokeFailure(f"threadsense {command} printed no JSON: {stderr[-500:]}") from error
    return payload


def source_root() -> Path:
    return Path(threadsense.__file__).resolve().parents[1]


_LOCAL_PATH = re.compile(r"/(?:Users|private|var|tmp|home)/[^\s'\",}\]]*")


def redact_local_paths(text: str) -> str:
    return _LOCAL_PATH.sub("<local-path>", text)


def child_base_env() -> dict[str, str]:
    """Scrubbed child environment; children import the same sources as this script."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("THREADSENSE_") and key != "PYTHONPATH"
    }
    env["PYTHONPATH"] = str(source_root())
    env["THREADSENSE_RUNTIME_ENABLED"] = "false"
    return env


def closed_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
    return port


def endpoint_port(endpoint: str) -> int:
    port = urlsplit(endpoint).port
    if port is None:
        raise SmokeFailure(f"endpoint has no explicit port: {endpoint}")
    return port


def load_json_object(path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return payload


class AnnotationsScenario:
    def __init__(self, work_dir: Path, endpoint: str) -> None:
        self.work_dir = work_dir
        self.endpoint = endpoint
        self.repo_root = Path(__file__).resolve().parent.parent
        self.store_root = work_dir / "store"
        self.base_env = child_base_env()
        self.evidence: list[dict[str, Any]] = []
        self.observations: dict[str, Any] = {
            "endpoint": endpoint,
            "fixture_provenance": FIXTURE_PROVENANCE,
            "scenario_scope": SCENARIO_SCOPE,
        }
        self.staged_input = work_dir / "input" / "canonical_feedback_thread.json"
        self.staged_sha = ""
        self.baseline_path = work_dir / "baseline" / "analysis.json"
        self.baseline_sha = ""
        self.baseline_findings_digest = ""
        self.annotations_dir = work_dir / "annotations"
        self.first_decision_sha = ""
        self.first_decision_path = ""
        self.first_run_id = ""

    def record(self, step: str, **observed: Any) -> None:
        self.evidence.append({"step": step, "observed": observed})
        print(f"step={step} ok")

    def cli(self, *args: str, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> tuple[int, dict[str, Any]]:
        env = {**self.base_env, "THREADSENSE_STORAGE_ROOT": str(self.store_root)}
        completed = run_process(
            [sys.executable, "-m", "threadsense", "--output-format", "json", *args],
            capture_output=True,
            text=True,
            check=False,
            cwd=self.work_dir,
            env=env,
            timeout=timeout,
        )
        return completed.returncode, decode_payload(
            completed.stdout, completed.stderr, " ".join(args)
        )

    def annotate(
        self, endpoint: str, requirement: str, output_dir: Path
    ) -> tuple[int, dict[str, Any]]:
        return self.cli(
            *("decisions", "annotate", "--input", str(self.staged_input)),
            *("--endpoint", endpoint, "--requirement", requirement),
            *("--output-dir", str(output_dir)),
            *("--domain", DOMAIN, "--objective", OBJECTIVE),
            timeout=ANNOTATE_TIMEOUT_SECONDS,
        )

    def relative(self, path: Path) -> str:
        return str(path.resolve().relative_to(self.work_dir))

    def published_path(self, raw: str) -> Path:
        path = Path(raw)
        return path if path.is_absolute() else self.work_dir / path

    def run(self) -> None:
        self.core_without_model()
        self.stage_input()
        self.baseline_analysis()
        self.required_real_annotation()
        self.server_memory_observation()
        self.deterministic_replay()
        self.optional_unavailable()
        self.required_unavailable()
        self.refused_output_location()
        self.refused_endpoint()
        self.input_and_baseline_unchanged()

    def core_without_model(self) -> None:
        completed = run_process(
            [sys.executable, "-c", CORE_PROBE],
            capture_output=True,
            text=True,
            check=False,
            cwd=self.work_dir,
            env=self.base_env,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        check(completed.returncode == 0, f"core probe failed: {completed.stderr[-500:]}")
        facts = decode_payload(completed.stdout, completed.stderr, "core probe")
        root = source_root()
        threadsense_file = Path(facts["threadsense_file"]).resolve()
        check(root in threadsense_file.parents, f"threadsense imported from {threadsense_file}")
        check(facts["torch_spec_none"], "torch is installed in the core environment")
        check(facts["transformers_spec_none"], "transformers is installed in the core environment")
        check(not facts["heavy_loaded"], f"core import loaded {facts['heavy_loaded']}")
        self.record(
            "core_without_model",
            threadsense_file_inside_source_root=True,
            torch_find_spec_is_none=facts["torch_spec_none"],
            transformers_find_spec_is_none=facts["transformers_spec_none"],
            heavy_modules_loaded=facts["heavy_loaded"],
            importable_modules=["threadsense.cli", "threadsense.decisions.annotate"],
        )

    def stage_input(self) -> None:
        source = self.repo_root / FIXTURE_RELPATH
        self.staged_input.parent.mkdir(parents=True, exist_ok=True)
        self.staged_input.write_bytes(source.read_bytes())
        self.staged_sha = file_sha256(self.staged_input)
        document = load_json_object(self.staged_input)
        comments = document["thread"]["comments"]
        check(len(comments) == COMMENT_COUNT, f"fixture has {len(comments)} comments")
        self.record(
            "stage_input",
            fixture=FIXTURE_RELPATH,
            sha256=self.staged_sha,
            bytes=self.staged_input.stat().st_size,
            comment_count=len(comments),
        )

    def findings_digest(self, path: Path) -> str:
        findings = load_json_object(path)["analysis"]["findings"]
        check(isinstance(findings, list) and bool(findings), "analysis has no findings")
        return canonical_sha256(findings)

    def run_baseline(self, output: Path) -> None:
        code, payload = self.cli(
            *("analyze", "normalized", "--input", str(self.staged_input)),
            *("--output", str(output)),
        )
        check(code == 0, f"baseline analysis failed: {payload}")
        check(output.is_file(), f"baseline analysis wrote nothing at {output}")

    def baseline_analysis(self) -> None:
        self.run_baseline(self.baseline_path)
        self.baseline_sha = file_sha256(self.baseline_path)
        self.baseline_findings_digest = self.findings_digest(self.baseline_path)
        self.record(
            "baseline_analysis",
            runtime_enabled=False,
            artifact=self.relative(self.baseline_path),
            artifact_sha256=self.baseline_sha,
            findings_sha256=self.baseline_findings_digest,
        )

    def check_item(
        self, item: dict[str, Any], body: str, questions: dict[str, dict[str, Any]], repo: str
    ) -> dict[str, Any]:
        comment_id = item["comment_id"]
        check(item["status"] == "ok", f"{comment_id}: status {item['status']}")
        check(
            item["comment_text_sha256"] == text_sha256(body),
            f"{comment_id}: comment_text_sha256 does not match the fixture body",
        )
        answers = {answer["question_id"]: answer for answer in item["answers"]}
        check(
            sorted(answers) == sorted(EXPECTED_QUESTION_IDS) and len(item["answers"]) == 6,
            f"{comment_id}: answers {sorted(answers)}",
        )
        summary: dict[str, Any] = {}
        for question_id, answer in answers.items():
            check(answer["calibration"] == "uncalibrated", f"{comment_id}/{question_id}: calib")
            check(bool(answer["confidence_quantities"]), f"{comment_id}/{question_id}: no conf")
            kind = answer["type"]
            check(kind == questions[question_id]["type"], f"{comment_id}/{question_id}: type")
            summary[question_id] = answer[kind]
            if kind == "score":
                levels = len(questions[question_id]["criteria"])
                check(
                    0 <= answer["score"] <= levels - 1,
                    f"{comment_id}/{question_id}: score {answer['score']} out of range",
                )
        routing = item["routing"]
        check(routing["repo"] == repo, f"{comment_id}: routing repo differs from verification")
        return summary

    def check_decision(
        self, payload: dict[str, Any], decision: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        coverage = decision["coverage"]
        check(
            (coverage["status"], coverage["total"], coverage["ok"])
            == ("complete", COMMENT_COUNT, COMMENT_COUNT),
            f"coverage {coverage}",
        )
        check(
            (decision["mode"], decision["policy"], decision["calibration"])
            == ("shadow", "none", "uncalibrated"),
            "mode/policy/calibration",
        )
        snapshot = decision["input"]
        check(snapshot["sha256"] == self.staged_sha, "input.sha256 differs from staged input")
        check(snapshot["bytes"] == self.staged_input.stat().st_size, "input.bytes differs")
        route = decision["route"]
        check(
            route["model"] == "english"
            and route["language"] == {"value": "en", "basis": "declared"}
            and route["detection"] is None,
            f"route {route}",
        )
        backend = decision["backend"]
        check(backend["manifest_sha256"] == payload["manifest_sha256"], "manifest sha differs")
        verification = backend["checkpoint_verification"]
        check(verification is not None, "checkpoint_verification missing")
        check(verification["basis"] == CHECKPOINT_BASIS, f"basis {verification['basis']}")
        check(
            verification["files"] == backend["manifest"]["checkpoint"]["files"],
            "verified checkpoint files differ from the manifest",
        )
        repo = verification["repo"]
        check(Path(repo).is_dir(), "verified checkpoint directory does not exist")
        by_id = {q["question_id"]: q for q in decision["questions"]["questions"]}
        check(
            sorted(by_id) == sorted(EXPECTED_QUESTION_IDS),
            f"question ids {sorted(by_id)}",
        )
        return by_id

    def check_exchanges(self, decision: dict[str, Any]) -> list[dict[str, Any]]:
        exchanges = decision["exchanges"]
        check(bool(exchanges), "decision has no exchanges")
        for exchange in exchanges:
            check(
                (exchange["outcome"], exchange["status_code"]) == ("response", 200),
                f"exchange {exchange['batch_index']}: {exchange['outcome']}",
            )
            check(
                text_sha256(exchange["response_body"]) == exchange["response_sha256"],
                f"exchange {exchange['batch_index']}: response digest mismatch",
            )
        return [
            {
                "batch_index": exchange["batch_index"],
                "item_count": len(exchange["item_indexes"]),
                "status_code": exchange["status_code"],
                "response_sha256": exchange["response_sha256"],
            }
            for exchange in exchanges
        ]

    def required_real_annotation(self) -> None:
        code, payload = self.annotate(self.endpoint, "required", self.annotations_dir)
        check(code == 0, f"required annotation exited {code}: {payload}")
        check(
            payload["artifact_type"] == "decision_annotations"
            and payload["satisfied"] is True
            and payload["requirement"] == "required"
            and payload["status"] == "complete",
            f"required annotation payload {payload}",
        )
        decision_path = self.published_path(payload["decision_path"])
        run_path = self.published_path(payload["run_path"])
        decision = load_json_object(decision_path)
        run = load_json_object(run_path)
        check(file_sha256(decision_path) == payload["decision_sha256"], "decision file sha")
        questions = self.check_decision(payload, decision)
        repo = decision["backend"]["checkpoint_verification"]["repo"]
        bodies = {
            comment["comment_id"]: comment["body"]
            for comment in load_json_object(self.staged_input)["thread"]["comments"]
        }
        items = decision["items"]
        check(
            sorted(item["comment_id"] for item in items) == sorted(bodies),
            "item comment ids differ from the fixture",
        )
        answers = {
            item["comment_id"]: self.check_item(item, bodies[item["comment_id"]], questions, repo)
            for item in items
        }
        exchanges = self.check_exchanges(decision)
        check(run["satisfied"] is True, "run record not satisfied")
        timings = run["requests"]
        check(
            bool(timings) and all(timing["status_code"] == 200 for timing in timings),
            f"run requests {timings}",
        )
        self.first_decision_sha = payload["decision_sha256"]
        self.first_decision_path = payload["decision_path"]
        self.first_run_id = payload["run_id"]
        self.observations["checkpoint_directory_name"] = Path(repo).name
        self.record(
            "required_real_annotation",
            decision_path=self.relative(decision_path),
            run_path=self.relative(run_path),
            decision_sha256=payload["decision_sha256"],
            run_id=payload["run_id"],
            manifest_sha256=payload["manifest_sha256"],
            question_set_sha256=payload["question_set_sha256"],
            checkpoint_directory_name=Path(repo).name,
            checkpoint_path_is_absolute=Path(repo).is_absolute(),
            exchanges=exchanges,
            per_request_wall_seconds=[timing["wall_seconds"] for timing in timings],
            total_http_wall_seconds=run["total_http_wall_seconds"],
            wall_time_label=LABEL_HTTP_WALL,
            answers=answers,
            answers_label=LABEL_NO_QUALITY_CLAIM,
        )

    def server_memory_observation(self) -> None:
        port = endpoint_port(self.endpoint)
        listing = run_process(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
            capture_output=True,
            text=True,
            check=False,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        pids = listing.stdout.split()
        check(len(pids) == 1, f"expected exactly one listener on port {port}, got {pids}")
        pid = pids[0]
        command = self.ps_field("command=", pid)
        check("laya.serve" in command, "listening process is not laya.serve")
        rss_kib = int(self.ps_field("rss=", pid))
        self.record(
            "server_memory_observation",
            pid=int(pid),
            command_contains_laya_serve=True,
            rss_kib=rss_kib,
            rss_bytes=rss_kib * 1024,
            rss_label=LABEL_RSS,
        )

    def ps_field(self, field: str, pid: str) -> str:
        completed = run_process(
            ["ps", "-o", field, "-p", pid],
            capture_output=True,
            text=True,
            check=False,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        value = completed.stdout.strip()
        check(completed.returncode == 0 and bool(value), f"ps -o {field} -p {pid} failed")
        return value

    def input_and_baseline_unchanged(self) -> None:
        check(file_sha256(self.staged_input) == self.staged_sha, "staged input changed")
        check(file_sha256(self.baseline_path) == self.baseline_sha, "baseline analysis changed")
        after = self.work_dir / "baseline" / "analysis-after.json"
        self.run_baseline(after)
        after_digest = self.findings_digest(after)
        check(
            after_digest == self.baseline_findings_digest,
            "baseline findings differ after annotation",
        )
        self.record(
            "input_and_baseline_unchanged",
            input_sha256=self.staged_sha,
            baseline_sha256=self.baseline_sha,
            findings_sha256_before=self.baseline_findings_digest,
            findings_sha256_after=after_digest,
            compared="findings section only; timestamps and run metadata excluded",
        )

    def deterministic_replay(self) -> None:
        code, payload = self.annotate(self.endpoint, "required", self.annotations_dir)
        check(code == 0, f"replay exited {code}: {payload}")
        check(
            payload["decision_sha256"] == self.first_decision_sha,
            "replay produced a different decision hash",
        )
        check(
            payload["decision_path"] == self.first_decision_path,
            "replay published the decision at a different path",
        )
        check(payload["run_id"] != self.first_run_id, "replay reused the first run id")
        run_path = self.published_path(payload["run_path"])
        check(run_path.is_file(), "replay run record missing")
        self.record(
            "deterministic_replay",
            decision_sha256=payload["decision_sha256"],
            decision_path=self.relative(self.published_path(payload["decision_path"])),
            first_run_id=self.first_run_id,
            second_run_id=payload["run_id"],
            second_run_path=self.relative(run_path),
        )

    def check_all_unavailable(self, payload: dict[str, Any]) -> dict[str, Any]:
        decision = load_json_object(self.published_path(payload["decision_path"]))
        coverage: dict[str, Any] = decision["coverage"]
        check(
            (coverage["status"], coverage["total"], coverage["unavailable"])
            == ("unavailable", COMMENT_COUNT, COMMENT_COUNT),
            f"coverage {coverage}",
        )
        check(len(decision["items"]) == COMMENT_COUNT, "unexpected item count")
        for item in decision["items"]:
            check(item["status"] == "unavailable", f"{item['comment_id']}: {item['status']}")
            check(
                item["reasons"] == ["runtime_health_unavailable"],
                f"{item['comment_id']}: reasons {item['reasons']}",
            )
            check(item["answers"] == [], f"{item['comment_id']}: answers present")
        return coverage

    def optional_unavailable(self) -> None:
        port = closed_loopback_port()
        output_dir = self.work_dir / "annotations-unavailable"
        code, payload = self.annotate(f"http://127.0.0.1:{port}", "optional", output_dir)
        check(code == 0, f"optional unavailable exited {code}: {payload}")
        check(payload["satisfied"] is True, "optional run must be satisfied")
        check(payload["status"] == "unavailable", f"status {payload['status']}")
        coverage = self.check_all_unavailable(payload)
        self.record(
            "optional_unavailable",
            exit_code=code,
            coverage=coverage,
            satisfied=payload["satisfied"],
            reasons=["runtime_health_unavailable"],
            decision_path=self.relative(self.published_path(payload["decision_path"])),
        )

    def required_unavailable(self) -> None:
        port = closed_loopback_port()
        output_dir = self.work_dir / "annotations-required-unavailable"
        code, payload = self.annotate(f"http://127.0.0.1:{port}", "required", output_dir)
        check(code == 1, f"required unavailable exited {code}: {payload}")
        check(payload["satisfied"] is False, "required gap must not be satisfied")
        coverage = self.check_all_unavailable(payload)
        self.record(
            "required_unavailable",
            exit_code=code,
            coverage=coverage,
            satisfied=payload["satisfied"],
            artifact_published=True,
            decision_path=self.relative(self.published_path(payload["decision_path"])),
        )

    def refused_output_location(self) -> None:
        output_dir = self.store_root / "normalized" / "annotations"
        code, payload = self.annotate(self.endpoint, "optional", output_dir)
        reason = payload.get("error", {}).get("details", {}).get("reason")
        check(code != 0, "annotation into protected storage exited 0")
        check(reason == "output_inside_protected_storage", f"refusal reason {reason}: {payload}")
        check(not output_dir.exists(), "refused output directory was created")
        self.record(
            "refused_output_location",
            exit_code=code,
            reason=reason,
            directory_exists_after=False,
        )

    def refused_endpoint(self) -> None:
        endpoint = f"http://localhost:{endpoint_port(self.endpoint)}"
        output_dir = self.work_dir / "annotations-refused-endpoint"
        code, payload = self.annotate(endpoint, "optional", output_dir)
        reason = payload.get("error", {}).get("details", {}).get("reason")
        check(code != 0, "non-literal loopback endpoint exited 0")
        check(reason == "endpoint_not_literal_loopback", f"refusal reason {reason}: {payload}")
        self.record("refused_endpoint", exit_code=code, reason=reason, endpoint_host="localhost")
