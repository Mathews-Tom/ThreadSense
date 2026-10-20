"""Annotation of one canonical thread: published artifacts, run records and failure transitions.

Every test drives the real ``annotate_canonical_thread`` with a real ``TypedDecisionClient`` and a
real ``CheckpointVerifier``. Only the HTTP transport is replaced (httpx MockTransport serving the
exchange recorded from the pinned service), so these are engineering tests of the client-side
protocol and never real-model evidence.
"""

from __future__ import annotations

import copy
import hashlib
import itertools
import json
import os
import re
import shutil
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from typed_decision_support import (
    CANONICAL_FIXTURE,
    ENDPOINT,
    HEALTH_PAYLOAD,
    RECORDED_BATCH_FIXTURE,
    WIRE,
    Handler,
    build_manifest,
    json_response,
    recorded_batch,
    routed_transport,
    write_checkpoint,
)

from threadsense.contracts.analysis_contract import ObjectiveType
from threadsense.decisions.annotate import (
    AnnotationJob,
    AnnotationOutcome,
    annotate_canonical_thread,
)
from threadsense.decisions.artifact import AnnotationItem, publish_immutable
from threadsense.decisions.client import TypedDecisionClient
from threadsense.decisions.contracts import (
    PINNED_CONTEXT_POLICY,
    PINNED_INFERENCE_SETTINGS,
    AggregateStatus,
    CommentState,
    ContextPolicy,
    InferenceSettings,
    ItemFailure,
    ItemStatus,
    QuestionSet,
    QuestionType,
    Requirement,
    RuntimeManifest,
    ValidatedAnswer,
)
from threadsense.decisions.questions import build_question_set
from threadsense.decisions.runtime import CheckpointVerifier
from threadsense.decisions.validation import validate_result
from threadsense.domains.loader import load_domain_vocabulary
from threadsense.errors import (
    AnalysisBoundaryError,
    DecisionArtifactError,
    DecisionInputError,
    SchemaBoundaryError,
)
from threadsense.monitoring.observations import comment_text_sha256, thread_text_sha256

OBJECTIVE = ObjectiveType.FRICTION_ANALYSIS
DOMAIN = "developer_tools"
COMMENT_COUNT = 7
RUN_VARYING_KEYS = frozenset(
    {
        "started_at_utc",
        "completed_at_utc",
        "wall_seconds",
        "run_id",
        "endpoint",
        "total_http_wall_seconds",
    }
)


@dataclass(frozen=True)
class Staged:
    root: Path
    input_path: Path
    output_root: Path
    checkpoint: Path
    manifest: RuntimeManifest
    verifier: CheckpointVerifier
    request_text: str
    response_text: str
    payload: dict[str, Any]


@dataclass
class Calls:
    health: list[httpx.Request] = field(default_factory=list)
    batch: list[httpx.Request] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.health) + len(self.batch)


@dataclass(frozen=True)
class Run:
    outcome: AnnotationOutcome
    calls: Calls
    decision_bytes: bytes
    decision: dict[str, Any]
    run_record: dict[str, Any]


@pytest.fixture
def staged(tmp_path: Path) -> Staged:
    input_path = tmp_path / "input" / "canonical.json"
    input_path.parent.mkdir()
    shutil.copyfile(CANONICAL_FIXTURE, input_path)
    checkpoint = write_checkpoint(tmp_path / "checkpoint")
    manifest = build_manifest()
    request_text, payload = recorded_batch(checkpoint)
    fixture = json.loads(RECORDED_BATCH_FIXTURE.read_text(encoding="utf-8"))
    response_text: str = fixture["response_body"].replace(
        fixture["provenance"]["repo_placeholder"], str(checkpoint)
    )
    return Staged(
        root=tmp_path,
        input_path=input_path,
        output_root=tmp_path / "out",
        checkpoint=checkpoint,
        manifest=manifest,
        verifier=CheckpointVerifier(manifest),
        request_text=request_text,
        response_text=response_text,
        payload=payload,
    )


def _ticker(start: float, step: float) -> Callable[[], float]:
    counter = itertools.count()
    return lambda: start + step * next(counter)


def _health_ok(_request: httpx.Request) -> httpx.Response:
    return json_response(HEALTH_PAYLOAD)


def _recording(handler: Handler, sink: list[httpx.Request]) -> Handler:
    def wrapped(request: httpx.Request) -> httpx.Response:
        sink.append(request)
        return handler(request)

    return wrapped


def _serve_text(text: str) -> Handler:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, content=text.encode("utf-8"), headers={"content-type": "application/json"}
        )

    return handler


def _serve_payload(payload: dict[str, Any]) -> Handler:
    # json.dumps keeps NaN literals, so a payload holding float("nan") is served as raw NaN.
    return _serve_text(json.dumps(payload))


def _serve_without(staged: Staged, omitted: int) -> Handler:
    results = [item for index, item in enumerate(staged.payload["results"]) if index != omitted]
    total = {
        key: sum(item["usage"][key] for item in results)
        for key in ("input_tokens", "output_tokens")
    }
    return _serve_payload({"results": results, "total_usage": total})


def _status_503(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(503, content=b"service unavailable")


def _refuse_connection(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("connection refused", request=request)


def _client(
    batch: Handler, calls: Calls, *, health: Handler = _health_ok, tick: float = 0.5
) -> TypedDecisionClient:
    return TypedDecisionClient(
        ENDPOINT,
        wire=WIRE,
        timeout_seconds=30,
        transport=routed_transport(
            _recording(health, calls.health), _recording(batch, calls.batch)
        ),
        clock=_ticker(0.0, tick),
    )


def _job(
    staged: Staged,
    *,
    requirement: Requirement = Requirement.REQUIRED,
    output_root: Path | None = None,
    protected_roots: tuple[Path, ...] = (),
    domain: str = DOMAIN,
) -> AnnotationJob:
    return AnnotationJob(
        input_path=staged.input_path,
        output_root=staged.output_root if output_root is None else output_root,
        protected_roots=protected_roots,
        requirement=requirement,
        objective=OBJECTIVE,
        domain=domain,
    )


def _annotate(
    staged: Staged,
    client: TypedDecisionClient,
    *,
    job: AnnotationJob | None = None,
    run_id: str | None = "run-1",
    wall_start: float = 1000.0,
    settings: InferenceSettings = PINNED_INFERENCE_SETTINGS,
    context_policy: ContextPolicy = PINNED_CONTEXT_POLICY,
) -> AnnotationOutcome:
    return annotate_canonical_thread(
        _job(staged) if job is None else job,
        client=client,
        manifest=staged.manifest,
        verifier=staged.verifier,
        settings=settings,
        context_policy=context_policy,
        wall_clock=_ticker(wall_start, 2.5),
        run_id=run_id,
    )


def _load_json(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def _run(
    staged: Staged,
    batch: Handler,
    *,
    health: Handler = _health_ok,
    requirement: Requirement = Requirement.REQUIRED,
    run_id: str | None = "run-1",
    wall_start: float = 1000.0,
    tick: float = 0.5,
    context_policy: ContextPolicy = PINNED_CONTEXT_POLICY,
) -> Run:
    calls = Calls()
    client = _client(batch, calls, health=health, tick=tick)
    try:
        outcome = _annotate(
            staged,
            client,
            job=_job(staged, requirement=requirement),
            run_id=run_id,
            wall_start=wall_start,
            context_policy=context_policy,
        )
    finally:
        client.close()
    return Run(
        outcome=outcome,
        calls=calls,
        decision_bytes=outcome.decision_path.read_bytes(),
        decision=_load_json(outcome.decision_path),
        run_record=_load_json(outcome.run_path),
    )


@pytest.fixture
def happy(staged: Staged) -> Run:
    return _run(staged, _serve_text(staged.response_text))


def _question_set() -> QuestionSet:
    return build_question_set(OBJECTIVE, load_domain_vocabulary(DOMAIN))


def _all_keys(value: Any) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, dict):
        keys.update(value)
        for child in value.values():
            keys |= _all_keys(child)
    elif isinstance(value, list):
        for child in value:
            keys |= _all_keys(child)
    return keys


def _stray_temp_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*") if path.name.endswith(".tmp"))


def _usage_variant(staged: Staged, index: int, changes: dict[str, Any]) -> dict[str, Any]:
    payload = copy.deepcopy(staged.payload)
    payload["results"][index]["usage"].update(changes)
    return payload


def _rewrite_comment_body(staged: Staged, index: int, body: str) -> None:
    canonical = json.loads(staged.input_path.read_text(encoding="utf-8"))
    canonical["thread"]["comments"][index]["body"] = body
    staged.input_path.write_text(json.dumps(canonical), encoding="utf-8")


def _expect_unavailable_for_all(run: Run, *, reason: str, failure_reason: str) -> None:
    items = run.decision["items"]
    assert len(items) == COMMENT_COUNT
    assert {item["status"] for item in items} == {"unavailable"}
    assert all(item["reasons"] == [reason] for item in items)
    assert all(item["failure"]["reason"] == failure_reason for item in items)
    assert all(item["answers"] == [] for item in items)
    assert run.decision["coverage"] == {
        "status": "unavailable",
        "total": COMMENT_COUNT,
        "ok": 0,
        "refused": 0,
        "unavailable": COMMENT_COUNT,
    }


# --- 1. Happy path on the recorded real exchange -------------------------------------------


def test_annotate_recorded_exchange_reports_complete_coverage(happy: Run) -> None:
    coverage = happy.outcome.coverage

    assert coverage.status is AggregateStatus.COMPLETE
    assert (coverage.total, coverage.ok, coverage.refused, coverage.unavailable) == (7, 7, 0, 0)
    assert happy.decision["coverage"] == coverage.to_dict()


@pytest.mark.parametrize("requirement", [Requirement.REQUIRED, Requirement.OPTIONAL])
def test_annotate_recorded_exchange_is_satisfied_for_either_requirement(
    staged: Staged, requirement: Requirement
) -> None:
    run = _run(staged, _serve_text(staged.response_text), requirement=requirement)

    assert run.outcome.satisfied is True
    assert run.outcome.requirement is requirement
    assert run.decision["requirement"] == requirement.value
    assert run.run_record["satisfied"] is True


def test_annotate_publishes_decision_under_snapshot_and_content_hashes(
    staged: Staged, happy: Run
) -> None:
    snapshot_sha = hashlib.sha256(staged.input_path.read_bytes()).hexdigest()
    decision_sha = hashlib.sha256(happy.decision_bytes).hexdigest()
    root = staged.output_root.resolve()

    assert happy.outcome.snapshot_sha256 == snapshot_sha
    assert happy.outcome.decision_sha256 == decision_sha
    assert happy.outcome.decision_path == root / snapshot_sha / "decisions" / f"{decision_sha}.json"
    assert happy.outcome.run_path == root / snapshot_sha / "runs" / "run-1.json"
    assert happy.outcome.run_id == "run-1"


def test_annotate_outcome_binds_question_set_and_manifest_hashes(
    staged: Staged, happy: Run
) -> None:
    assert happy.outcome.question_set_sha256 == _question_set().sha256()
    assert happy.outcome.manifest_sha256 == staged.manifest.sha256()


def test_decision_document_declares_shadow_uncalibrated_typed_annotations(happy: Run) -> None:
    decision = happy.decision

    assert decision["artifact_kind"] == "typed_decision_annotations"
    assert decision["mode"] == "shadow"
    assert decision["policy"] == "none"
    assert decision["calibration"] == "uncalibrated"


def test_decision_input_binding_matches_canonical_snapshot(staged: Staged, happy: Run) -> None:
    raw = staged.input_path.read_bytes()
    canonical = json.loads(raw)
    thread = canonical["thread"]

    binding = happy.decision["input"]

    assert binding["sha256"] == hashlib.sha256(raw).hexdigest()
    assert binding["bytes"] == len(raw)
    assert binding["artifact_kind"] == canonical["artifact_kind"]
    assert binding["schema_version"] == canonical["schema_version"]
    assert binding["normalization_version"] == canonical["normalization_version"]
    assert binding["thread_id"] == thread["thread_id"]
    assert binding["source"] == thread["source"]
    assert binding["thread_text_sha256"] == thread_text_sha256(thread["title"], thread["body"])
    assert binding["comment_count"] == thread["comment_count"] == COMMENT_COUNT
    assert binding["comments_present"] == len(thread["comments"])


def test_decision_items_follow_input_order_with_comment_text_hashes(
    staged: Staged, happy: Run
) -> None:
    comments = json.loads(staged.input_path.read_bytes())["thread"]["comments"]

    items = happy.decision["items"]

    assert [item["index"] for item in items] == list(range(COMMENT_COUNT))
    assert [item["comment_id"] for item in items] == [c["comment_id"] for c in comments]
    assert [item["comment_text_sha256"] for item in items] == [
        comment_text_sha256(c["body"]) for c in comments
    ]
    assert {item["status"] for item in items} == {"ok"}
    assert all(item["reasons"] == [] and item["failure"] is None for item in items)
    assert {item["batch_index"] for item in items} == {0}


def test_decision_items_keep_typed_answers_of_their_own_result(staged: Staged, happy: Run) -> None:
    question_ids = list(_question_set().question_ids())
    value_keys = {"choice", "score", "noul"}

    for item, result in zip(happy.decision["items"], staged.payload["results"], strict=True):
        assert [answer["question_id"] for answer in item["answers"]] == question_ids
        for answer in item["answers"]:
            raw = result["answers"][answer["question_id"]]
            kind = raw["type"]
            assert answer["type"] == kind
            assert value_keys & set(answer) == {kind}
            assert answer[kind] == raw[kind]
            assert answer["calibration"] == "uncalibrated"
            assert [q["name"] for q in answer["confidence_quantities"]] == [
                "answer_confidence",
                "confidence",
            ]
            assert [q["value"] for q in answer["confidence_quantities"]] == [
                raw["answer_confidence"],
                raw["confidence"],
            ]
            expected_probabilities = [] if kind == "noul" else list(raw["probabilities"].items())
            assert answer["probabilities"] == [list(pair) for pair in expected_probabilities]


def test_decision_first_item_records_values_of_the_recorded_service(happy: Run) -> None:
    answers = {answer["question_id"]: answer for answer in happy.decision["items"][0]["answers"]}

    assert answers["theme"]["choice"] == "documentation"
    assert answers["evidence_type"]["choice"] == "opinion_or_preference"
    assert answers["request_intent"]["choice"] == "neither"
    assert answers["objective_relevance"]["score"] == 0.1433
    assert answers["actionability"]["score"] == 0.2301
    assert answers["promotional_framing"]["noul"] == 0.0026


def test_decision_backend_pins_manifest_and_client_verified_checkpoint(
    staged: Staged, happy: Run
) -> None:
    backend = happy.decision["backend"]
    verification = backend["checkpoint_verification"]

    assert backend["manifest_sha256"] == staged.manifest.sha256()
    assert backend["manifest"] == staged.manifest.to_dict()
    assert backend["code_identity_basis"] == "declared_by_manifest_attested_by_reuse_procedure"
    assert verification["repo"] == str(staged.checkpoint)
    assert verification["basis"] == "client_file_sha256"
    assert verification["files"] == [f.to_dict() for f in staged.manifest.checkpoint.files]


def test_decision_route_declares_language_and_never_detects_it(staged: Staged, happy: Run) -> None:
    assert happy.decision["route"] == {
        "model": "english",
        "language": {"value": "en", "basis": "declared"},
        "detection": None,
    }
    for item in happy.decision["items"]:
        assert item["routing"]["repo"] == str(staged.checkpoint)
        assert item["routing"]["detection"] is None


def test_decision_records_question_settings_and_context_policy_identities(happy: Run) -> None:
    decision = happy.decision

    assert decision["questions"]["sha256"] == _question_set().sha256()
    assert decision["settings"]["sha256"] == PINNED_INFERENCE_SETTINGS.sha256()
    assert decision["settings"]["model"] == "english"
    assert decision["context_policy"]["sha256"] == PINNED_CONTEXT_POLICY.sha256()
    assert (
        decision["context_policy"]["comment_max_chars"] == PINNED_CONTEXT_POLICY.comment_max_chars
    )


def test_decision_exchange_keeps_verbatim_response_and_the_request_as_sent(
    staged: Staged, happy: Run
) -> None:
    (exchange,) = happy.decision["exchanges"]
    (sent,) = happy.calls.batch

    assert exchange["response_body"] == staged.response_text
    assert exchange["response_sha256"] == hashlib.sha256(staged.response_text.encode()).hexdigest()
    assert exchange["request_body"] == sent.content.decode("utf-8") == staged.request_text
    assert exchange["request_sha256"] == hashlib.sha256(staged.request_text.encode()).hexdigest()
    assert exchange["outcome"] == "response"
    assert exchange["status_code"] == 200
    assert exchange["error"] is None
    assert exchange["item_indexes"] == list(range(COMMENT_COUNT))


def test_annotate_verifies_checkpoint_files_once_for_all_items(staged: Staged, happy: Run) -> None:
    assert staged.verifier.files_hashed == len(staged.manifest.checkpoint.files)
    assert len(happy.calls.health) == 1
    assert len(happy.calls.batch) == 1


def test_annotate_success_leaves_no_temporary_files(staged: Staged, happy: Run) -> None:
    assert _stray_temp_files(staged.output_root) == []


# --- 2. Determinism and execution separation -------------------------------------------------


def test_annotate_rerun_with_new_run_id_and_clocks_reuses_the_identical_decision(
    staged: Staged,
) -> None:
    serve = _serve_text(staged.response_text)

    first = _run(staged, serve, run_id="run-a", wall_start=1000.0, tick=0.5)
    second = _run(staged, serve, run_id="run-b", wall_start=5000.0, tick=0.125)

    assert second.outcome.decision_sha256 == first.outcome.decision_sha256
    assert second.outcome.decision_path == first.outcome.decision_path
    assert second.decision_bytes == first.decision_bytes
    assert second.outcome.run_path != first.outcome.run_path
    assert sorted(p.name for p in first.outcome.decision_path.parent.iterdir()) == [
        f"{first.outcome.decision_sha256}.json"
    ]
    assert sorted(p.name for p in first.outcome.run_path.parent.iterdir()) == [
        "run-a.json",
        "run-b.json",
    ]
    assert first.run_record["started_at_utc"] != second.run_record["started_at_utc"]


def test_decision_document_excludes_values_that_vary_between_runs(happy: Run) -> None:
    text = happy.decision_bytes.decode("utf-8")

    assert RUN_VARYING_KEYS & _all_keys(happy.decision) == set()
    assert ENDPOINT not in text
    assert "run-1" not in text


def test_run_record_carries_endpoint_timing_and_observation_scope(
    staged: Staged, happy: Run
) -> None:
    record = happy.run_record
    relpath = f"{happy.outcome.snapshot_sha256}/decisions/{happy.outcome.decision_sha256}.json"

    assert record["artifact_kind"] == "typed_decision_annotation_run"
    assert record["run_id"] == "run-1"
    assert record["decision_sha256"] == happy.outcome.decision_sha256
    assert record["decision_path"] == relpath
    assert record["endpoint"] == ENDPOINT
    assert record["started_at_utc"] == 1000.0
    assert record["completed_at_utc"] == 1002.5
    assert record["health"]["status"] == "ok"
    assert record["health"]["payload"] == HEALTH_PAYLOAD
    assert record["requests"] == [
        {"batch_index": None, "outcome": "response", "status_code": 200, "wall_seconds": 0.5},
        {"batch_index": 0, "outcome": "response", "status_code": 200, "wall_seconds": 0.5},
    ]
    assert record["total_http_wall_seconds"] == 1.0
    assert record["coverage"] == happy.decision["coverage"]
    assert record["requirement"] == "required"
    assert isinstance(record["observation_scope"], str)
    assert record["observation_scope"]


def test_annotate_without_run_id_derives_a_unique_id_from_the_start_time(staged: Staged) -> None:
    serve = _serve_text(staged.response_text)

    first = _run(staged, serve, run_id=None, wall_start=1000.0)
    second = _run(staged, serve, run_id=None, wall_start=1000.0)

    pattern = re.compile(r"19700101T001640Z-[0-9a-f]{12}")
    assert pattern.fullmatch(first.outcome.run_id)
    assert pattern.fullmatch(second.outcome.run_id)
    assert first.outcome.run_id != second.outcome.run_id
    assert first.outcome.decision_sha256 == second.outcome.decision_sha256


def test_annotate_invalid_run_id_is_refused_and_publishes_nothing(staged: Staged) -> None:
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(DecisionArtifactError) as raised:
        _annotate(staged, client, run_id="../escape")

    assert raised.value.details["reason"] == "artifact_name_invalid"
    assert raised.value.details["field"] == "run_id"
    assert not staged.output_root.exists()


# --- 3. Input integrity ----------------------------------------------------------------------


def test_annotate_leaves_input_bytes_and_mtime_unchanged(staged: Staged) -> None:
    os.utime(staged.input_path, ns=(1_000_000_000_000_000_000, 1_000_000_000_000_000_000))
    before_bytes = staged.input_path.read_bytes()
    before_mtime = staged.input_path.stat().st_mtime_ns

    run = _run(staged, _serve_text(staged.response_text))

    assert staged.input_path.read_bytes() == before_bytes
    assert staged.input_path.stat().st_mtime_ns == before_mtime
    assert run.decision["input"]["sha256"] == hashlib.sha256(before_bytes).hexdigest()


class _ReadCounter:
    """Counts outermost reads of one path through the pathlib read entry points."""

    def __init__(self, target: Path) -> None:
        self.target = target
        self.count = 0
        self._depth = 0

    def wrap(self, original: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(path: Path, *args: Any, **kwargs: Any) -> Any:
            if self._depth == 0 and path == self.target:
                self.count += 1
            self._depth += 1
            try:
                return original(path, *args, **kwargs)
            finally:
                self._depth -= 1

        return wrapper


def test_annotate_reads_the_input_exactly_once(
    staged: Staged, monkeypatch: pytest.MonkeyPatch
) -> None:
    counter = _ReadCounter(staged.input_path)
    for name in ("read_bytes", "read_text", "open"):
        monkeypatch.setattr(Path, name, counter.wrap(getattr(Path, name)))

    run = _run(staged, _serve_text(staged.response_text))

    assert counter.count == 1
    assert run.outcome.coverage.ok == COMMENT_COUNT


def test_annotate_decision_hashes_come_from_the_bytes_read_before_the_run(
    staged: Staged,
) -> None:
    before = staged.input_path.read_bytes()
    comments = json.loads(before)["thread"]["comments"]

    run = _run(staged, _serve_text(staged.response_text))

    assert run.decision["input"]["sha256"] == hashlib.sha256(before).hexdigest()
    assert [item["comment_text_sha256"] for item in run.decision["items"]] == [
        comment_text_sha256(comment["body"]) for comment in comments
    ]


# --- 4. Unavailability: optional vs required -----------------------------------------------


@pytest.mark.parametrize(
    ("requirement", "satisfied"), [(Requirement.REQUIRED, False), (Requirement.OPTIONAL, True)]
)
def test_annotate_unreachable_health_marks_every_item_unavailable_without_batching(
    staged: Staged, requirement: Requirement, satisfied: bool
) -> None:
    run = _run(
        staged,
        _serve_text(staged.response_text),
        health=_refuse_connection,
        requirement=requirement,
    )

    _expect_unavailable_for_all(
        run, reason="runtime_health_unavailable", failure_reason="connect_failed"
    )
    assert run.calls.batch == []
    assert len(run.calls.health) == 1
    assert run.outcome.coverage.status is AggregateStatus.UNAVAILABLE
    assert run.outcome.satisfied is satisfied
    assert run.run_record["satisfied"] is satisfied
    assert run.decision["exchanges"] == []
    assert run.outcome.decision_path.is_file()


def test_annotate_unreachable_health_keeps_timing_out_of_the_decision(staged: Staged) -> None:
    run = _run(staged, _serve_text(staged.response_text), health=_refuse_connection)

    assert RUN_VARYING_KEYS - {"endpoint"} & _all_keys(run.decision) == set()
    assert run.run_record["requests"] == [
        {"batch_index": None, "outcome": "error", "status_code": None, "wall_seconds": 0.5}
    ]
    assert run.run_record["total_http_wall_seconds"] == 0.5


def test_annotate_health_not_ready_marks_every_item_unavailable_without_batching(
    staged: Staged,
) -> None:
    def loading(_request: httpx.Request) -> httpx.Response:
        return json_response({**HEALTH_PAYLOAD, "status": "loading"})

    run = _run(staged, _serve_text(staged.response_text), health=loading)

    _expect_unavailable_for_all(
        run, reason="runtime_health_unavailable", failure_reason="health_not_ready"
    )
    assert run.calls.batch == []
    assert run.run_record["requests"][0]["outcome"] == "error"
    assert run.run_record["requests"][0]["status_code"] == 200


@pytest.mark.parametrize(
    ("requirement", "satisfied"), [(Requirement.REQUIRED, False), (Requirement.OPTIONAL, True)]
)
def test_annotate_batch_status_503_marks_items_unavailable_by_requirement(
    staged: Staged, requirement: Requirement, satisfied: bool
) -> None:
    run = _run(staged, _status_503, requirement=requirement)

    _expect_unavailable_for_all(run, reason="runtime_request_failed", failure_reason="http_status")
    assert all(item["failure"]["details"]["status_code"] == 503 for item in run.decision["items"])
    assert run.outcome.satisfied is satisfied


def test_annotate_batch_status_503_records_the_error_exchange_without_timing(
    staged: Staged,
) -> None:
    run = _run(staged, _status_503)

    (exchange,) = run.decision["exchanges"]
    assert exchange["outcome"] == "error"
    assert exchange["status_code"] is None
    assert exchange["response_body"] is None
    assert exchange["response_sha256"] is None
    assert exchange["request_body"] == staged.request_text
    assert "wall_seconds" not in _all_keys(run.decision)
    assert run.run_record["requests"][1] == {
        "batch_index": 0,
        "outcome": "error",
        "status_code": None,
        "wall_seconds": 0.5,
    }


def test_decision_content_for_a_failed_batch_excludes_the_endpoint(staged: Staged) -> None:
    run = _run(staged, _status_503)

    assert ENDPOINT not in run.decision_bytes.decode("utf-8")
    assert "endpoint" not in _all_keys(run.decision)


def test_annotate_non_json_content_type_marks_items_invalid_response(staged: Staged) -> None:
    def html(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<html></html>", headers={"content-type": "text/html"})

    run = _run(staged, html)

    _expect_unavailable_for_all(
        run, reason="invalid_response", failure_reason="unexpected_content_type"
    )
    assert run.decision["exchanges"][0]["outcome"] == "error"


# --- 5. Index association and counts ---------------------------------------------------------


def test_annotate_six_results_for_seven_states_marks_all_items_invalid_response(
    staged: Staged,
) -> None:
    payload = copy.deepcopy(staged.payload)
    del payload["results"][-1]
    served = json.dumps(payload)

    run = _run(staged, _serve_text(served))

    _expect_unavailable_for_all(
        run, reason="invalid_response", failure_reason="result_count_mismatch"
    )
    assert run.decision["items"][0]["failure"]["details"]["expected"] == COMMENT_COUNT
    assert run.decision["items"][0]["failure"]["details"]["observed"] == COMMENT_COUNT - 1
    (exchange,) = run.decision["exchanges"]
    assert exchange["outcome"] == "response"
    assert exchange["response_body"] == served


def test_annotate_reversed_results_are_associated_strictly_by_position(staged: Staged) -> None:
    reordered = list(reversed(staged.payload["results"]))
    question_set = _question_set()

    run = _run(staged, _serve_payload({**staged.payload, "results": reordered}))

    items = run.decision["items"]
    assert {item["status"] for item in items} == {"ok"}
    for item, raw in zip(items, reordered, strict=True):
        expected = validate_result(raw, question_set, PINNED_INFERENCE_SETTINGS)
        assert item["answers"] == [answer.to_dict() for answer in expected.answers]
        assert item["usage"] == expected.usage.to_dict()
    original_first = staged.payload["results"][0]
    in_order = validate_result(original_first, question_set, PINNED_INFERENCE_SETTINGS)
    assert items[0]["answers"] != [answer.to_dict() for answer in in_order.answers]


# --- 6. Coverage refusal ---------------------------------------------------------------------

_REFUSAL_CASES = [
    pytest.param(
        {"state_tokens_dropped": 5},
        "server_dropped_state_tokens",
        ("state_tokens_dropped", 5),
        id="state_tokens_dropped",
    ),
    pytest.param(
        {"truncated": True}, "server_reported_truncation", ("truncated", True), id="truncated"
    ),
    pytest.param(
        {"truncated_questions": ["theme"]},
        "server_truncated_questions",
        ("truncated_questions", ["theme"]),
        id="truncated_questions",
    ),
    pytest.param(
        {"options": {}}, "server_reported_option_usage", ("options_reported", True), id="options"
    ),
]


@pytest.mark.parametrize(("changes", "reason", "usage_entry"), _REFUSAL_CASES)
def test_annotate_server_coverage_signal_refuses_only_that_item(
    staged: Staged, changes: dict[str, Any], reason: str, usage_entry: tuple[str, Any]
) -> None:
    run = _run(staged, _serve_payload(_usage_variant(staged, 3, changes)))

    items = run.decision["items"]
    refused = items[3]
    assert refused["status"] == "refused"
    assert refused["reasons"] == [reason]
    assert refused["answers"] == []
    assert refused["failure"] is None
    assert refused["usage"][usage_entry[0]] == usage_entry[1]
    assert refused["routing"] is not None
    assert [item["status"] for item in items if item is not refused] == ["ok"] * 6
    coverage = run.outcome.coverage
    assert coverage.status is AggregateStatus.PARTIAL
    assert (coverage.ok, coverage.refused, coverage.unavailable) == (6, 1, 0)
    assert coverage.ok + coverage.refused + coverage.unavailable == coverage.total


def test_annotate_every_server_coverage_signal_is_listed_in_a_stable_order(
    staged: Staged,
) -> None:
    changes = {
        "state_tokens_dropped": 5,
        "truncated": True,
        "truncated_questions": ["theme"],
        "options": {},
    }

    run = _run(staged, _serve_payload(_usage_variant(staged, 0, changes)))

    assert run.decision["items"][0]["reasons"] == [
        "server_dropped_state_tokens",
        "server_reported_truncation",
        "server_truncated_questions",
        "server_reported_option_usage",
    ]


@pytest.mark.parametrize(
    ("requirement", "satisfied"), [(Requirement.REQUIRED, False), (Requirement.OPTIONAL, True)]
)
def test_annotate_partial_coverage_is_satisfied_only_when_optional(
    staged: Staged, requirement: Requirement, satisfied: bool
) -> None:
    served = _serve_payload(_usage_variant(staged, 3, {"state_tokens_dropped": 5}))

    run = _run(staged, served, requirement=requirement)

    assert run.outcome.coverage.status is AggregateStatus.PARTIAL
    assert run.outcome.satisfied is satisfied
    assert run.run_record["satisfied"] is satisfied


def test_annotate_oversized_comment_is_refused_before_sending(staged: Staged) -> None:
    limit = PINNED_CONTEXT_POLICY.comment_max_chars
    oversized = "x" * (limit + 1)
    _rewrite_comment_body(staged, 2, oversized)
    calls = Calls()
    client = _client(_serve_without(staged, 2), calls)

    outcome = _annotate(staged, client)

    decision = _load_json(outcome.decision_path)
    items = decision["items"]
    refused = items[2]
    assert refused["status"] == "refused"
    assert refused["reasons"] == ["comment_exceeds_client_char_budget"]
    assert refused["batch_index"] is None
    assert refused["usage"] is None
    assert refused["routing"] is None
    assert refused["answers"] == []
    assert [item["status"] for item in items if item is not refused] == ["ok"] * 6
    assert outcome.coverage.status is AggregateStatus.PARTIAL
    assert (outcome.coverage.ok, outcome.coverage.refused) == (6, 1)
    (sent,) = calls.batch
    assert len(json.loads(sent.content)["states"]) == COMMENT_COUNT - 1
    assert oversized not in sent.content.decode("utf-8")
    assert decision["exchanges"][0]["item_indexes"] == [0, 1, 3, 4, 5, 6]


def test_annotate_comment_at_the_character_budget_is_still_sent(staged: Staged) -> None:
    limit = PINNED_CONTEXT_POLICY.comment_max_chars
    _rewrite_comment_body(staged, 2, "x" * limit)

    run = _run(staged, _serve_text(staged.response_text))

    assert run.decision["items"][2]["status"] == "ok"
    (sent,) = run.calls.batch
    states = json.loads(sent.content)["states"]
    assert len(states) == COMMENT_COUNT
    assert states[2]["comment"] == "x" * limit


@pytest.mark.parametrize(
    ("requirement", "satisfied"), [(Requirement.REQUIRED, False), (Requirement.OPTIONAL, True)]
)
def test_annotate_all_comments_refused_sends_no_batch_and_reports_unavailable(
    staged: Staged, requirement: Requirement, satisfied: bool
) -> None:
    policy = replace(PINNED_CONTEXT_POLICY, comment_max_chars=10)

    run = _run(
        staged,
        _serve_text(staged.response_text),
        requirement=requirement,
        context_policy=policy,
    )

    assert run.calls.batch == []
    assert {item["status"] for item in run.decision["items"]} == {"refused"}
    assert run.decision["exchanges"] == []
    assert run.decision["context_policy"]["sha256"] == policy.sha256()
    assert run.outcome.coverage.status is AggregateStatus.UNAVAILABLE
    assert (run.outcome.coverage.refused, run.outcome.coverage.total) == (7, 7)
    assert run.outcome.satisfied is satisfied


# --- 7. Invalid typed answers ----------------------------------------------------------------


def test_annotate_reordered_choice_distribution_makes_only_that_item_unavailable(
    staged: Staged,
) -> None:
    payload = copy.deepcopy(staged.payload)
    theme = payload["results"][4]["answers"]["theme"]
    theme["probabilities"] = dict(reversed(list(theme["probabilities"].items())))
    served = json.dumps(payload)

    run = _run(staged, _serve_text(served))

    items = run.decision["items"]
    rejected = items[4]
    assert rejected["status"] == "unavailable"
    assert rejected["reasons"] == ["invalid_typed_answer"]
    assert rejected["failure"]["reason"] == "choice_option_order_mismatch"
    assert rejected["answers"] == []
    assert rejected["usage"] is None
    assert rejected["routing"] is None
    assert [item["status"] for item in items if item is not rejected] == ["ok"] * 6
    assert run.decision["exchanges"][0]["response_body"] == served
    assert (run.outcome.coverage.ok, run.outcome.coverage.unavailable) == (6, 1)


def test_annotate_nan_in_the_response_body_marks_all_items_invalid_response(
    staged: Staged,
) -> None:
    payload = copy.deepcopy(staged.payload)
    payload["results"][0]["answers"]["objective_relevance"]["score"] = float("nan")
    served = json.dumps(payload)
    assert "NaN" in served

    run = _run(staged, _serve_text(served))

    _expect_unavailable_for_all(run, reason="invalid_response", failure_reason="invalid_json")
    assert run.decision["exchanges"][0]["outcome"] == "error"
    assert run.decision["exchanges"][0]["response_body"] is None


# --- 8. Checkpoint identity ------------------------------------------------------------------


def test_annotate_changed_checkpoint_byte_marks_all_items_unavailable_after_one_hash(
    staged: Staged,
) -> None:
    (staged.checkpoint / "config.json").write_bytes(b'{"model_type": "tesx"}\n')

    run = _run(staged, _serve_text(staged.response_text))

    _expect_unavailable_for_all(
        run,
        reason="checkpoint_identity_unverified",
        failure_reason="checkpoint_file_digest_mismatch",
    )
    assert run.decision["items"][0]["failure"]["details"]["path"] == "config.json"
    assert staged.verifier.files_hashed == 1
    assert run.decision["backend"]["checkpoint_verification"] is None


def test_annotate_missing_checkpoint_directory_marks_all_items_unavailable(
    staged: Staged,
) -> None:
    payload = copy.deepcopy(staged.payload)
    for result in payload["results"]:
        result["routing"]["repo"] = str(staged.root / "no-such-checkpoint")

    run = _run(staged, _serve_payload(payload))

    _expect_unavailable_for_all(
        run,
        reason="checkpoint_identity_unverified",
        failure_reason="checkpoint_repo_missing",
    )
    assert staged.verifier.files_hashed == 0


def test_annotate_routing_repo_change_mid_run_refuses_only_the_diverging_item(
    staged: Staged,
) -> None:
    other = write_checkpoint(staged.root / "checkpoint-copy")
    payload = copy.deepcopy(staged.payload)
    payload["results"][1]["routing"]["repo"] = str(other)

    run = _run(staged, _serve_payload(payload))

    items = run.decision["items"]
    changed = items[1]
    assert changed["status"] == "unavailable"
    assert changed["reasons"] == ["checkpoint_repo_changed"]
    assert changed["failure"]["reason"] == "checkpoint_repo_changed"
    assert changed["failure"]["details"] == {
        "first_repo": str(staged.checkpoint),
        "repo": str(other),
    }
    assert changed["answers"] == []
    assert [item["status"] for item in items if item is not changed] == ["ok"] * 6
    assert run.decision["backend"]["checkpoint_verification"]["repo"] == str(staged.checkpoint)
    assert (run.outcome.coverage.ok, run.outcome.coverage.unavailable) == (6, 1)


# --- 9. Output guards and publication --------------------------------------------------------


@pytest.mark.parametrize("suffix", ["annotations", ""], ids=["inside", "equal"])
def test_annotate_output_in_protected_storage_is_refused_before_any_http_call(
    staged: Staged, suffix: str
) -> None:
    store = staged.root / "store"
    protected = (store / "normalized", store / "monitoring")
    job = _job(staged, output_root=store / "normalized" / suffix, protected_roots=protected)
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(DecisionInputError) as raised:
        _annotate(staged, client, job=job)

    assert raised.value.details["reason"] == "output_inside_protected_storage"
    assert calls.total == 0
    assert not store.exists()


def test_annotate_output_beside_a_protected_root_with_a_shared_name_prefix_is_accepted(
    staged: Staged,
) -> None:
    store = staged.root / "store"
    protected = (store / "normalized", store / "monitoring")
    job = _job(staged, output_root=store / "normalized-annotations", protected_roots=protected)
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    outcome = _annotate(staged, client, job=job)

    assert outcome.coverage.status is AggregateStatus.COMPLETE
    assert outcome.decision_path.is_relative_to((store / "normalized-annotations").resolve())


def test_annotate_output_equal_to_the_input_path_is_refused_as_an_alias(staged: Staged) -> None:
    before = staged.input_path.read_bytes()
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(DecisionInputError) as raised:
        _annotate(staged, client, job=_job(staged, output_root=staged.input_path))

    assert raised.value.details["reason"] == "output_aliases_input"
    assert calls.total == 0
    assert staged.input_path.read_bytes() == before


def test_annotate_output_root_that_is_an_existing_file_is_refused(staged: Staged) -> None:
    occupied = staged.root / "occupied.txt"
    occupied.write_bytes(b"not a directory")
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(DecisionInputError) as raised:
        _annotate(staged, client, job=_job(staged, output_root=occupied))

    assert raised.value.details["reason"] == "output_root_not_directory"
    assert calls.total == 0
    assert occupied.read_bytes() == b"not a directory"


def test_annotate_different_file_at_the_decision_path_is_a_conflict_and_stays_intact(
    staged: Staged,
) -> None:
    serve = _serve_text(staged.response_text)
    first = _run(staged, serve, run_id="run-a")
    first.outcome.decision_path.write_bytes(b"tampered")

    with pytest.raises(DecisionArtifactError) as raised:
        _run(staged, serve, run_id="run-b")

    assert raised.value.details["reason"] == "artifact_conflict"
    assert first.outcome.decision_path.read_bytes() == b"tampered"
    assert sorted(p.name for p in first.outcome.run_path.parent.iterdir()) == ["run-a.json"]
    assert _stray_temp_files(staged.output_root) == []


def test_annotate_reused_run_id_refuses_to_replace_the_run_record(staged: Staged) -> None:
    serve = _serve_text(staged.response_text)
    first = _run(staged, serve, run_id="run-a", wall_start=1000.0)
    record_bytes = first.outcome.run_path.read_bytes()

    with pytest.raises(DecisionArtifactError) as raised:
        _run(staged, serve, run_id="run-a", wall_start=9000.0)

    assert raised.value.details["reason"] == "artifact_conflict"
    assert first.outcome.run_path.read_bytes() == record_bytes
    assert sorted(p.name for p in first.outcome.decision_path.parent.iterdir()) == [
        f"{first.outcome.decision_sha256}.json"
    ]
    assert _stray_temp_files(staged.output_root) == []


def test_publish_immutable_new_path_creates_parents_and_writes_exact_bytes(
    tmp_path: Path,
) -> None:
    target = tmp_path / "a" / "b" / "artifact.json"

    publish_immutable(target, b"payload\n", reuse_identical=False)

    assert target.read_bytes() == b"payload\n"
    assert _stray_temp_files(tmp_path) == []


def test_publish_immutable_identical_content_is_reused_without_replacing_the_file(
    tmp_path: Path,
) -> None:
    target = tmp_path / "artifact.json"
    target.write_bytes(b"payload\n")
    inode = target.stat().st_ino

    publish_immutable(target, b"payload\n", reuse_identical=True)

    assert target.stat().st_ino == inode
    assert target.read_bytes() == b"payload\n"
    assert _stray_temp_files(tmp_path) == []


@pytest.mark.parametrize(
    ("existing", "reuse_identical"),
    [(b"payload\n", False), (b"other\n", True), (b"other\n", False)],
    ids=["identical_without_reuse", "different_with_reuse", "different_without_reuse"],
)
def test_publish_immutable_existing_file_is_a_conflict_that_leaves_it_untouched(
    tmp_path: Path, existing: bytes, reuse_identical: bool
) -> None:
    target = tmp_path / "artifact.json"
    target.write_bytes(existing)

    with pytest.raises(DecisionArtifactError) as raised:
        publish_immutable(target, b"payload\n", reuse_identical=reuse_identical)

    assert raised.value.details["reason"] == "artifact_conflict"
    assert target.read_bytes() == existing
    assert _stray_temp_files(tmp_path) == []


# --- 10. Configuration errors raise before contacting the service ----------------------------


def test_annotate_unknown_domain_is_refused_before_contacting_the_service(
    staged: Staged,
) -> None:
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(AnalysisBoundaryError) as raised:
        _annotate(staged, client, job=_job(staged, domain="no_such_domain"))

    assert raised.value.details["domain"] == "no_such_domain"
    assert calls.total == 0
    assert not staged.output_root.exists()


@pytest.mark.parametrize(
    "content",
    [
        b'{"artifact_kind": NaN}',
        b'{"artifact_kind": "a", "artifact_kind": "b"}',
        b"[]",
        b"\xff\xfe",
    ],
    ids=["nan_constant", "duplicate_key", "non_object_root", "invalid_utf8"],
)
def test_annotate_non_strict_json_input_is_refused_before_contacting_the_service(
    staged: Staged, content: bytes
) -> None:
    staged.input_path.write_bytes(content)
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(DecisionInputError):
        _annotate(staged, client)

    assert calls.total == 0
    assert not staged.output_root.exists()


def test_annotate_missing_input_file_is_refused_before_contacting_the_service(
    staged: Staged,
) -> None:
    staged.input_path.unlink()
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(DecisionInputError) as raised:
        _annotate(staged, client)

    assert raised.value.details["reason"] == "input_unreadable"
    assert calls.total == 0


@pytest.mark.parametrize(
    ("key", "value"),
    [("artifact_kind", "analysis_artifact"), ("schema_version", 1)],
    ids=["artifact_kind", "schema_version"],
)
def test_annotate_non_canonical_artifact_is_refused_before_contacting_the_service(
    staged: Staged, key: str, value: Any
) -> None:
    canonical = json.loads(staged.input_path.read_text(encoding="utf-8"))
    canonical[key] = value
    staged.input_path.write_text(json.dumps(canonical), encoding="utf-8")
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(SchemaBoundaryError):
        _annotate(staged, client)

    assert calls.total == 0
    assert not staged.output_root.exists()


@pytest.mark.parametrize(
    "settings",
    [
        InferenceSettings(model="other", lang="en", max_len=512, head_max_len=256),
        InferenceSettings(model="english", lang="de", max_len=512, head_max_len=256),
    ],
    ids=["model", "language"],
)
def test_annotate_settings_that_differ_from_the_manifest_are_refused_before_contacting_the_service(
    staged: Staged, settings: InferenceSettings
) -> None:
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(DecisionInputError) as raised:
        _annotate(staged, client, settings=settings)

    assert raised.value.details["reason"] == "settings_manifest_mismatch"
    assert raised.value.details["manifest_model"] == "english"
    assert raised.value.details["manifest_lang"] == "en"
    assert calls.total == 0
    assert not staged.output_root.exists()


# --- 11. AnnotationItem invariants -----------------------------------------------------------


def _comment_state() -> CommentState:
    return CommentState(
        index=0,
        comment_id="c1",
        parent_comment_id=None,
        comment_text_sha256="a" * 64,
        parent_text_sha256=None,
        parent_context="none",
        state={"comment": "text", "thread_title": "title"},
        state_chars=40,
        windows=(),
        refusal_reasons=(),
    )


def _answer() -> ValidatedAnswer:
    return ValidatedAnswer(
        question_id="theme",
        question_type=QuestionType.CHOICE,
        choice="documentation",
        score=None,
        noul=None,
        probabilities=(("documentation", 1.0),),
        confidence_quantities=(),
    )


def _item(**overrides: Any) -> AnnotationItem:
    values: dict[str, Any] = {
        "state": _comment_state(),
        "status": ItemStatus.OK,
        "reasons": (),
        "failure": None,
        "batch_index": 0,
        "usage": None,
        "routing": None,
        "answers": (_answer(),),
    }
    values.update(overrides)
    return AnnotationItem(**values)


@pytest.mark.parametrize(
    "overrides",
    [
        {"answers": ()},
        {"status": ItemStatus.REFUSED, "reasons": ("server_reported_truncation",)},
        {"status": ItemStatus.UNAVAILABLE, "reasons": ("invalid_response",)},
        {"status": ItemStatus.REFUSED, "answers": (), "reasons": ()},
        {"status": ItemStatus.UNAVAILABLE, "answers": (), "reasons": ()},
        {"reasons": ("server_reported_truncation",)},
        {"failure": ItemFailure("connect_failed", "message", {})},
    ],
    ids=[
        "ok_without_answers",
        "refused_with_answers",
        "unavailable_with_answers",
        "refused_without_reasons",
        "unavailable_without_reasons",
        "ok_with_reasons",
        "ok_with_failure",
    ],
)
def test_annotation_item_rejects_inconsistent_status_answers_and_reasons(
    overrides: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        _item(**overrides)


def test_annotation_item_ok_with_answers_serializes_without_reasons() -> None:
    serialized = _item().to_dict()

    assert serialized["status"] == "ok"
    assert serialized["reasons"] == []
    assert serialized["failure"] is None
    assert [answer["question_id"] for answer in serialized["answers"]] == ["theme"]


def test_annotation_item_refused_with_reasons_serializes_without_answers() -> None:
    item = _item(status=ItemStatus.REFUSED, answers=(), reasons=("server_reported_truncation",))

    serialized = item.to_dict()

    assert serialized["status"] == "refused"
    assert serialized["reasons"] == ["server_reported_truncation"]
    assert serialized["answers"] == []


# --- 12. Regressions from independent review ---------------------------------------------


def _rewrite_input(staged: Staged, text: str) -> None:
    staged.input_path.write_text(text, encoding="utf-8")


def test_annotate_required_empty_thread_with_dead_runtime_is_unsatisfied(
    staged: Staged,
) -> None:
    payload = json.loads(staged.input_path.read_text(encoding="utf-8"))
    payload["thread"]["comments"] = []
    payload["thread"]["comment_count"] = 0
    _rewrite_input(staged, json.dumps(payload))

    required = _run(staged, _serve_text(staged.response_text), health=_refuse_connection)

    assert required.calls.batch == []
    assert required.decision["coverage"]["total"] == 0
    assert required.outcome.satisfied is False
    assert required.run_record["health"]["details"]["reason"] == "connect_failed"


def test_annotate_optional_empty_thread_with_dead_runtime_stays_satisfied(
    staged: Staged,
) -> None:
    payload = json.loads(staged.input_path.read_text(encoding="utf-8"))
    payload["thread"]["comments"] = []
    payload["thread"]["comment_count"] = 0
    _rewrite_input(staged, json.dumps(payload))

    optional = _run(
        staged,
        _serve_text(staged.response_text),
        health=_refuse_connection,
        requirement=Requirement.OPTIONAL,
    )

    assert optional.outcome.satisfied is True
    assert optional.run_record["health"]["details"]["reason"] == "connect_failed"


@pytest.mark.parametrize("literal", ["1e999", "-1e999"])
def test_annotate_overflowing_number_in_response_is_invalid_json(
    staged: Staged, literal: str
) -> None:
    text = staged.response_text.replace('"noul":', f'"noul":{literal},"unused":', 1)

    run = _run(staged, _serve_text(text))

    _expect_unavailable_for_all(run, reason="invalid_response", failure_reason="invalid_json")


def test_annotate_unpaired_surrogate_in_response_is_invalid_json(staged: Staged) -> None:
    text = staged.response_text.replace('"laya-rl-agent"', '"laya-rl-agent\\ud800"', 1)

    run = _run(staged, _serve_text(text))

    _expect_unavailable_for_all(run, reason="invalid_response", failure_reason="invalid_json")


@pytest.mark.parametrize(
    "replacement",
    ['"body": 1e999, "unused": ', '"body": "broken \\udc00 text", "unused": '],
)
def test_annotate_non_encodable_input_is_refused_before_contacting_service(
    staged: Staged, replacement: str
) -> None:
    text = staged.input_path.read_text(encoding="utf-8").replace('"body": ', replacement, 2)
    _rewrite_input(staged, text)
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(DecisionInputError) as caught:
        _annotate(staged, client)

    assert caught.value.details["reason"] == "invalid_json"
    assert calls.total == 0


def test_annotate_truncated_result_with_malformed_answers_is_refused_not_invalid(
    staged: Staged,
) -> None:
    payload = copy.deepcopy(staged.payload)
    result = payload["results"][2]
    result["usage"]["truncated"] = True
    del result["answers"]["theme"]

    run = _run(staged, _serve_payload(payload))

    item = run.decision["items"][2]
    assert item["status"] == "refused"
    assert item["reasons"] == ["server_reported_truncation"]
    assert item["answers"] == []
    assert run.decision["coverage"]["ok"] == COMMENT_COUNT - 1


@pytest.mark.parametrize("run_id", ["run-1\n", "", "../escape", ".hidden"])
def test_annotate_invalid_run_id_is_refused_before_contacting_service(
    staged: Staged, run_id: str
) -> None:
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)

    with pytest.raises(DecisionArtifactError) as caught:
        _annotate(staged, client, run_id=run_id)

    assert caught.value.details["reason"] == "artifact_name_invalid"
    assert calls.total == 0
    assert not staged.output_root.exists()


def test_annotate_output_root_containing_protected_storage_is_refused(staged: Staged) -> None:
    calls = Calls()
    client = _client(_serve_text(staged.response_text), calls)
    store = staged.root / "store"
    job = _job(staged, output_root=store, protected_roots=(store / "normalized",))

    with pytest.raises(DecisionInputError) as caught:
        _annotate(staged, client, job=job)

    assert caught.value.details["reason"] == "output_contains_protected_storage"
    assert calls.total == 0


def test_publish_immutable_os_failure_raises_artifact_write_failed_without_temp_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse_link(source: object, target: object) -> None:
        raise PermissionError("hard links are not permitted here")

    monkeypatch.setattr(os, "link", refuse_link)
    target = tmp_path / "decisions" / "artifact.json"

    with pytest.raises(DecisionArtifactError) as caught:
        publish_immutable(target, b"content", reuse_identical=True)

    assert caught.value.details["reason"] == "artifact_write_failed"
    assert caught.value.details["error_type"] == "PermissionError"
    assert not target.exists()
    assert list(target.parent.iterdir()) == []
