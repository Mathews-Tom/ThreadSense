"""Shipped `threadsense decisions annotate` over a recorded loopback transport.

The service side replays bytes recorded from the pinned local Laya service over a real socket.
That proves the client, CLI, identity and artifact wiring; it is not real-model evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from importlib import resources
from pathlib import Path, PurePosixPath
from typing import Any

import pytest
from recorded_laya import (
    BATCH_PATH,
    HEALTH_PATH,
    HEALTH_PAYLOAD,
    RecordedBatch,
    RecordedLayaServer,
    RecordedResponse,
    json_response,
    load_recorded_batch,
    serve_recorded_laya,
)

import threadsense.cli
from threadsense.cli import main
from threadsense.decisions.contracts import RuntimeManifest
from threadsense.decisions.runtime import (
    CheckpointVerifier,
    load_pinned_runtime_manifest,
    parse_runtime_manifest,
)
from threadsense.errors import DecisionIdentityError

REPO_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "analysis" / "canonical_feedback_thread.json"
RECORDED_COMMENT_COUNT = 7
SMALL_CHECKPOINT_FILES = {
    "config.json": b'{"model_type": "recorded"}\n',
    "tokenizer/tokenizer.json": b'{"version": "recorded"}\n',
}
PROXY_VARIABLES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)
NO_PROXY_VARIABLES = ("NO_PROXY", "no_proxy")

PACKAGED_FILES = {
    "config.json": (160, "7add7c041b53f7091e65908553af79ab3f077946e5f2312d510ae13b17c7c134"),
    "encoder/config.json": (
        2083,
        "bf3ab80598fdccf414855a2ce80f22859e4492d06ca8a62ddd1cfb63972f8979",
    ),
    "rl_agent_config.json": (
        745,
        "ae287b56bbcf5f8c4f4541ae9dfd00c914c4c48b940b8398c3058af37ba92bbd",
    ),
    "tokenizer/tokenizer.json": (
        3583228,
        "6c8aaa9a542084f2457eab775d4eeb51f92a70c0fd9de28d5edb0ddec3c08d30",
    ),
    "tokenizer/tokenizer_config.json": (
        308,
        "50044de60daaa73df97d262e15a40d4faf0160e7d742df64b377877a1320dd12",
    ),
    "model.safetensors": (
        842609210,
        "891102d372688fc2a094dac56a384bc537b87c63f21f9f3dac0be2b7cbc8d86c",
    ),
}
ABSOLUTE_PATH_MARKERS = ("/Users", "/var", "/tmp", "/private", "/home")


# --- builders ---------------------------------------------------------------------------------


def write_small_checkpoint(directory: Path) -> Path:
    for relative, content in SMALL_CHECKPOINT_FILES.items():
        target = directory / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return directory


def small_manifest() -> RuntimeManifest:
    """The pinned manifest shape, but pinned to the two small files of the temp checkpoint."""
    packaged = json.loads(
        resources.files("threadsense.decisions").joinpath("laya_runtime_manifest.json").read_bytes()
    )
    packaged["checkpoint"]["files"] = [
        {
            "path": relative,
            "bytes": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        for relative, content in SMALL_CHECKPOINT_FILES.items()
    ]
    return parse_runtime_manifest(packaged)


def unused_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_bytes())
    assert isinstance(payload, dict)
    return payload


def sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def batch_requests(server: RecordedLayaServer) -> list[bytes]:
    return [item.body for item in server.requests if item.path == BATCH_PATH]


def item_statuses(decision: dict[str, Any]) -> list[str]:
    return [item["status"] for item in decision["items"]]


# --- harness ----------------------------------------------------------------------------------


@dataclass
class AnnotateHarness:
    server: RecordedLayaServer
    recorded: RecordedBatch
    input_path: Path
    store_root: Path
    checkpoint: Path
    manifest: RuntimeManifest
    capsys: pytest.CaptureFixture[str]

    def run_cli(self, *argv: str) -> tuple[int, dict[str, Any]]:
        self.capsys.readouterr()
        exit_code = main(["--output-format", "json", *argv])
        payload = json.loads(self.capsys.readouterr().out)
        assert isinstance(payload, dict)
        return exit_code, payload

    def annotate(
        self,
        requirement: str = "required",
        *,
        endpoint: str | None = None,
        output_dir: Path | None = None,
        input_path: Path | None = None,
    ) -> tuple[int, dict[str, Any]]:
        argv = [
            "decisions",
            "annotate",
            "--input",
            str(input_path if input_path is not None else self.input_path),
            "--endpoint",
            endpoint if endpoint is not None else self.server.endpoint,
            "--requirement",
            requirement,
            "--domain",
            "developer_tools",
            "--objective",
            "friction_analysis",
        ]
        if output_dir is not None:
            argv += ["--output-dir", str(output_dir)]
        return self.run_cli(*argv)

    def set_batch(self, response: RecordedResponse) -> None:
        self.server.batch_response = response

    def set_recorded_batch(self, body: bytes) -> None:
        self.set_batch(RecordedResponse(200, "application/json", body))


@pytest.fixture
def harness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[AnnotateHarness]:
    checkpoint = write_small_checkpoint(tmp_path / "checkpoint")
    manifest = small_manifest()
    recorded = load_recorded_batch(checkpoint)
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    input_path = input_dir / "canonical_feedback_thread.json"
    input_path.write_bytes(CANONICAL_FIXTURE.read_bytes())
    store_root = tmp_path / "store"

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(store_root))
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "false")
    monkeypatch.delenv("THREADSENSE_STORAGE_ANNOTATIONS_DIR", raising=False)
    for name in (*PROXY_VARIABLES, *NO_PROXY_VARIABLES):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(threadsense.cli, "load_pinned_runtime_manifest", lambda: manifest)

    health = json_response(HEALTH_PAYLOAD)
    batch = RecordedResponse(200, "application/json", recorded.response_body)
    with serve_recorded_laya(health, batch) as server:
        yield AnnotateHarness(
            server=server,
            recorded=recorded,
            input_path=input_path,
            store_root=store_root,
            checkpoint=checkpoint,
            manifest=manifest,
            capsys=capsys,
        )


# --- 1. packaged manifest ---------------------------------------------------------------------


def test_packaged_manifest_pins_laya_code_and_checkpoint_identity() -> None:
    # Act
    manifest = load_pinned_runtime_manifest()

    # Assert
    assert manifest.backend == "laya"
    assert manifest.code.repository == "https://github.com/NandhaKishorM/laya"
    assert manifest.code.commit == "ffc142012af63d015fa19f05f29f9c5ff329a2cc"
    assert manifest.code.package == "laya"
    assert manifest.code.version == "0.4.12"
    assert manifest.code.license == "Apache-2.0"
    assert manifest.checkpoint.repository == "convaiinnovations/laya"
    assert manifest.checkpoint.revision == "7b928d828b7b0e022f929d9bd2e44165aa270148"
    assert manifest.checkpoint.license == "Apache-2.0"


def test_packaged_manifest_lists_exactly_the_six_pinned_checkpoint_files() -> None:
    # Act
    manifest = load_pinned_runtime_manifest()

    # Assert
    observed = [(item.path, item.bytes, item.sha256) for item in manifest.checkpoint.files]
    expected = [(path, size, digest) for path, (size, digest) in PACKAGED_FILES.items()]
    assert sorted(observed) == sorted(expected)
    assert len(observed) == len(PACKAGED_FILES)


def test_packaged_manifest_pins_the_wire_contract() -> None:
    # Act
    wire = load_pinned_runtime_manifest().wire

    # Assert
    assert wire.health_path == "/health"
    assert wire.batch_path == "/v1/systemone/batch"
    assert wire.model == "english"
    assert wire.language == "en"


def test_packaged_manifest_bytes_contain_no_absolute_path_and_only_relative_files() -> None:
    # Arrange
    raw = (
        resources.files("threadsense.decisions").joinpath("laya_runtime_manifest.json").read_bytes()
    )

    # Act
    text = raw.decode("utf-8")
    files = json.loads(text)["checkpoint"]["files"]

    # Assert
    assert [marker for marker in ABSOLUTE_PATH_MARKERS if marker in text] == []
    for item in files:
        relative = PurePosixPath(item["path"])
        assert not relative.is_absolute()
        assert ".." not in relative.parts
        assert "\\" not in item["path"]


def test_packaged_manifest_refuses_wrong_size_checkpoint_files(tmp_path: Path) -> None:
    # Arrange: a real-looking checkpoint directory with the six pinned names and wrong content
    checkpoint = tmp_path / "lookalike"
    for relative in PACKAGED_FILES:
        target = checkpoint / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"not the pinned bytes\n")
    verifier = CheckpointVerifier(load_pinned_runtime_manifest())

    # Act
    with pytest.raises(DecisionIdentityError) as raised:
        verifier.verify(str(checkpoint))

    # Assert
    assert raised.value.details["reason"] == "checkpoint_file_size_mismatch"


def test_packaged_manifest_refuses_right_size_wrong_digest_checkpoint_file(
    tmp_path: Path,
) -> None:
    # Arrange: config.json has the pinned size (160 bytes) but different content
    checkpoint = tmp_path / "lookalike"
    for relative, (size, _digest) in PACKAGED_FILES.items():
        target = checkpoint / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x" * size if relative == "config.json" else b"")
    verifier = CheckpointVerifier(load_pinned_runtime_manifest())

    # Act
    with pytest.raises(DecisionIdentityError) as raised:
        verifier.verify(str(checkpoint))

    # Assert
    assert raised.value.details["reason"] == "checkpoint_file_digest_mismatch"
    assert raised.value.details["path"] == "config.json"


# --- 2. required success over the recorded server ---------------------------------------------


def test_annotate_required_recorded_service_returns_complete_satisfied_payload(
    harness: AnnotateHarness,
) -> None:
    # Arrange
    input_before = harness.input_path.read_bytes()

    # Act
    exit_code, payload = harness.annotate("required")

    # Assert
    assert exit_code == 0
    assert payload["artifact_type"] == "decision_annotations"
    assert payload["mode"] == "shadow"
    assert payload["status"] == "complete"
    assert payload["requirement"] == "required"
    assert payload["satisfied"] is True
    assert payload["coverage"] == {
        "status": "complete",
        "total": RECORDED_COMMENT_COUNT,
        "ok": RECORDED_COMMENT_COUNT,
        "refused": 0,
        "unavailable": 0,
    }
    assert payload["snapshot_sha256"] == sha256_hex(input_before)
    assert payload["manifest_sha256"] == harness.manifest.sha256()
    assert harness.input_path.read_bytes() == input_before


def test_annotate_required_recorded_service_publishes_hash_named_decision_with_ok_items(
    harness: AnnotateHarness,
) -> None:
    # Act
    _exit_code, payload = harness.annotate("required")

    # Assert
    decision_path = Path(payload["decision_path"])
    decision_bytes = decision_path.read_bytes()
    decision = json.loads(decision_bytes)
    assert sha256_hex(decision_bytes) == payload["decision_sha256"]
    assert decision_path.name == f"{payload['decision_sha256']}.json"
    assert Path(payload["run_path"]).is_file()
    assert item_statuses(decision) == ["ok"] * RECORDED_COMMENT_COUNT
    assert all(item["answers"] for item in decision["items"])
    assert decision["coverage"] == payload["coverage"]


def test_annotate_required_sends_one_health_get_and_the_recorded_request_bytes(
    harness: AnnotateHarness,
) -> None:
    # Act
    harness.annotate("required")

    # Assert
    received = harness.server.requests
    assert [(item.method, item.path) for item in received] == [
        ("GET", HEALTH_PATH),
        ("POST", BATCH_PATH),
    ]
    assert received[1].body == harness.recorded.request_body


def test_annotate_without_output_dir_publishes_under_storage_annotations(
    harness: AnnotateHarness,
) -> None:
    # Act
    _exit_code, payload = harness.annotate("required")

    # Assert
    annotations_root = (harness.store_root / "annotations").resolve()
    assert Path(payload["decision_path"]).resolve().is_relative_to(annotations_root)
    assert Path(payload["run_path"]).resolve().is_relative_to(annotations_root)


def test_annotate_explicit_output_dir_overrides_storage_annotations(
    harness: AnnotateHarness, tmp_path: Path
) -> None:
    # Arrange
    output_dir = tmp_path / "custom-annotations"

    # Act
    exit_code, payload = harness.annotate("required", output_dir=output_dir)

    # Assert
    assert exit_code == 0
    assert Path(payload["decision_path"]).resolve().is_relative_to(output_dir.resolve())
    assert not (harness.store_root / "annotations").exists()


def test_annotate_repeated_run_reuses_the_identical_decision_artifact(
    harness: AnnotateHarness,
) -> None:
    # Act
    _first_exit, first = harness.annotate("required")
    _second_exit, second = harness.annotate("required")

    # Assert
    assert first["decision_sha256"] == second["decision_sha256"]
    assert first["decision_path"] == second["decision_path"]
    assert first["run_id"] != second["run_id"]
    assert first["run_path"] != second["run_path"]


# --- 3. proxy environment ignored -------------------------------------------------------------


def test_annotate_ignores_proxy_environment_and_reaches_the_loopback_service(
    harness: AnnotateHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange: every proxy variable points at a discard port nothing listens on
    for name in PROXY_VARIABLES:
        monkeypatch.setenv(name, "http://127.0.0.1:9")

    # Act
    exit_code, payload = harness.annotate("required")

    # Assert
    assert exit_code == 0
    assert payload["coverage"]["status"] == "complete"
    assert len(batch_requests(harness.server)) == 1


# --- 4. unavailable runtime -------------------------------------------------------------------


def test_annotate_optional_closed_port_exits_zero_with_all_items_unavailable(
    harness: AnnotateHarness,
) -> None:
    # Arrange
    endpoint = f"http://127.0.0.1:{unused_loopback_port()}"

    # Act
    exit_code, payload = harness.annotate("optional", endpoint=endpoint)

    # Assert
    assert exit_code == 0
    assert payload["satisfied"] is True
    assert payload["status"] == "unavailable"
    assert payload["coverage"] == {
        "status": "unavailable",
        "total": RECORDED_COMMENT_COUNT,
        "ok": 0,
        "refused": 0,
        "unavailable": RECORDED_COMMENT_COUNT,
    }
    decision = read_json(Path(payload["decision_path"]))
    assert item_statuses(decision) == ["unavailable"] * RECORDED_COMMENT_COUNT
    assert all(item["answers"] == [] for item in decision["items"])
    assert all(item["reasons"] == ["runtime_health_unavailable"] for item in decision["items"])
    assert harness.server.requests == []


def test_annotate_required_closed_port_exits_one_and_still_publishes_artifact(
    harness: AnnotateHarness,
) -> None:
    # Arrange
    endpoint = f"http://127.0.0.1:{unused_loopback_port()}"

    # Act
    exit_code, payload = harness.annotate("required", endpoint=endpoint)

    # Assert
    assert exit_code == 1
    assert payload["satisfied"] is False
    assert payload["coverage"]["status"] == "unavailable"
    assert Path(payload["decision_path"]).is_file()
    assert Path(payload["run_path"]).is_file()


def test_annotate_health_not_ok_marks_items_unavailable_without_a_batch_request(
    harness: AnnotateHarness,
) -> None:
    # Arrange
    harness.server.health_response = json_response({**HEALTH_PAYLOAD, "status": "loading"})

    # Act
    exit_code, payload = harness.annotate("required")

    # Assert
    assert exit_code == 1
    assert payload["coverage"]["status"] == "unavailable"
    decision = read_json(Path(payload["decision_path"]))
    assert all(item["failure"]["reason"] == "health_not_ready" for item in decision["items"])
    assert batch_requests(harness.server) == []


# --- 5. status and protocol failures through a real socket ------------------------------------


def test_annotate_batch_503_marks_items_unavailable_with_http_status_failure(
    harness: AnnotateHarness,
) -> None:
    # Arrange
    harness.set_batch(json_response({"detail": "busy"}, status=503))

    # Act
    exit_code, payload = harness.annotate("required")

    # Assert
    assert exit_code == 1
    assert payload["satisfied"] is False
    decision = read_json(Path(payload["decision_path"]))
    assert item_statuses(decision) == ["unavailable"] * RECORDED_COMMENT_COUNT
    for item in decision["items"]:
        assert item["reasons"] == ["runtime_request_failed"]
        assert item["failure"]["reason"] == "http_status"
        assert item["answers"] == []
    assert decision["exchanges"][0]["outcome"] == "error"


def test_annotate_batch_redirect_is_not_followed(harness: AnnotateHarness) -> None:
    # Arrange
    harness.set_batch(RecordedResponse(302, None, b"", (("Location", "/elsewhere"),)))

    # Act
    exit_code, payload = harness.annotate("required")

    # Assert
    assert exit_code == 1
    paths = [(item.method, item.path) for item in harness.server.requests]
    assert paths == [("GET", HEALTH_PATH), ("POST", BATCH_PATH)]
    decision = read_json(Path(payload["decision_path"]))
    assert item_statuses(decision) == ["unavailable"] * RECORDED_COMMENT_COUNT
    assert all(item["failure"]["reason"] == "http_status" for item in decision["items"])


def test_annotate_non_json_content_type_marks_items_unavailable_invalid_response(
    harness: AnnotateHarness,
) -> None:
    # Arrange
    harness.set_batch(RecordedResponse(200, "text/plain", harness.recorded.response_body))

    # Act
    exit_code, payload = harness.annotate("required")

    # Assert
    assert exit_code == 1
    decision = read_json(Path(payload["decision_path"]))
    assert item_statuses(decision) == ["unavailable"] * RECORDED_COMMENT_COUNT
    for item in decision["items"]:
        assert item["reasons"] == ["invalid_response"]
        assert item["failure"]["reason"] == "unexpected_content_type"


def test_annotate_nan_in_response_body_marks_items_unavailable_invalid_response(
    harness: AnnotateHarness,
) -> None:
    # Arrange
    harness.set_recorded_batch(b'{"results": [{"usage": NaN}], "total_usage": {}}')

    # Act
    exit_code, payload = harness.annotate("required")

    # Assert
    assert exit_code == 1
    decision = read_json(Path(payload["decision_path"]))
    for item in decision["items"]:
        assert item["status"] == "unavailable"
        assert item["reasons"] == ["invalid_response"]
        assert item["failure"]["reason"] == "invalid_json"
        assert item["answers"] == []


def test_annotate_result_count_mismatch_marks_items_unavailable_invalid_response(
    harness: AnnotateHarness,
) -> None:
    # Arrange
    recorded = json.loads(harness.recorded.response_body)
    recorded["results"] = recorded["results"][:-1]
    harness.set_batch(json_response(recorded))

    # Act
    exit_code, payload = harness.annotate("required")

    # Assert
    assert exit_code == 1
    decision = read_json(Path(payload["decision_path"]))
    for item in decision["items"]:
        assert item["reasons"] == ["invalid_response"]
        assert item["failure"]["reason"] == "result_count_mismatch"


# --- 6. coverage refusal ----------------------------------------------------------------------


def dropped_state_tokens_response(body: bytes) -> RecordedResponse:
    payload = json.loads(body)
    payload["results"][0]["usage"]["state_tokens_dropped"] = 3
    return json_response(payload)


@pytest.mark.parametrize(("requirement", "expected_exit"), [("required", 1), ("optional", 0)])
def test_annotate_server_dropped_state_tokens_refuses_only_that_item(
    harness: AnnotateHarness, requirement: str, expected_exit: int
) -> None:
    # Arrange
    harness.set_batch(dropped_state_tokens_response(harness.recorded.response_body))

    # Act
    exit_code, payload = harness.annotate(requirement)

    # Assert
    assert exit_code == expected_exit
    assert payload["satisfied"] is (requirement == "optional")
    assert payload["status"] == "partial"
    assert payload["coverage"] == {
        "status": "partial",
        "total": RECORDED_COMMENT_COUNT,
        "ok": RECORDED_COMMENT_COUNT - 1,
        "refused": 1,
        "unavailable": 0,
    }
    decision = read_json(Path(payload["decision_path"]))
    first, *others = decision["items"]
    assert first["status"] == "refused"
    assert first["reasons"] == ["server_dropped_state_tokens"]
    assert first["answers"] == []
    assert [item["status"] for item in others] == ["ok"] * (RECORDED_COMMENT_COUNT - 1)
    assert all(item["answers"] for item in others)


# --- 7. identity ------------------------------------------------------------------------------


def test_annotate_checkpoint_changed_after_manifest_creation_marks_items_unavailable(
    harness: AnnotateHarness,
) -> None:
    # Arrange: same length, different bytes, so only the digest can catch it
    config = harness.checkpoint / "config.json"
    original = config.read_bytes()
    config.write_bytes(b"x" * len(original))

    # Act
    exit_code, payload = harness.annotate("required")

    # Assert
    assert exit_code == 1
    assert payload["satisfied"] is False
    assert payload["coverage"]["status"] == "unavailable"
    decision = read_json(Path(payload["decision_path"]))
    for item in decision["items"]:
        assert item["status"] == "unavailable"
        assert item["reasons"] == ["checkpoint_identity_unverified"]
        assert item["failure"]["reason"] == "checkpoint_file_digest_mismatch"
        assert item["answers"] == []
    assert decision["backend"]["checkpoint_verification"] is None


def test_annotate_verified_run_records_client_observed_checkpoint_digests(
    harness: AnnotateHarness,
) -> None:
    # Act
    _exit_code, payload = harness.annotate("required")

    # Assert
    decision = read_json(Path(payload["decision_path"]))
    verification = decision["backend"]["checkpoint_verification"]
    observed = {item["path"]: item["sha256"] for item in verification["files"]}
    assert observed == {
        relative: sha256_hex(content) for relative, content in SMALL_CHECKPOINT_FILES.items()
    }
    assert verification["basis"] == "client_file_sha256"


# --- 8. refusals before any request -----------------------------------------------------------


def test_annotate_hostname_endpoint_is_refused_before_any_request(
    harness: AnnotateHarness,
) -> None:
    # Act
    exit_code, payload = harness.annotate(endpoint=f"http://localhost:{harness.server.port}")

    # Assert
    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["error"]["details"]["reason"] == "endpoint_not_literal_loopback"
    assert harness.server.requests == []
    assert not (harness.store_root / "annotations").exists()


def test_annotate_https_endpoint_is_refused_before_any_request(harness: AnnotateHarness) -> None:
    # Act
    exit_code, payload = harness.annotate(endpoint=f"https://127.0.0.1:{harness.server.port}")

    # Assert
    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["error"]["details"]["reason"] == "endpoint_invalid"
    assert harness.server.requests == []
    assert not (harness.store_root / "annotations").exists()


@pytest.mark.parametrize("protected", ["normalized", "monitoring"])
def test_annotate_output_inside_protected_storage_is_refused_and_creates_nothing(
    harness: AnnotateHarness, protected: str
) -> None:
    # Arrange
    output_dir = harness.store_root / protected / "annotations"

    # Act
    exit_code, payload = harness.annotate(output_dir=output_dir)

    # Assert
    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["error"]["details"]["reason"] == "output_inside_protected_storage"
    assert not output_dir.exists()
    assert not (harness.store_root / protected).exists()
    assert harness.server.requests == []


def test_annotate_output_dir_equal_to_input_file_is_refused_as_alias(
    harness: AnnotateHarness,
) -> None:
    # Arrange
    input_before = harness.input_path.read_bytes()

    # Act
    exit_code, payload = harness.annotate(output_dir=harness.input_path)

    # Assert
    assert exit_code == 1
    assert payload["error"]["details"]["reason"] == "output_aliases_input"
    assert harness.input_path.read_bytes() == input_before
    assert harness.server.requests == []


def test_annotate_escaping_annotations_dirname_is_refused_without_output_dir(
    harness: AnnotateHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.setenv("THREADSENSE_STORAGE_ANNOTATIONS_DIR", "../escape")

    # Act
    exit_code, payload = harness.annotate()

    # Assert
    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["error"]["details"]["reason"] == "annotations_dirname_invalid"
    assert not (harness.store_root.parent / "escape").exists()
    assert harness.server.requests == []


def test_annotate_missing_input_is_refused_before_any_request(
    harness: AnnotateHarness, tmp_path: Path
) -> None:
    # Act
    exit_code, payload = harness.annotate(input_path=tmp_path / "absent.json")

    # Assert
    assert exit_code == 1
    assert payload["error"]["details"]["reason"] == "input_unreadable"
    assert harness.server.requests == []


# --- 9. baseline unchanged --------------------------------------------------------------------


def test_annotate_leaves_baseline_analysis_bytes_and_findings_unchanged(
    harness: AnnotateHarness, tmp_path: Path
) -> None:
    # Arrange
    baseline = tmp_path / "analysis.json"
    analyze_exit, _ = harness.run_cli(
        "analyze", "normalized", "--input", str(harness.input_path), "--output", str(baseline)
    )
    assert analyze_exit == 0
    baseline_bytes = baseline.read_bytes()

    # Act
    annotate_exit, payload = harness.annotate("required")
    fresh = tmp_path / "analysis-after.json"
    fresh_exit, _ = harness.run_cli(
        "analyze", "normalized", "--input", str(harness.input_path), "--output", str(fresh)
    )

    # Assert
    assert annotate_exit == 0
    assert payload["satisfied"] is True
    assert fresh_exit == 0
    assert baseline.read_bytes() == baseline_bytes
    before = json.loads(baseline_bytes)["analysis"]
    after = read_json(fresh)["analysis"]
    assert after["findings"] == before["findings"]
    assert after["duplicate_groups"] == before["duplicate_groups"]
    assert after["top_quotes"] == before["top_quotes"]


# --- 10. core without a model stack -----------------------------------------------------------


def core_environment() -> dict[str, str]:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(REPO_ROOT / "src")
    return environment


CORE_IMPORT_PROBE = """
import importlib.util
import sys

import threadsense.cli
import threadsense.decisions.annotate

banned = ("torch", "transformers", "numpy", "fastapi")
loaded = [name for name in banned if name in sys.modules]
assert not loaded, loaded
for name in ("torch", "transformers"):
    assert importlib.util.find_spec(name) is None, name
"""


def test_core_import_of_cli_and_annotate_loads_no_model_stack() -> None:
    # Act
    result = subprocess.run(
        [sys.executable, "-c", CORE_IMPORT_PROBE],
        env=core_environment(),
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    # Assert
    assert result.returncode == 0, result.stderr


def test_module_entrypoint_decisions_annotate_help_exits_zero() -> None:
    # Act
    result = subprocess.run(
        [sys.executable, "-m", "threadsense", "decisions", "annotate", "--help"],
        env=core_environment(),
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    # Assert
    assert result.returncode == 0, result.stderr
    assert "--endpoint" in result.stdout
