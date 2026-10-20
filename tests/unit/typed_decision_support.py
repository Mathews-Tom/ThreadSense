"""Shared builders for typed-decision tests.

The recorded batch exchange in tests/fixtures/decisions/ came from the pinned local Laya
service; only its absolute checkpoint path was replaced. Tests serve it through httpx
MockTransport, which is engineering coverage of the protocol and never real-model evidence.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from threadsense.decisions.contracts import (
    CheckpointFile,
    CheckpointIdentity,
    CodeIdentity,
    RuntimeManifest,
    WireContract,
)

CANONICAL_FIXTURE = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
RECORDED_BATCH_FIXTURE = Path("tests/fixtures/decisions/recorded_feedback_batch.json")
ENDPOINT = "http://127.0.0.1:62635"
WIRE = WireContract(
    health_path="/health", batch_path="/v1/systemone/batch", model="english", language="en"
)
HEALTH_PAYLOAD: dict[str, Any] = {
    "status": "ok",
    "loaded": ["english"],
    "revisions": {"english": None},
    "device": "cpu",
    "device_is_preference": False,
    "checkpoint_devices": {"english": "cpu"},
    "cpu_fallbacks": {"english": {"count": 0, "last_reason": None}},
}
_CHECKPOINT_FILES = {
    "config.json": b'{"model_type": "test"}\n',
    "tokenizer/tokenizer.json": b'{"version": "test"}\n',
}

Handler = Callable[[httpx.Request], httpx.Response]


def write_checkpoint(directory: Path) -> Path:
    """Create a small checkpoint directory whose files the test manifest pins."""
    for relative, content in _CHECKPOINT_FILES.items():
        target = directory / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return directory


def build_manifest() -> RuntimeManifest:
    files = tuple(
        CheckpointFile(
            path=relative, bytes=len(content), sha256=hashlib.sha256(content).hexdigest()
        )
        for relative, content in _CHECKPOINT_FILES.items()
    )
    return RuntimeManifest(
        manifest_version=1,
        backend="laya",
        code=CodeIdentity(
            repository="https://github.com/NandhaKishorM/laya",
            commit="ffc142012af63d015fa19f05f29f9c5ff329a2cc",
            package="laya",
            version="0.4.12",
            license="Apache-2.0",
            identity_basis="declared_by_manifest_attested_by_reuse_procedure",
        ),
        checkpoint=CheckpointIdentity(
            repository="convaiinnovations/laya",
            revision="7b928d828b7b0e022f929d9bd2e44165aa270148",
            license="Apache-2.0",
            files=files,
            identity_basis="client_file_sha256",
        ),
        wire=WIRE,
    )


def recorded_batch(repo: Path) -> tuple[str, dict[str, Any]]:
    """Return the recorded request text and the response payload pointing at ``repo``."""
    fixture = json.loads(RECORDED_BATCH_FIXTURE.read_text(encoding="utf-8"))
    placeholder = fixture["provenance"]["repo_placeholder"]
    response_text = fixture["response_body"].replace(placeholder, str(repo))
    payload: dict[str, Any] = json.loads(response_text)
    return fixture["request_body"], payload


def json_response(payload: object, status_code: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code,
        content=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
    )


def routed_transport(health: Handler, batch: Handler) -> httpx.MockTransport:
    """Route GET /health and POST /v1/systemone/batch; anything else fails the test."""

    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == WIRE.health_path:
            return health(request)
        if request.method == "POST" and request.url.path == WIRE.batch_path:
            return batch(request)
        raise AssertionError(f"unexpected request {request.method} {request.url}")

    return httpx.MockTransport(handle)
