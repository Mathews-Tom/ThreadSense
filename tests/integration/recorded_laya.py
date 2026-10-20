"""Recorded loopback transport for the typed-decision service.

The batch response body comes from a real recorded exchange with the pinned local Laya service
(``tests/fixtures/decisions/recorded_feedback_batch.json``); only the service's absolute
checkpoint directory was replaced by a placeholder. This module replays those bytes over a real
loopback socket. It exercises the shipped client and CLI wire path deterministically and is not
evidence about any model.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread

RECORDED_BATCH_FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "decisions" / "recorded_feedback_batch.json"
)
HEALTH_PATH = "/health"
BATCH_PATH = "/v1/systemone/batch"
HEALTH_PAYLOAD: dict[str, object] = {
    "status": "ok",
    "loaded": ["english"],
    "revisions": {"english": None},
    "device": "cpu",
    "device_is_preference": False,
    "checkpoint_devices": {"english": "cpu"},
    "cpu_fallbacks": {"english": {"count": 0, "last_reason": None}},
}


@dataclass(frozen=True)
class RecordedBatch:
    """Recorded request bytes and the recorded response bytes aimed at one checkpoint dir."""

    request_body: bytes
    response_body: bytes


def load_recorded_batch(repo: Path) -> RecordedBatch:
    fixture = json.loads(RECORDED_BATCH_FIXTURE.read_text(encoding="utf-8"))
    placeholder = fixture["provenance"]["repo_placeholder"]
    escaped_repo = json.dumps(str(repo))[1:-1]
    response_text = fixture["response_body"].replace(placeholder, escaped_repo)
    return RecordedBatch(
        request_body=fixture["request_body"].encode("utf-8"),
        response_body=response_text.encode("utf-8"),
    )


@dataclass(frozen=True)
class RecordedResponse:
    status: int
    content_type: str | None
    body: bytes
    headers: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class ReceivedRequest:
    method: str
    path: str
    body: bytes


def json_response(payload: object, status: int = 200) -> RecordedResponse:
    return RecordedResponse(status, "application/json", json.dumps(payload).encode("utf-8"))


@dataclass
class RecordedLayaServer:
    """Serves GET /health and POST /v1/systemone/batch; records every request it receives."""

    endpoint: str
    health_response: RecordedResponse
    batch_response: RecordedResponse
    received: list[ReceivedRequest] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock)

    def record(self, request: ReceivedRequest) -> None:
        with self._lock:
            self.received.append(request)

    @property
    def requests(self) -> list[ReceivedRequest]:
        with self._lock:
            return list(self.received)

    @property
    def port(self) -> int:
        return int(self.endpoint.rsplit(":", 1)[1])


def _handler_for(server: RecordedLayaServer) -> type[BaseHTTPRequestHandler]:
    class RecordedHandler(BaseHTTPRequestHandler):
        def _send(self, response: RecordedResponse) -> None:
            self.send_response(response.status)
            if response.content_type is not None:
                self.send_header("Content-Type", response.content_type)
            for name, value in response.headers:
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(response.body)))
            self.end_headers()
            self.wfile.write(response.body)

        def _route(self, body: bytes) -> None:
            server.record(ReceivedRequest(self.command, self.path, body))
            if self.command == "GET" and self.path == HEALTH_PATH:
                self._send(server.health_response)
            elif self.command == "POST" and self.path == BATCH_PATH:
                self._send(server.batch_response)
            else:
                self._send(RecordedResponse(404, "text/plain", b"not found"))

        def do_GET(self) -> None:
            self._route(b"")

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            self._route(self.rfile.read(length))

        def log_message(self, format: str, *args: object) -> None:
            return None

    return RecordedHandler


@contextmanager
def serve_recorded_laya(
    health_response: RecordedResponse, batch_response: RecordedResponse
) -> Iterator[RecordedLayaServer]:
    placeholder = RecordedLayaServer("", health_response, batch_response)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _handler_for(placeholder))
    placeholder.endpoint = f"http://127.0.0.1:{httpd.server_address[1]}"
    thread = Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield placeholder
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join()
