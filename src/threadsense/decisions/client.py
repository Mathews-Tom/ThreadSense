"""Typed decision HTTP client for the separate local Laya service.

Not a chat-completion client: one request carries states plus typed questions, and every
transport or status failure is surfaced as-is. There are no retries and no fallbacks.
"""

from __future__ import annotations

import ipaddress
import math
import time
from collections.abc import Callable
from types import TracebackType
from typing import Any
from urllib.parse import urlsplit

import httpx

from threadsense.decisions.contracts import (
    MAX_BATCH_STATES,
    MAX_BODY_BYTES,
    MAX_QUESTIONS,
    HttpExchange,
    WireContract,
    decode_strict_json_object,
    encode_request_body,
)
from threadsense.errors import (
    DecisionInputError,
    DecisionProtocolError,
    DecisionRuntimeUnavailableError,
)

_FORBIDDEN_CONTROLS = ("sort_by_length", "batch_size", "min_confidence")
_EXCERPT_CHARS = 2000


def _endpoint_error(reason: str, endpoint: str) -> DecisionInputError:
    return DecisionInputError(
        "typed decision endpoint must be an http literal-loopback address with a port",
        {"reason": reason, "endpoint": endpoint},
    )


def validate_loopback_endpoint(endpoint: str) -> str:
    """Return the normalized base URL; DNS names (including localhost) are refused."""
    if any(char.isspace() for char in endpoint) or "?" in endpoint or "#" in endpoint:
        raise _endpoint_error("endpoint_invalid", endpoint)
    try:
        parts = urlsplit(endpoint)
        port = parts.port
        hostname = parts.hostname
    except ValueError as error:
        raise _endpoint_error("endpoint_invalid", endpoint) from error
    if parts.scheme != "http" or "@" in parts.netloc or parts.path not in ("", "/"):
        raise _endpoint_error("endpoint_invalid", endpoint)
    if parts.query or parts.fragment or port is None or port == 0 or hostname is None:
        raise _endpoint_error("endpoint_invalid", endpoint)
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError as error:
        raise _endpoint_error("endpoint_not_literal_loopback", endpoint) from error
    if not address.is_loopback:
        raise _endpoint_error("endpoint_not_literal_loopback", endpoint)
    host = f"[{address}]" if address.version == 6 else str(address)
    return f"http://{host}:{port}"


def _input_error(message: str, reason: str, **extra: Any) -> DecisionInputError:
    return DecisionInputError(message, {"reason": reason, **extra})


def _failure_reason(error: httpx.HTTPError | httpx.StreamError | httpx.InvalidURL) -> str:
    if isinstance(error, httpx.TimeoutException):
        return "timeout"
    if isinstance(error, httpx.ConnectError):
        return "connect_failed"
    if isinstance(error, httpx.RemoteProtocolError | httpx.LocalProtocolError):
        return "protocol_failure"
    if isinstance(error, httpx.TransportError):
        return "transport_failure"
    return "http_failure"


class TypedDecisionClient:
    def __init__(
        self,
        endpoint: str,
        *,
        wire: WireContract,
        timeout_seconds: float,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._endpoint = validate_loopback_endpoint(endpoint)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int | float)
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise _input_error(
                "typed decision timeout must be a finite number above zero",
                "timeout_invalid",
                timeout_seconds=repr(timeout_seconds),
            )
        self._wire = wire
        self._clock = clock
        self._client = httpx.Client(
            base_url=self._endpoint,
            timeout=httpx.Timeout(float(timeout_seconds)),
            trust_env=False,
            follow_redirects=False,
            transport=transport if transport is not None else httpx.HTTPTransport(retries=0),
        )

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> TypedDecisionClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def health(self) -> HttpExchange:
        return self._exchange("GET", self._wire.health_path, None)

    def post_batch(self, body: dict[str, Any]) -> HttpExchange:
        encoded = self._checked_batch_body(body)
        return self._exchange("POST", self._wire.batch_path, encoded)

    def _checked_batch_body(self, body: dict[str, Any]) -> bytes:
        forbidden = [key for key in _FORBIDDEN_CONTROLS if key in body]
        if forbidden:
            raise _input_error(
                "request carries a forbidden server control",
                "forbidden_request_control",
                controls=forbidden,
            )
        if body.get("model") != self._wire.model:
            raise _input_error(
                "request model differs from the pinned wire model",
                "request_model_mismatch",
                expected=self._wire.model,
                observed=body.get("model"),
            )
        states = body.get("states")
        if not isinstance(states, list) or not 1 <= len(states) <= MAX_BATCH_STATES:
            raise _input_error(
                "request states are out of bounds",
                "request_states_out_of_bounds",
                limit=MAX_BATCH_STATES,
            )
        questions = body.get("questions")
        if not isinstance(questions, dict) or not 1 <= len(questions) <= MAX_QUESTIONS:
            raise _input_error(
                "request questions are out of bounds",
                "request_questions_out_of_bounds",
                limit=MAX_QUESTIONS,
            )
        encoded = encode_request_body(body)
        if len(encoded) > MAX_BODY_BYTES:
            raise _input_error(
                "request body exceeds the server limit",
                "request_body_too_large",
                bytes=len(encoded),
                limit=MAX_BODY_BYTES,
            )
        return encoded

    def _send(self, method: str, path: str, content: bytes | None) -> httpx.Response:
        if content is None:
            return self._client.request(method, path, headers={"Accept": "application/json"})
        return self._client.request(
            method,
            path,
            content=content,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )

    def _exchange(self, method: str, path: str, content: bytes | None) -> HttpExchange:
        started = self._clock()
        try:
            response = self._send(method, path, content)
        except (httpx.HTTPError, httpx.StreamError, httpx.InvalidURL) as error:
            raise DecisionRuntimeUnavailableError(
                "typed decision service request failed",
                {
                    "reason": _failure_reason(error),
                    "method": method,
                    "path": path,
                    "endpoint": self._endpoint,
                    "error_type": type(error).__name__,
                    "error": str(error),
                    "wall_seconds": self._clock() - started,
                },
            ) from error
        wall_seconds = self._clock() - started
        if response.status_code != 200:
            raise DecisionRuntimeUnavailableError(
                "typed decision service returned a non-200 status",
                {
                    "reason": "http_status",
                    "method": method,
                    "path": path,
                    "endpoint": self._endpoint,
                    "status_code": response.status_code,
                    "response_excerpt": response.text[:_EXCERPT_CHARS],
                    "wall_seconds": wall_seconds,
                },
            )
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith("application/json"):
            raise DecisionProtocolError(
                "typed decision response is not JSON",
                {
                    "reason": "unexpected_content_type",
                    "method": method,
                    "path": path,
                    "content_type": content_type,
                    "wall_seconds": wall_seconds,
                },
            )
        try:
            payload = decode_strict_json_object(
                response.content, DecisionProtocolError, "typed decision response"
            )
        except DecisionProtocolError as error:
            error.details.update({"method": method, "path": path, "wall_seconds": wall_seconds})
            raise
        return HttpExchange(
            method=method,
            path=path,
            status_code=response.status_code,
            request_body=None if content is None else content.decode("utf-8"),
            response_body=response.content.decode("utf-8"),
            payload=payload,
            wall_seconds=wall_seconds,
        )
