from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, Any
from urllib import error, request

from threadsense.config import RuntimeConfig
from threadsense.errors import InferenceBoundaryError, NetworkBoundaryError, SchemaBoundaryError
from threadsense.inference.contracts import (
    InferenceMessage,
    InferenceRequest,
    InferenceResponse,
    validate_task_output,
)

if TYPE_CHECKING:
    from threadsense.models.analysis import ThreadAnalysis

JsonObject = dict[str, Any]
JsonRequest = Callable[[str, JsonObject, float], tuple[int, JsonObject]]


@dataclass(frozen=True)
class RuntimeProbeResult:
    ok: bool
    endpoint: str
    model: str
    status_code: int | None
    latency_ms: float | None
    response_id: str | None
    response_model: str | None
    finish_reason: str | None
    stream: bool
    error: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "endpoint": self.endpoint,
            "model": self.model,
            "status_code": self.status_code,
            "latency_ms": self.latency_ms,
            "response_id": self.response_id,
            "response_model": self.response_model,
            "finish_reason": self.finish_reason,
            "stream": self.stream,
            "error": self.error,
        }


class LocalRuntimeClient:
    def __init__(self, config: RuntimeConfig) -> None:
        self._config = config

    def build_probe_payload(self) -> JsonObject:
        return self.build_chat_payload(
            messages=[
                InferenceMessage(role="system", content="Return the single token READY."),
                InferenceMessage(role="user", content="READY"),
            ],
            temperature=0,
        )

    def build_chat_payload(
        self,
        messages: list[InferenceMessage],
        temperature: float,
    ) -> JsonObject:
        payload: JsonObject = {
            "model": self._config.model,
            "messages": [
                {"role": message.role, "content": message.content} for message in messages
            ],
            "stream": False,
            "temperature": temperature,
        }
        if self._config.json_mode:
            payload["response_format"] = {"type": "json_object"}
        return payload

    def probe(self, opener: JsonRequest | None = None) -> RuntimeProbeResult:
        request_fn = opener or send_json_request
        started_at = perf_counter()
        payload = self.build_probe_payload()
        try:
            status_code, response_body = request_fn(
                self._config.chat_endpoint,
                payload,
                self._config.timeout_seconds,
            )
            parsed = validate_chat_completion_response(response_body)
        except (InferenceBoundaryError, NetworkBoundaryError, SchemaBoundaryError) as error:
            return RuntimeProbeResult(
                ok=False,
                endpoint=self._config.chat_endpoint,
                model=self._config.model,
                status_code=error.details.get("status_code")
                if isinstance(error.details.get("status_code"), int)
                else None,
                latency_ms=round((perf_counter() - started_at) * 1000, 2),
                response_id=None,
                response_model=None,
                finish_reason=None,
                stream=False,
                error=str(error),
            )

        latency_ms = round((perf_counter() - started_at) * 1000, 2)
        return RuntimeProbeResult(
            ok=True,
            endpoint=self._config.chat_endpoint,
            model=self._config.model,
            status_code=status_code,
            latency_ms=latency_ms,
            response_id=parsed["id"],
            response_model=parsed["model"],
            finish_reason=parsed["choices"][0]["finish_reason"],
            stream=False,
            error=None,
        )

    def complete(
        self,
        inference_request: InferenceRequest,
        opener: JsonRequest | None = None,
        *,
        analysis: ThreadAnalysis | None = None,
    ) -> InferenceResponse:
        request_fn = opener or send_json_request
        messages = list(inference_request.messages)
        effective_retries = 0 if self._config.json_mode else inference_request.repair_retries
        for attempt in range(effective_retries + 1):
            payload = self.build_chat_payload(messages=messages, temperature=0)
            _status_code, response_body = request_fn(
                self._config.chat_endpoint,
                payload,
                self._config.timeout_seconds,
            )
            parsed = validate_chat_completion_response(response_body)
            content = extract_message_content(parsed)
            try:
                output = validate_task_output(
                    inference_request.task,
                    parse_structured_output(content),
                    analysis=analysis,
                )
            except SchemaBoundaryError:
                if attempt >= inference_request.repair_retries:
                    raise
                messages = repair_messages(
                    existing=messages,
                    invalid_content=content,
                    repair_instruction=inference_request.repair_instruction,
                )
                continue

            return InferenceResponse(
                task=inference_request.task,
                provider="local_openai_compatible",
                model=parsed["model"],
                finish_reason=parsed["choices"][0]["finish_reason"],
                output=output,
                used_fallback=False,
                degraded=False,
                failure_reason=None,
            )

        raise InferenceBoundaryError("inference request exhausted repair attempts")


def send_json_request(
    url: str,
    payload: JsonObject,
    timeout_seconds: float,
) -> tuple[int, JsonObject]:
    body = json.dumps(payload).encode("utf-8")
    http_request = request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with request.urlopen(http_request, timeout=timeout_seconds) as response:
            raw_body = response.read().decode("utf-8")
            parsed = json.loads(raw_body)
            if not isinstance(parsed, dict):
                raise SchemaBoundaryError("runtime response must be a JSON object")
            return response.status, parsed
    except error.HTTPError as http_error:
        response_body = http_error.read().decode("utf-8", errors="replace")
        raise InferenceBoundaryError(
            "runtime rejected chat completion request",
            details={
                "status_code": http_error.code,
                "url": url,
                "response_body": response_body,
            },
        ) from http_error
    except error.URLError as url_error:
        raise NetworkBoundaryError(
            "runtime endpoint is unreachable",
            details={"url": url, "reason": str(url_error.reason)},
        ) from url_error
    except TimeoutError as timeout_error:
        raise NetworkBoundaryError(
            "runtime request timed out",
            details={"url": url, "timeout_seconds": timeout_seconds},
        ) from timeout_error
    except json.JSONDecodeError as decode_error:
        raise SchemaBoundaryError(
            "runtime returned invalid JSON",
            details={"url": url},
        ) from decode_error


def validate_chat_completion_response(response_body: Mapping[str, Any]) -> JsonObject:
    object_type = response_body.get("object")
    response_id = response_body.get("id")
    model = response_body.get("model")
    choices = response_body.get("choices")

    if not isinstance(response_id, str) or not response_id:
        raise SchemaBoundaryError("runtime response is missing id")
    if object_type != "chat.completion":
        raise SchemaBoundaryError(
            "runtime response object type is invalid",
            details={"object": object_type},
        )
    if not isinstance(model, str) or not model:
        raise SchemaBoundaryError("runtime response is missing model")
    if not isinstance(choices, list) or not choices:
        raise SchemaBoundaryError("runtime response choices must be a non-empty list")

    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        raise SchemaBoundaryError("runtime response choice must be an object")
    message = first_choice.get("message")
    finish_reason = first_choice.get("finish_reason")
    if not isinstance(message, dict):
        raise SchemaBoundaryError("runtime response choice is missing message")
    if not isinstance(message.get("content"), str):
        raise SchemaBoundaryError("runtime response message content must be a string")
    if finish_reason is not None and not isinstance(finish_reason, str):
        raise SchemaBoundaryError("runtime response finish_reason must be a string or null")

    return {
        "id": response_id,
        "object": object_type,
        "model": model,
        "choices": choices,
    }


def extract_message_content(response_body: Mapping[str, Any]) -> str:
    choices = response_body["choices"]
    first_choice = choices[0]
    message = first_choice["message"]
    content = message["content"]
    if not isinstance(content, str) or not content.strip():
        raise SchemaBoundaryError("runtime response message content must be a non-empty string")
    return content.strip()


def parse_structured_output(content: str) -> dict[str, Any]:
    normalized = content.strip()
    if normalized.startswith("```"):
        normalized = normalized.removeprefix("```json").removeprefix("```").strip()
        if normalized.endswith("```"):
            normalized = normalized[:-3].strip()
    try:
        payload = json.loads(normalized)
    except json.JSONDecodeError as error:
        raise SchemaBoundaryError("runtime response content is not valid JSON") from error
    if not isinstance(payload, dict):
        raise SchemaBoundaryError("runtime response content must decode to an object")
    return payload


def repair_messages(
    existing: list[InferenceMessage],
    invalid_content: str,
    repair_instruction: str,
) -> list[InferenceMessage]:
    return [
        *existing,
        InferenceMessage(role="assistant", content=invalid_content),
        InferenceMessage(role="user", content=repair_instruction),
    ]
