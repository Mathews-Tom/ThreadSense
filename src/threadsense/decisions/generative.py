"""Existing theme-task acquisition and identity-bound, network-free replay."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from threadsense.config import RuntimeConfig
from threadsense.decisions.artifact import decision_bytes
from threadsense.decisions.client import validate_loopback_endpoint
from threadsense.decisions.comparators import observe_generative_availability
from threadsense.decisions.contracts import OTHER_OR_UNCERTAIN, decode_strict_json_object
from threadsense.decisions.evaluation_contracts import Partition
from threadsense.domains.vocabulary import DomainVocabulary
from threadsense.errors import (
    DecisionEvaluationError,
    DecisionInputError,
    InferenceBoundaryError,
    NetworkBoundaryError,
    SchemaBoundaryError,
)
from threadsense.inference.local_runtime import (
    LocalRuntimeClient,
    extract_message_content,
    parse_structured_output,
    validate_chat_completion_response,
)
from threadsense.inference.prompts import build_reclassification_request
from threadsense.models.canonical import Thread


@dataclass(frozen=True)
class GenerativeInput:
    thread: Thread
    comment_ids: tuple[str, ...]
    partition: Partition


@dataclass(frozen=True)
class GenerativeCaptureResult:
    availability: dict[str, Any]
    capture: dict[str, Any] | None
    rows: tuple[dict[str, Any], ...]
    unsupported: tuple[dict[str, str], ...]


def generative_capture_bytes(capture: dict[str, Any]) -> bytes:
    return decision_bytes(capture)


def _fail(reason: str) -> DecisionEvaluationError:
    return DecisionEvaluationError("generative capture is invalid", {"reason": reason})


def _endpoint(runtime: RuntimeConfig) -> str:
    base = validate_loopback_endpoint(runtime.base_url)
    parts = urlsplit(runtime.chat_endpoint)
    origin = f"{parts.scheme}://{parts.netloc}"
    if (
        validate_loopback_endpoint(origin) != base
        or parts.query
        or parts.fragment
        or any(c.isspace() for c in runtime.chat_endpoint)
        or not parts.path.startswith("/")
        or parts.path.startswith("//")
    ):
        raise DecisionInputError("invalid local chat endpoint", {"reason": "endpoint_invalid"})
    return runtime.chat_endpoint


def _plan(
    inputs: tuple[GenerativeInput, ...], vocabulary: DomainVocabulary, runtime: RuntimeConfig
) -> tuple[list[dict[str, Any]], tuple[dict[str, str], ...]]:
    batches: list[dict[str, Any]] = []
    unsupported: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in inputs:
        index = {c.comment_id: c for c in item.thread.comments}
        if len(index) != len(item.thread.comments) or len(set(item.comment_ids)) != len(
            item.comment_ids
        ):
            raise _fail("duplicate_input_id")
        if not set(item.comment_ids) <= index.keys():
            raise _fail("unknown_input_id")
        eligible = set(item.comment_ids)
        ids: list[str] = []
        for comment in item.thread.comments:
            key = (item.thread.thread_id, comment.comment_id)
            if key in seen:
                raise _fail("duplicate_input_id")
            seen.add(key)
            reason = None
            if comment.comment_id not in eligible:
                reason = "typed_ineligible_or_unsupported_language"
            elif len(comment.body) > 300:
                reason = "body_exceeds_300_characters"
            if reason:
                unsupported.append({"thread_id": key[0], "comment_id": key[1], "reason": reason})
            else:
                ids.append(comment.comment_id)
        if ids:
            request = build_reclassification_request(
                item.thread, ids, vocabulary.theme_rules, repair_retries=runtime.repair_retries
            )
            batches.append(
                {
                    "thread_id": item.thread.thread_id,
                    "partition": item.partition.value,
                    "comment_ids": ids,
                    "request": request,
                }
            )
    return batches, tuple(unsupported)


def _layout(batch: dict[str, Any]) -> dict[str, Any]:
    request = batch["request"]
    return {
        "thread_id": batch["thread_id"],
        "partition": batch["partition"],
        "comment_ids": batch["comment_ids"],
        "task": request.task.value,
        "messages": [{"role": m.role, "content": m.content} for m in request.messages],
        "repair_retries": request.repair_retries,
        "repair_instruction": request.repair_instruction,
    }


def _raw_rows(
    batch: dict[str, Any], response: dict[str, Any], vocabulary: DomainVocabulary
) -> tuple[dict[str, Any], ...]:
    parsed = validate_chat_completion_response(response)
    raw = parse_structured_output(extract_message_content(parsed)).get("classifications")
    if not isinstance(raw, list):
        raise _fail("invalid_response_association")
    by_id: dict[str, dict[str, Any]] = {}
    for row in raw:
        if not isinstance(row, dict):
            raise _fail("invalid_response_association")
        cid, theme, confidence = row.get("comment_id"), row.get("theme"), row.get("confidence")
        if not isinstance(cid, str) or cid in by_id or cid not in batch["comment_ids"]:
            raise _fail("invalid_response_association")
        if not isinstance(theme, str) or not theme.strip():
            raise _fail("invalid_theme")
        if (
            isinstance(confidence, bool)
            or not isinstance(confidence, int | float)
            or not math.isfinite(confidence)
            or not 0 <= confidence <= 1
        ):
            raise _fail("invalid_confidence")
        # Free-form names are not coerced into an existing bounded option.
        label = (
            theme
            if theme in vocabulary.theme_rules and theme != vocabulary.default_theme
            else OTHER_OR_UNCERTAIN
        )
        abstained = label == OTHER_OR_UNCERTAIN
        by_id[cid] = {
            "thread_id": batch["thread_id"],
            "comment_id": cid,
            "partition": batch["partition"],
            "status": "abstained" if abstained else "decided",
            "label": label,
            "raw_theme": theme,
            "confidence": confidence,
            "reason": ("default_theme" if theme == vocabulary.default_theme else "new_theme")
            if abstained
            else None,
        }
    if set(by_id) != set(batch["comment_ids"]):
        raise _fail("invalid_response_association")
    return tuple(by_id[cid] for cid in batch["comment_ids"])


def _identity(runtime: RuntimeConfig) -> dict[str, Any]:
    return runtime.model_dump(mode="json")


def capture_generative_theme(
    *,
    inputs: tuple[GenerativeInput, ...],
    vocabulary: DomainVocabulary,
    bindings: dict[str, Any],
    runtime_config: RuntimeConfig,
    transport: httpx.BaseTransport | None = None,
) -> GenerativeCaptureResult:
    batches, unsupported = _plan(inputs, vocabulary, runtime_config)
    try:
        endpoint = _endpoint(runtime_config)
    except (DecisionInputError, ValueError) as error:
        return GenerativeCaptureResult(
            {
                "status": "not_observed",
                "reason": "endpoint_invalid",
                "error_type": type(error).__name__,
            },
            None,
            (),
            unsupported,
        )
    availability = observe_generative_availability(runtime_config, transport=transport)
    if availability["status"] != "available":
        return GenerativeCaptureResult(availability, None, (), unsupported)
    if not batches:
        return GenerativeCaptureResult(
            {**availability, "reason": "no_supported_inputs"}, None, (), unsupported
        )
    records: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    client = LocalRuntimeClient(runtime_config)
    try:
        with httpx.Client(transport=transport, trust_env=False, follow_redirects=False) as http:
            for batch in batches:
                exchanges: list[dict[str, Any]] = []

                def opener(
                    url: str,
                    payload: dict[str, Any],
                    timeout: float,
                    *,
                    recorded_exchanges: list[dict[str, Any]] = exchanges,
                ) -> tuple[int, dict[str, Any]]:
                    if url != endpoint:
                        raise _fail("unexpected_chat_endpoint")
                    response = http.post(url, json=payload, timeout=timeout)
                    if response.status_code != 200:
                        raise _fail("chat_http_status")
                    body = decode_strict_json_object(
                        response.content, DecisionEvaluationError, "chat response"
                    )
                    recorded_exchanges.append(
                        {
                            "url": url,
                            "request": payload,
                            "status_code": response.status_code,
                            "response_text": response.content.decode("utf-8"),
                        }
                    )
                    return response.status_code, body

                answer = client.complete(batch["request"], opener=opener)
                if answer.used_fallback or answer.degraded:
                    raise _fail("fallback_is_not_inference")
                body = decode_strict_json_object(
                    exchanges[-1]["response_text"].encode(),
                    DecisionEvaluationError,
                    "chat response",
                )
                derived = _raw_rows(batch, body, vocabulary)
                rows.extend(derived)
                records.append(
                    {
                        **_layout(batch),
                        "returned_model": answer.model,
                        "exchanges": exchanges,
                        "rows": list(derived),
                    }
                )
    except (
        httpx.HTTPError,
        DecisionEvaluationError,
        InferenceBoundaryError,
        NetworkBoundaryError,
        SchemaBoundaryError,
        UnicodeError,
    ) as error:
        return GenerativeCaptureResult(
            {
                **availability,
                "status": "unavailable",
                "reason": "inference_failed",
                "error_type": type(error).__name__,
                "failure_details": getattr(error, "details", {}),
            },
            None,
            (),
            unsupported,
        )
    capture: dict[str, Any] = {
        "schema_version": 1,
        "bindings": bindings,
        "runtime": _identity(runtime_config),
        "availability": availability,
        "unsupported": list(unsupported),
        "batches": records,
    }
    capture["capture_sha256"] = hashlib.sha256(decision_bytes(capture)).hexdigest()
    return GenerativeCaptureResult(availability, capture, tuple(rows), unsupported)


def replay_generative_theme(
    *,
    inputs: tuple[GenerativeInput, ...],
    vocabulary: DomainVocabulary,
    bindings: dict[str, Any],
    runtime_config: RuntimeConfig,
    content: bytes,
) -> GenerativeCaptureResult:
    capture = decode_strict_json_object(content, DecisionEvaluationError, "generative capture")
    unsigned = {k: v for k, v in capture.items() if k != "capture_sha256"}
    if (
        capture.get("schema_version") != 1
        or capture.get("capture_sha256") != hashlib.sha256(decision_bytes(unsigned)).hexdigest()
        or generative_capture_bytes(capture) != content
    ):
        raise _fail("capture_identity_mismatch")
    if capture.get("bindings") != bindings or capture.get("runtime") != _identity(runtime_config):
        raise _fail("capture_binding_mismatch")
    endpoint = _endpoint(runtime_config)
    batches, unsupported = _plan(inputs, vocabulary, runtime_config)
    if capture.get("unsupported") != list(unsupported):
        raise _fail("capture_input_mismatch")
    records = capture.get("batches")
    availability = capture.get("availability")
    if (
        not isinstance(records, list)
        or len(records) != len(batches)
        or not batches
        or not isinstance(availability, dict)
        or availability.get("status") != "available"
    ):
        raise _fail("capture_schema_invalid")
    client = LocalRuntimeClient(runtime_config)
    rows: list[dict[str, Any]] = []
    try:
        for batch, record in zip(batches, records, strict=True):
            if not isinstance(record, dict) or any(
                record.get(k) != v for k, v in _layout(batch).items()
            ):
                raise _fail("capture_request_mismatch")
            raw_exchanges = record.get("exchanges")
            if not isinstance(raw_exchanges, list) or not raw_exchanges:
                raise _fail("capture_exchanges_invalid")
            exchanges: list[Any] = raw_exchanges
            position = 0

            def opener(
                url: str,
                payload: dict[str, Any],
                timeout: float,
                *,
                recorded_exchanges: list[Any] = exchanges,
            ) -> tuple[int, dict[str, Any]]:
                nonlocal position
                if position >= len(recorded_exchanges):
                    raise _fail("capture_exchanges_missing")
                exchange = recorded_exchanges[position]
                position += 1
                if (
                    not isinstance(exchange, dict)
                    or url != endpoint
                    or exchange.get("url") != url
                    or exchange.get("request") != payload
                    or exchange.get("status_code") != 200
                    or not isinstance(exchange.get("response_text"), str)
                ):
                    raise _fail("capture_exchange_mismatch")
                return 200, decode_strict_json_object(
                    exchange["response_text"].encode(), DecisionEvaluationError, "chat response"
                )

            answer = client.complete(batch["request"], opener=opener)
            if (
                position != len(exchanges)
                or answer.model != record.get("returned_model")
                or answer.used_fallback
                or answer.degraded
            ):
                raise _fail("capture_exchange_mismatch")
            body = decode_strict_json_object(
                exchanges[-1]["response_text"].encode(), DecisionEvaluationError, "chat response"
            )
            derived = _raw_rows(batch, body, vocabulary)
            if record.get("rows") != list(derived):
                raise _fail("capture_rows_mismatch")
            rows.extend(derived)
    except (
        InferenceBoundaryError,
        NetworkBoundaryError,
        SchemaBoundaryError,
        UnicodeError,
    ) as error:
        raise _fail("capture_response_invalid") from error
    return GenerativeCaptureResult(availability, capture, tuple(rows), unsupported)
