"""Consumer-boundary tests for typed Laya decisions.

Negative cases are derived from the real recorded exchange in tests/fixtures/decisions/.
Transport is httpx MockTransport: engineering coverage of the protocol, not model evidence.
"""

from __future__ import annotations

import copy
import dataclasses
import json
import math
import time
from collections.abc import Callable
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
    build_manifest,
    json_response,
    recorded_batch,
    routed_transport,
    write_checkpoint,
)

from threadsense.contracts.analysis_contract import ObjectiveType
from threadsense.decisions import contracts
from threadsense.decisions.client import TypedDecisionClient, validate_loopback_endpoint
from threadsense.decisions.context import build_comment_states, plan_batches
from threadsense.decisions.contracts import (
    PINNED_CONTEXT_POLICY,
    PINNED_INFERENCE_SETTINGS,
    ContextPolicy,
    ContextWindow,
    InferenceSettings,
    QuestionSet,
    QuestionType,
    encode_request_body,
)
from threadsense.decisions.questions import ATTRIBUTION, build_question_set
from threadsense.decisions.runtime import (
    VERIFICATION_BASIS,
    CheckpointVerifier,
    parse_runtime_manifest,
)
from threadsense.decisions.validation import (
    split_batch_results,
    validate_health,
    validate_result,
)
from threadsense.domains.loader import load_domain_vocabulary
from threadsense.domains.vocabulary import DomainVocabulary
from threadsense.errors import (
    DecisionIdentityError,
    DecisionInputError,
    DecisionProtocolError,
    DecisionRuntimeUnavailableError,
)
from threadsense.models.canonical import Comment, Thread, load_canonical_thread
from threadsense.monitoring.observations import comment_text_sha256

PLACEHOLDER_REPO = Path("/checkpoint-placeholder")
RECORDED_RESULT_COUNT = 7
PathKey = str | int
_DELETE: Any = object()


# ---------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------


@pytest.fixture
def vocabulary() -> DomainVocabulary:
    return load_domain_vocabulary("developer_tools")


@pytest.fixture
def question_set(vocabulary: DomainVocabulary) -> QuestionSet:
    return build_question_set(ObjectiveType.FRICTION_ANALYSIS, vocabulary)


def _apply(root: Any, path: tuple[PathKey, ...], value: Any) -> None:
    target = root
    for key in path[:-1]:
        target = target[key]
    if value is _DELETE:
        del target[path[-1]]
    else:
        target[path[-1]] = value


def _recorded_payload() -> dict[str, Any]:
    _request, payload = recorded_batch(PLACEHOLDER_REPO)
    return payload


def _recorded_request_body() -> dict[str, Any]:
    request_text, _payload = recorded_batch(PLACEHOLDER_REPO)
    body: dict[str, Any] = json.loads(request_text)
    return body


def _recorded_result(index: int) -> dict[str, Any]:
    result: dict[str, Any] = _recorded_payload()["results"][index]
    return result


def _ticks(*values: float) -> Callable[[], float]:
    iterator = iter(values)
    return lambda: next(iterator)


def _serve(
    respond: Callable[[httpx.Request], httpx.Response],
) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    calls: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return respond(request)

    return httpx.MockTransport(handle), calls


def _ok(_request: httpx.Request) -> httpx.Response:
    return json_response({"ok": True})


def _client(
    transport: httpx.BaseTransport,
    *,
    timeout_seconds: float = 5.0,
    clock: Callable[[], float] | None = None,
) -> TypedDecisionClient:
    return TypedDecisionClient(
        ENDPOINT,
        wire=WIRE,
        timeout_seconds=timeout_seconds,
        transport=transport,
        clock=clock or time.perf_counter,
    )


def _raw(
    content: bytes, *, content_type: str = "application/json", status: int = 200
) -> Callable[[httpx.Request], httpx.Response]:
    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=content, headers={"content-type": content_type})

    return respond


def _minimal_body(comment: str) -> dict[str, Any]:
    return {
        "model": "english",
        "lang": "en",
        "max_len": 512,
        "head_max_len": 256,
        "questions": {"q": {"type": "noul", "instructions": "i"}},
        "states": [{"comment": comment}],
    }


def _thread(
    comments: list[Comment], *, title: str | None = None, body: str | None = None
) -> Thread:
    base = load_canonical_thread(CANONICAL_FIXTURE)
    return dataclasses.replace(
        base,
        comments=comments,
        title=base.title if title is None else title,
        body=body,
    )


def _comment(comment_id: str, body: str, parent: str | None = None) -> Comment:
    template = load_canonical_thread(CANONICAL_FIXTURE).comments[0]
    return dataclasses.replace(template, comment_id=comment_id, body=body, parent_comment_id=parent)


# ---------------------------------------------------------------------------------------
# 1. validate_loopback_endpoint
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("endpoint", "normalized"),
    [
        ("http://127.0.0.1:62635", "http://127.0.0.1:62635"),
        ("http://127.0.0.1:62635/", "http://127.0.0.1:62635"),
        ("http://127.0.0.2:9", "http://127.0.0.2:9"),
        ("http://[::1]:8080", "http://[::1]:8080"),
        ("http://[::1]:8080/", "http://[::1]:8080"),
        ("http://[0:0:0:0:0:0:0:1]:8080", "http://[::1]:8080"),
    ],
)
def test_validate_loopback_endpoint_literal_loopback_returns_normalized_base(
    endpoint: str, normalized: str
) -> None:
    assert validate_loopback_endpoint(endpoint) == normalized


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://localhost:62635",
        "http://example.com:80",
        "http://10.0.0.1:80",
        "http://192.168.1.5:80",
        "http://0.0.0.0:80",
    ],
)
def test_validate_loopback_endpoint_non_loopback_host_raises_not_literal_loopback(
    endpoint: str,
) -> None:
    with pytest.raises(DecisionInputError) as caught:
        validate_loopback_endpoint(endpoint)

    assert caught.value.details["reason"] == "endpoint_not_literal_loopback"
    assert caught.value.details["endpoint"] == endpoint


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://127.0.0.1:62635",
        "http://127.0.0.1",
        "http://u:p@127.0.0.1:1",
        "http://127.0.0.1:1/v1",
        "http://127.0.0.1:1//",
        "http://127.0.0.1:1?a=1",
        "http://127.0.0.1:1?",
        "http://127.0.0.1:1#frag",
        "http://127.0.0.1:1#",
        " http://127.0.0.1:1",
        "http://127.0.0.1:1 ",
        "http://127.0.0.1: 1",
        "http://127.0.0.1:99999",
        "http://127.0.0.1:abc",
        "http://:8080",
        "127.0.0.1:8080",
        "",
    ],
)
def test_validate_loopback_endpoint_malformed_url_raises_endpoint_invalid(endpoint: str) -> None:
    with pytest.raises(DecisionInputError) as caught:
        validate_loopback_endpoint(endpoint)

    assert caught.value.details["reason"] == "endpoint_invalid"
    assert caught.value.details["endpoint"] == endpoint


# ---------------------------------------------------------------------------------------
# 2. TypedDecisionClient
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("timeout", [0, -1, math.nan, math.inf, -math.inf, True])
def test_typed_decision_client_invalid_timeout_raises_timeout_invalid(timeout: float) -> None:
    transport, calls = _serve(_ok)

    with pytest.raises(DecisionInputError) as caught:
        _client(transport, timeout_seconds=timeout)

    assert caught.value.details["reason"] == "timeout_invalid"
    assert calls == []


def test_typed_decision_client_non_loopback_endpoint_raises_before_construction() -> None:
    transport, calls = _serve(_ok)

    with pytest.raises(DecisionInputError) as caught:
        TypedDecisionClient(
            "http://localhost:62635", wire=WIRE, timeout_seconds=5.0, transport=transport
        )

    assert caught.value.details["reason"] == "endpoint_not_literal_loopback"
    assert calls == []


@pytest.mark.parametrize(
    ("control", "value"),
    [("sort_by_length", True), ("batch_size", 8), ("min_confidence", 0.5)],
)
def test_post_batch_forbidden_control_raises_before_any_request(control: str, value: Any) -> None:
    transport, calls = _serve(_ok)
    body = _recorded_request_body()
    body[control] = value

    with _client(transport) as client, pytest.raises(DecisionInputError) as caught:
        client.post_batch(body)

    assert caught.value.details["reason"] == "forbidden_request_control"
    assert caught.value.details["controls"] == [control]
    assert calls == []


def test_post_batch_model_mismatch_raises_before_any_request() -> None:
    transport, calls = _serve(_ok)
    body = _recorded_request_body()
    body["model"] = "multilingual"

    with _client(transport) as client, pytest.raises(DecisionInputError) as caught:
        client.post_batch(body)

    assert caught.value.details["reason"] == "request_model_mismatch"
    assert caught.value.details["expected"] == "english"
    assert caught.value.details["observed"] == "multilingual"
    assert calls == []


@pytest.mark.parametrize("count", [0, contracts.MAX_BATCH_STATES + 1])
def test_post_batch_state_count_out_of_bounds_raises_before_any_request(count: int) -> None:
    transport, calls = _serve(_ok)
    body = _recorded_request_body()
    body["states"] = [{"comment": "x"}] * count

    with _client(transport) as client, pytest.raises(DecisionInputError) as caught:
        client.post_batch(body)

    assert caught.value.details["reason"] == "request_states_out_of_bounds"
    assert caught.value.details["limit"] == contracts.MAX_BATCH_STATES
    assert calls == []


@pytest.mark.parametrize("count", [0, contracts.MAX_QUESTIONS + 1])
def test_post_batch_question_count_out_of_bounds_raises_before_any_request(count: int) -> None:
    transport, calls = _serve(_ok)
    body = _recorded_request_body()
    body["questions"] = {f"q{i}": {"type": "noul", "instructions": "i"} for i in range(count)}

    with _client(transport) as client, pytest.raises(DecisionInputError) as caught:
        client.post_batch(body)

    assert caught.value.details["reason"] == "request_questions_out_of_bounds"
    assert caught.value.details["limit"] == contracts.MAX_QUESTIONS
    assert calls == []


def test_post_batch_maximum_states_and_questions_are_sent() -> None:
    transport, calls = _serve(_ok)
    body = _recorded_request_body()
    body["states"] = [{"comment": "x"}] * contracts.MAX_BATCH_STATES
    body["questions"] = {
        f"q{i}": {"type": "noul", "instructions": "i"} for i in range(contracts.MAX_QUESTIONS)
    }

    with _client(transport) as client:
        client.post_batch(body)

    assert len(calls) == 1


def test_post_batch_body_over_limit_raises_before_any_request() -> None:
    transport, calls = _serve(_ok)
    overhead = len(encode_request_body(_minimal_body("")))
    body = _minimal_body("x" * (contracts.MAX_BODY_BYTES - overhead + 1))

    with _client(transport) as client, pytest.raises(DecisionInputError) as caught:
        client.post_batch(body)

    assert caught.value.details["reason"] == "request_body_too_large"
    assert caught.value.details["bytes"] == contracts.MAX_BODY_BYTES + 1
    assert caught.value.details["limit"] == contracts.MAX_BODY_BYTES
    assert calls == []


def test_post_batch_body_exactly_at_limit_is_sent() -> None:
    transport, calls = _serve(_ok)
    overhead = len(encode_request_body(_minimal_body("")))
    body = _minimal_body("x" * (contracts.MAX_BODY_BYTES - overhead))

    with _client(transport) as client:
        client.post_batch(body)

    assert len(calls) == 1
    assert len(calls[0].content) == contracts.MAX_BODY_BYTES


@pytest.mark.parametrize(
    ("make_error", "reason"),
    [
        (lambda request: httpx.ConnectError("refused", request=request), "connect_failed"),
        (lambda request: httpx.ReadTimeout("slow", request=request), "timeout"),
        (lambda request: httpx.ConnectTimeout("slow", request=request), "timeout"),
        (lambda request: httpx.RemoteProtocolError("garbled", request=request), "protocol_failure"),
        (lambda request: httpx.ReadError("reset", request=request), "transport_failure"),
    ],
)
def test_post_batch_transport_failure_raises_runtime_unavailable_with_reason(
    make_error: Callable[[httpx.Request], Exception], reason: str
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        raise make_error(request)

    transport, calls = _serve(respond)

    with _client(transport, clock=_ticks(10.0, 12.5)) as client:
        with pytest.raises(DecisionRuntimeUnavailableError) as caught:
            client.post_batch(_recorded_request_body())

    assert caught.value.details["reason"] == reason
    assert caught.value.details["wall_seconds"] == 2.5
    assert caught.value.details["method"] == "POST"
    assert caught.value.details["path"] == WIRE.batch_path
    assert len(calls) == 1


def test_health_connect_failure_raises_runtime_unavailable_for_get() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    transport, _calls = _serve(respond)

    with _client(transport, clock=_ticks(1.0, 1.25)) as client:
        with pytest.raises(DecisionRuntimeUnavailableError) as caught:
            client.health()

    assert caught.value.details["reason"] == "connect_failed"
    assert caught.value.details["method"] == "GET"
    assert caught.value.details["path"] == WIRE.health_path
    assert caught.value.details["wall_seconds"] == 0.25


@pytest.mark.parametrize("status", [503, 500, 413, 307])
def test_post_batch_non_200_status_raises_http_status_without_following(status: int) -> None:
    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status, text="refused by service", headers={"location": f"{ENDPOINT}/elsewhere"}
        )

    transport, calls = _serve(respond)

    with _client(transport, clock=_ticks(0.0, 4.0)) as client:
        with pytest.raises(DecisionRuntimeUnavailableError) as caught:
            client.post_batch(_recorded_request_body())

    assert caught.value.details["reason"] == "http_status"
    assert caught.value.details["status_code"] == status
    assert caught.value.details["response_excerpt"] == "refused by service"
    assert caught.value.details["wall_seconds"] == 4.0
    assert len(calls) == 1


def test_post_batch_non_json_content_type_raises_unexpected_content_type() -> None:
    transport, _calls = _serve(_raw(b"{}", content_type="text/plain"))

    with _client(transport) as client, pytest.raises(DecisionProtocolError) as caught:
        client.post_batch(_recorded_request_body())

    assert caught.value.details["reason"] == "unexpected_content_type"
    assert caught.value.details["content_type"] == "text/plain"


@pytest.mark.parametrize(
    "content",
    [
        b'{"a": NaN}',
        b'{"a": Infinity}',
        b'{"a": -Infinity}',
        b'{"a": 1, "a": 2}',
        b'{"results": [{"x": 1, "x": 1}]}',
        b"\xff\xfe{}",
        b'{"a":',
        b"",
    ],
    ids=[
        "nan",
        "infinity",
        "negative_infinity",
        "duplicate_key",
        "nested_duplicate_key",
        "invalid_utf8",
        "truncated",
        "empty",
    ],
)
def test_post_batch_non_strict_json_raises_invalid_json_with_decode_error(content: bytes) -> None:
    transport, _calls = _serve(_raw(content))

    with _client(transport, clock=_ticks(0.0, 1.5)) as client:
        with pytest.raises(DecisionProtocolError) as caught:
            client.post_batch(_recorded_request_body())

    assert caught.value.details["reason"] == "invalid_json"
    assert isinstance(caught.value.details["decode_error"], str)
    assert caught.value.details["wall_seconds"] == 1.5


@pytest.mark.parametrize("content", [b"[1, 2]", b'"text"', b"7", b"null"])
def test_post_batch_non_object_json_root_raises_protocol_error_invalid_json(
    content: bytes,
) -> None:
    transport, _calls = _serve(_raw(content))

    with _client(transport) as client, pytest.raises(DecisionProtocolError) as caught:
        client.post_batch(_recorded_request_body())

    assert caught.value.details["reason"] == "invalid_json"


def test_post_batch_success_sends_canonical_bytes_and_records_exact_texts() -> None:
    body = _recorded_request_body()
    expected_bytes = encode_request_body(body)
    response_text = json.dumps(_recorded_payload(), ensure_ascii=False)
    transport, calls = _serve(_raw(response_text.encode("utf-8")))

    with _client(transport, clock=_ticks(10.0, 12.5)) as client:
        exchange = client.post_batch(body)

    assert len(calls) == 1
    assert calls[0].method == "POST"
    assert calls[0].url.path == WIRE.batch_path
    assert calls[0].content == expected_bytes
    assert calls[0].headers["content-type"] == "application/json"
    assert exchange.request_body == expected_bytes.decode("utf-8")
    assert exchange.response_body == response_text
    assert exchange.status_code == 200
    assert exchange.wall_seconds == 2.5
    assert exchange.payload["total_usage"] == {"input_tokens": 6034, "output_tokens": 0}


def test_post_batch_recorded_body_hashes_to_recorded_request_sha256() -> None:
    fixture = json.loads(RECORDED_BATCH_FIXTURE.read_text(encoding="utf-8"))
    transport, _calls = _serve(_ok)

    with _client(transport) as client:
        exchange = client.post_batch(_recorded_request_body())

    assert exchange.request_sha256() == fixture["provenance"]["request_sha256"]


def test_health_sends_get_without_body_and_returns_payload() -> None:
    transport, calls = _serve(lambda _request: json_response(HEALTH_PAYLOAD))

    with _client(transport) as client:
        exchange = client.health()

    assert calls[0].method == "GET"
    assert calls[0].url.path == WIRE.health_path
    assert calls[0].content == b""
    assert calls[0].headers["accept"] == "application/json"
    assert exchange.request_body is None
    assert exchange.request_sha256() is None
    assert validate_health(exchange.payload, WIRE).device == "cpu"


def test_client_ignores_proxy_environment_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.setenv(name, "http://bogus.invalid:9")
    transport, calls = _serve(lambda _request: json_response(HEALTH_PAYLOAD))

    with _client(transport) as client:
        exchange = client.health()

    assert exchange.status_code == 200
    assert [str(call.url) for call in calls] == [f"{ENDPOINT}{WIRE.health_path}"]


# ---------------------------------------------------------------------------------------
# 3. validate_health
# ---------------------------------------------------------------------------------------


def test_validate_health_ok_payload_keeps_null_revision_and_device() -> None:
    payload = copy.deepcopy(HEALTH_PAYLOAD)

    observed = validate_health(payload, WIRE)

    assert observed.status == "ok"
    assert observed.loaded == ("english",)
    assert observed.revision is None
    assert observed.device == "cpu"


def test_validate_health_string_revision_is_reported_as_is() -> None:
    payload = copy.deepcopy(HEALTH_PAYLOAD)
    payload["revisions"]["english"] = "abc123"

    assert validate_health(payload, WIRE).revision == "abc123"


def test_validate_health_null_device_is_accepted_as_null() -> None:
    payload = copy.deepcopy(HEALTH_PAYLOAD)
    payload["device"] = None

    assert validate_health(payload, WIRE).device is None


@pytest.mark.parametrize("status", ["loading", "error", None, True])
def test_validate_health_status_not_ok_raises_health_not_ready(status: Any) -> None:
    payload = copy.deepcopy(HEALTH_PAYLOAD)
    payload["status"] = status

    with pytest.raises(DecisionProtocolError) as caught:
        validate_health(payload, WIRE)

    assert caught.value.details["reason"] == "health_not_ready"
    assert caught.value.details["observed"] == status


def test_validate_health_model_missing_from_loaded_raises_model_not_loaded() -> None:
    payload = copy.deepcopy(HEALTH_PAYLOAD)
    payload["loaded"] = ["multilingual"]

    with pytest.raises(DecisionProtocolError) as caught:
        validate_health(payload, WIRE)

    assert caught.value.details["reason"] == "health_model_not_loaded"
    assert caught.value.details["expected"] == "english"
    assert caught.value.details["observed"] == ["multilingual"]


@pytest.mark.parametrize(
    ("path", "value", "field"),
    [
        (("revisions",), {}, "revisions"),
        (("revisions",), {"multilingual": None}, "revisions"),
        (("revisions",), ["english"], "revisions"),
        (("revisions", "english"), 7, "revisions"),
        (("revisions", "english"), {"sha": "x"}, "revisions"),
        (("device",), _DELETE, "device"),
        (("device",), 0, "device"),
        (("loaded",), "english", "loaded"),
        (("loaded",), ["english", 3], "loaded"),
        (("loaded",), _DELETE, "loaded"),
    ],
)
def test_validate_health_malformed_field_raises_health_invalid(
    path: tuple[PathKey, ...], value: Any, field: str
) -> None:
    payload = copy.deepcopy(HEALTH_PAYLOAD)
    _apply(payload, path, value)

    with pytest.raises(DecisionProtocolError) as caught:
        validate_health(payload, WIRE)

    assert caught.value.details["reason"] == "health_invalid"
    assert caught.value.details["field"] == field


# ---------------------------------------------------------------------------------------
# 4. split_batch_results
# ---------------------------------------------------------------------------------------


def test_split_batch_results_recorded_payload_returns_seven_results_in_order() -> None:
    payload = _recorded_payload()

    results = split_batch_results(payload, RECORDED_RESULT_COUNT)

    assert [result["usage"]["input_tokens"] for result in results] == [
        880,
        880,
        868,
        868,
        856,
        850,
        832,
    ]


@pytest.mark.parametrize("count", [6, 8])
def test_split_batch_results_wrong_count_raises_result_count_mismatch(count: int) -> None:
    payload = _recorded_payload()
    results = payload["results"]
    payload["results"] = results[:count] if count < len(results) else [*results, results[0]]

    with pytest.raises(DecisionProtocolError) as caught:
        split_batch_results(payload, RECORDED_RESULT_COUNT)

    assert caught.value.details["reason"] == "result_count_mismatch"
    assert caught.value.details["expected"] == RECORDED_RESULT_COUNT
    assert caught.value.details["observed"] == count


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("extra",), 1),
        (("results",), _DELETE),
        (("total_usage",), _DELETE),
        (("results",), {}),
        (("results",), "seven"),
        (("total_usage",), [6034, 0]),
        (("total_usage", "extra"), 1),
        (("total_usage", "output_tokens"), _DELETE),
        (("total_usage", "input_tokens"), -1),
        (("total_usage", "input_tokens"), True),
        (("total_usage", "output_tokens"), 0.5),
        (("total_usage", "output_tokens"), "0"),
    ],
)
def test_split_batch_results_malformed_batch_shape_raises_batch_shape_invalid(
    path: tuple[PathKey, ...], value: Any
) -> None:
    payload = _recorded_payload()
    _apply(payload, path, value)

    with pytest.raises(DecisionProtocolError) as caught:
        split_batch_results(payload, RECORDED_RESULT_COUNT)

    assert caught.value.details["reason"] == "batch_shape_invalid"


@pytest.mark.parametrize(
    ("key", "observed"),
    [
        ("input_tokens", [6035, 0]),
        ("output_tokens", [6034, 1]),
    ],
)
def test_split_batch_results_total_usage_differs_from_sums_raises_total_usage_mismatch(
    key: str, observed: list[int]
) -> None:
    payload = _recorded_payload()
    payload["total_usage"][key] += 1

    with pytest.raises(DecisionProtocolError) as caught:
        split_batch_results(payload, RECORDED_RESULT_COUNT)

    assert caught.value.details["reason"] == "total_usage_mismatch"
    assert caught.value.details["expected"] == [6034, 0]
    assert caught.value.details["observed"] == observed


# ---------------------------------------------------------------------------------------
# 5. validate_result on real recorded results
# ---------------------------------------------------------------------------------------


def _validate(result: dict[str, Any], question_set: QuestionSet) -> contracts.ValidatedResult:
    return validate_result(result, question_set, PINNED_INFERENCE_SETTINGS)


@pytest.mark.parametrize("index", range(RECORDED_RESULT_COUNT))
def test_validate_result_recorded_result_returns_answers_in_question_set_order(
    index: int, question_set: QuestionSet
) -> None:
    validated = _validate(_recorded_result(index), question_set)

    assert tuple(answer.question_id for answer in validated.answers) == question_set.question_ids()
    assert [answer.question_type for answer in validated.answers] == [
        question.question_type for question in question_set.questions
    ]


@pytest.mark.parametrize("index", range(RECORDED_RESULT_COUNT))
def test_validate_result_recorded_result_keeps_server_choice_score_and_noul(
    index: int, question_set: QuestionSet
) -> None:
    raw = _recorded_result(index)

    validated = _validate(raw, question_set)

    for answer in validated.answers:
        server = raw["answers"][answer.question_id]
        assert answer.choice == server.get("choice")
        assert answer.score == server.get("score")
        assert answer.noul == server.get("noul")


def test_validate_result_recorded_score_above_one_is_accepted_as_expected_level(
    question_set: QuestionSet,
) -> None:
    raw = _recorded_result(4)
    assert raw["answers"]["objective_relevance"]["score"] > 1

    validated = _validate(raw, question_set)

    relevance = validated.answers[0]
    assert relevance.question_id == "objective_relevance"
    assert relevance.score == raw["answers"]["objective_relevance"]["score"]
    assert relevance.score is not None
    assert relevance.score > 1


@pytest.mark.parametrize("index", range(RECORDED_RESULT_COUNT))
def test_validate_result_recorded_result_reports_named_confidence_quantities(
    index: int, question_set: QuestionSet
) -> None:
    raw = _recorded_result(index)

    validated = _validate(raw, question_set)

    for answer in validated.answers:
        server = raw["answers"][answer.question_id]
        by_name = {item.name: item.value for item in answer.confidence_quantities}
        assert set(by_name) == {"answer_confidence", "confidence"}
        assert by_name["answer_confidence"] == server["answer_confidence"]
        assert by_name["confidence"] == server["confidence"]


@pytest.mark.parametrize("index", range(RECORDED_RESULT_COUNT))
def test_validate_result_recorded_result_has_no_usage_refusal_reasons(
    index: int, question_set: QuestionSet
) -> None:
    validated = _validate(_recorded_result(index), question_set)

    assert validated.usage.refusal_reasons() == ()
    assert validated.usage.options_reported is False


def test_validate_result_recorded_routing_is_explicit_without_detection(
    question_set: QuestionSet,
) -> None:
    validated = _validate(_recorded_result(0), question_set)

    assert validated.routing.response_model == "laya-rl-agent"
    assert validated.routing.model == "english"
    assert validated.routing.repo == str(PLACEHOLDER_REPO)
    assert validated.routing.to_dict()["detection"] is None
    assert validated.routing.to_dict()["workflow"] is None


def test_validate_result_noul_answer_has_no_probabilities_and_two_quantities(
    question_set: QuestionSet,
) -> None:
    raw = _recorded_result(0)

    validated = _validate(raw, question_set)

    promotional = validated.answers[4]
    assert promotional.question_type is QuestionType.NOUL
    assert promotional.probabilities == ()
    assert [item.name for item in promotional.confidence_quantities] == [
        "answer_confidence",
        "confidence",
    ]


@pytest.mark.parametrize(
    ("path", "value", "reason", "details"),
    [
        pytest.param(
            ("routing", "model"),
            "multilingual",
            "routing_model_mismatch",
            {"expected": "english", "observed": "multilingual"},
            id="routing_model",
        ),
        pytest.param(
            ("routing", "reason"), "auto", "routing_not_explicit", {"observed": "auto"}, id="auto"
        ),
        pytest.param(
            ("routing", "detection"),
            {"lang": "en"},
            "routing_language_detected",
            {"field": "detection"},
            id="detection",
        ),
        pytest.param(
            ("routing", "workflow"),
            "wf",
            "routing_workflow_present",
            {"field": "workflow"},
            id="workflow",
        ),
        pytest.param(
            ("routing", "repo"),
            "relative/checkpoint",
            "routing_repo_invalid",
            {"observed": "relative/checkpoint"},
            id="relative_repo",
        ),
        pytest.param(
            ("routing", "repo"), 7, "routing_repo_invalid", {"observed": 7}, id="non_string_repo"
        ),
        pytest.param(
            ("routing", "extra"), 1, "result_shape_invalid", {"field": "routing"}, id="routing_key"
        ),
        pytest.param(
            ("routing",), _DELETE, "result_shape_invalid", {"missing": ["routing"]}, id="no_routing"
        ),
        pytest.param(("model",), "", "result_shape_invalid", {"field": "model"}, id="empty_model"),
        pytest.param(
            ("extra",), 1, "result_shape_invalid", {"unexpected": ["extra"]}, id="result_key"
        ),
        pytest.param(
            ("usage", "surprise"),
            1,
            "usage_unknown_field",
            {"unexpected": ["surprise"]},
            id="usage_unknown_key",
        ),
        pytest.param(
            ("usage", "input_tokens"),
            -1,
            "result_shape_invalid",
            {"field": "input_tokens"},
            id="usage_negative",
        ),
        pytest.param(
            ("usage", "state_tokens"),
            True,
            "result_shape_invalid",
            {"field": "state_tokens"},
            id="usage_bool_count",
        ),
        pytest.param(
            ("usage", "truncated"),
            "no",
            "result_shape_invalid",
            {"field": "truncated"},
            id="usage_truncated_not_bool",
        ),
        pytest.param(
            ("usage", "truncated_questions"),
            ["ghost"],
            "result_shape_invalid",
            {"field": "truncated_questions"},
            id="usage_unknown_question",
        ),
        pytest.param(
            ("usage", "output_tokens"),
            _DELETE,
            "result_shape_invalid",
            {"missing": ["output_tokens"]},
            id="usage_missing_key",
        ),
        pytest.param(
            ("answers",), [], "result_shape_invalid", {"field": "answers"}, id="answers_list"
        ),
    ],
)
def test_validate_result_malformed_envelope_raises_stable_reason(
    path: tuple[PathKey, ...],
    value: Any,
    reason: str,
    details: dict[str, Any],
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    _apply(result, path, value)

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == reason
    for key, expected in details.items():
        assert caught.value.details[key] == expected


def test_validate_result_non_object_result_raises_result_shape_invalid(
    question_set: QuestionSet,
) -> None:
    with pytest.raises(DecisionProtocolError) as caught:
        validate_result([], question_set, PINNED_INFERENCE_SETTINGS)

    assert caught.value.details["reason"] == "result_shape_invalid"
    assert caught.value.details["observed"] == "list"


@pytest.mark.parametrize(
    ("updates", "reasons"),
    [
        pytest.param({"state_tokens_dropped": 3}, ("server_dropped_state_tokens",), id="dropped"),
        pytest.param({"truncated": True}, ("server_reported_truncation",), id="truncated"),
        pytest.param(
            {"truncated_questions": ["theme"]}, ("server_truncated_questions",), id="questions"
        ),
        pytest.param(
            {"options": {"theme": 8}}, ("server_reported_option_usage",), id="options_reported"
        ),
        pytest.param(
            {"state_tokens_dropped": 1, "truncated": True, "truncated_questions": ["theme"]},
            (
                "server_dropped_state_tokens",
                "server_reported_truncation",
                "server_truncated_questions",
            ),
            id="combined",
        ),
    ],
)
def test_validate_result_usage_coverage_signals_map_to_refusal_reasons(
    updates: dict[str, Any], reasons: tuple[str, ...], question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    result["usage"].update(updates)

    validated = _validate(result, question_set)

    assert validated.usage.refusal_reasons() == reasons
    assert validated.usage.options_reported is ("options" in updates)


def test_validate_result_usage_options_key_marks_options_reported(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    result["usage"]["options"] = {"theme": 8}

    validated = _validate(result, question_set)

    assert validated.usage.options_reported is True
    assert "server_reported_option_usage" in validated.usage.refusal_reasons()
    assert validated.usage.to_dict()["options_reported"] is True


def test_validate_result_missing_answer_id_raises_answer_ids_mismatch(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    del result["answers"]["theme"]

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "answer_ids_mismatch"
    assert caught.value.details["missing"] == ["theme"]
    assert caught.value.details["unexpected"] == []


def test_validate_result_extra_answer_id_raises_answer_ids_mismatch(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    result["answers"]["ghost"] = copy.deepcopy(result["answers"]["theme"])

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "answer_ids_mismatch"
    assert caught.value.details["missing"] == []
    assert caught.value.details["unexpected"] == ["ghost"]


@pytest.mark.parametrize(
    ("question_id", "swapped_type"),
    [("theme", "score"), ("objective_relevance", "choice"), ("promotional_framing", "choice")],
)
def test_validate_result_answer_type_swapped_raises_answer_type_mismatch(
    question_id: str, swapped_type: str, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    expected_type = result["answers"][question_id]["type"]
    result["answers"][question_id]["type"] = swapped_type

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "answer_type_mismatch"
    assert caught.value.details["question_id"] == question_id
    assert caught.value.details["expected"] == expected_type
    assert caught.value.details["observed"] == swapped_type


def test_validate_result_choice_options_reordered_raises_option_order_mismatch(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    probabilities = result["answers"]["theme"]["probabilities"]
    result["answers"]["theme"]["probabilities"] = dict(reversed(list(probabilities.items())))

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "choice_option_order_mismatch"
    assert caught.value.details["question_id"] == "theme"
    assert caught.value.details["expected"] == list(probabilities)


def test_validate_result_choice_option_renamed_raises_option_order_mismatch(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    probabilities = result["answers"]["evidence_type"]["probabilities"]
    probabilities["invented_option"] = probabilities.pop("question")

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "choice_option_order_mismatch"


@pytest.mark.parametrize(
    "value", [10**400, -(10**400)], ids=["oversized_positive", "oversized_negative"]
)
@pytest.mark.parametrize(
    ("question_id", "path", "reason"),
    [
        ("objective_relevance", ("score",), "score_out_of_range"),
        ("objective_relevance", ("probabilities", "0"), "probability_invalid"),
        ("evidence_type", ("probabilities", "question"), "probability_invalid"),
        ("objective_relevance", ("confidence",), "probability_invalid"),
        ("objective_relevance", ("answer_confidence",), "probability_invalid"),
        ("evidence_type", ("confidence",), "probability_invalid"),
        ("evidence_type", ("answer_confidence",), "probability_invalid"),
        ("promotional_framing", ("noul",), "probability_invalid"),
        ("promotional_framing", ("confidence",), "probability_invalid"),
        ("promotional_framing", ("answer_confidence",), "probability_invalid"),
    ],
)
def test_validate_result_oversized_wire_integer_raises_protocol_error(
    value: int, question_id: str, path: tuple[str, ...], reason: str, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    _apply(result, ("answers", question_id, *path), value)
    decoded = json.loads(json.dumps(result))

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(decoded, question_set)

    assert caught.value.details["reason"] == reason
    assert caught.value.details["question_id"] == question_id
    assert caught.value.details["observed"] == value
    if reason == "probability_invalid":
        assert caught.value.details["field"] == ".".join(path)


@pytest.mark.parametrize("score", [0, 1, 2])
@pytest.mark.parametrize("noul", [0, 1])
def test_validate_result_integer_boundaries_and_expected_ordinal_are_accepted(
    score: int, noul: int, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    relevance = result["answers"]["objective_relevance"]
    relevance["probabilities"] = {key: int(key == str(score)) for key in relevance["legend"]}
    relevance.update({"score": score, "confidence": 1, "answer_confidence": 1})
    evidence = result["answers"]["evidence_type"]
    evidence["probabilities"] = {
        label: int(label == evidence["choice"]) for label in evidence["probabilities"]
    }
    evidence.update({"confidence": 0, "answer_confidence": 1})
    result["answers"]["promotional_framing"].update(
        {"noul": noul, "confidence": 1, "answer_confidence": 1}
    )

    validated = _validate(result, question_set)

    assert validated.answers[0].score == score
    assert dict(validated.answers[0].probabilities) == relevance["probabilities"]
    assert dict(validated.answers[1].probabilities) == evidence["probabilities"]
    assert validated.answers[4].noul == noul
    for index in (0, 1, 4):
        quantities = {
            item.name: item.value for item in validated.answers[index].confidence_quantities
        }
        raw = result["answers"][validated.answers[index].question_id]
        assert quantities == {
            "confidence": raw["confidence"],
            "answer_confidence": raw["answer_confidence"],
        }


@pytest.mark.parametrize("value", [1.2, -0.1, True, math.nan, math.inf, "0.5", None])
def test_validate_result_choice_probability_outside_unit_interval_raises_probability_invalid(
    value: Any, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    result["answers"]["evidence_type"]["probabilities"]["question"] = value

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "probability_invalid"
    assert caught.value.details["question_id"] == "evidence_type"
    assert caught.value.details["field"] == "probabilities.question"


@pytest.mark.parametrize(
    ("question_id", "label", "labels_count"),
    [("evidence_type", "question", 5), ("theme", "performance", 8)],
)
def test_validate_result_probability_mass_beyond_rounding_raises_mass_out_of_tolerance(
    question_id: str, label: str, labels_count: int, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    excess = 2 * labels_count * contracts.ROUNDING_QUANTUM
    result["answers"][question_id]["probabilities"][label] += excess

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "probability_mass_out_of_tolerance"
    assert caught.value.details["question_id"] == question_id
    assert caught.value.details["observed"] == pytest.approx(1 + excess, abs=1e-9)


@pytest.mark.parametrize(
    ("question_id", "label", "labels_count"),
    [("evidence_type", "question", 5), ("theme", "performance", 8)],
)
def test_validate_result_probability_mass_within_rounding_is_accepted(
    question_id: str, label: str, labels_count: int, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    result["answers"][question_id]["probabilities"][label] += (
        0.5 * labels_count * contracts.ROUNDING_QUANTUM
    )

    validated = _validate(result, question_set)

    answer = next(item for item in validated.answers if item.question_id == question_id)
    assert dict(answer.probabilities)[label] == pytest.approx(
        result["answers"][question_id]["probabilities"][label]
    )


def test_validate_result_choice_not_a_requested_label_raises_choice_unknown(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    result["answers"]["theme"]["choice"] = "pricing"

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "choice_unknown"
    assert caught.value.details["observed"] == "pricing"


def test_validate_result_choice_not_maximal_raises_choice_not_maximal(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    result["answers"]["evidence_type"]["choice"] = "question"

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "choice_not_maximal"
    assert caught.value.details["observed"] == "question"


def test_validate_result_choice_tied_at_maximum_is_accepted_and_kept(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    answer = result["answers"]["evidence_type"]
    labels = list(answer["probabilities"])
    answer["probabilities"] = dict(zip(labels, [0.4, 0.4, 0.1, 0.05, 0.05], strict=True))
    answer["choice"] = labels[1]
    answer["answer_confidence"] = 0.4

    validated = _validate(result, question_set)

    assert validated.answers[1].choice == labels[1]


@pytest.mark.parametrize(
    ("question_id", "value"),
    [("evidence_type", 0.3848), ("evidence_type", 0.9), ("objective_relevance", 0.5)],
)
def test_validate_result_answer_confidence_off_by_more_than_rounding_raises_inconsistent(
    question_id: str, value: float, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    result["answers"][question_id]["answer_confidence"] = value

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "answer_confidence_inconsistent"
    assert caught.value.details["question_id"] == question_id
    assert caught.value.details["field"] == "answer_confidence"
    assert caught.value.details["observed"] == value


def test_validate_result_answer_confidence_within_rounding_is_accepted(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    nudge = 0.4 * contracts.ROUNDING_QUANTUM
    result["answers"]["evidence_type"]["answer_confidence"] = 0.3838 + nudge

    validated = _validate(result, question_set)

    quantities = {item.name: item.value for item in validated.answers[1].confidence_quantities}
    assert quantities["answer_confidence"] == pytest.approx(0.38382)


@pytest.mark.parametrize("field", ["confidence", "answer_confidence"])
@pytest.mark.parametrize("value", [1.2, -0.1, True, math.nan])
def test_validate_result_confidence_field_outside_unit_interval_raises_probability_invalid(
    field: str, value: Any, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    result["answers"]["evidence_type"][field] = value

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "probability_invalid"
    assert caught.value.details["field"] == field


@pytest.mark.parametrize(
    "mutate_legend",
    [
        pytest.param(lambda legend: legend.update({"0": "irrelevant"}), id="relabelled"),
        pytest.param(
            lambda legend: legend.update({"0": legend["2"], "2": legend["0"]}), id="values_swapped"
        ),
        pytest.param(lambda legend: legend.pop("2"), id="level_dropped"),
        pytest.param(lambda legend: legend.update({"3": "extra level"}), id="level_added"),
    ],
)
def test_validate_result_score_legend_changed_raises_score_legend_mismatch(
    mutate_legend: Callable[[dict[str, str]], object], question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    mutate_legend(result["answers"]["objective_relevance"]["legend"])

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "score_legend_mismatch"
    assert caught.value.details["question_id"] == "objective_relevance"


def test_validate_result_score_legend_keys_reordered_raises_score_legend_mismatch(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    answer = result["answers"]["objective_relevance"]
    answer["legend"] = dict(reversed(list(answer["legend"].items())))

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "score_legend_mismatch"


def test_validate_result_score_probability_keys_reordered_raises_level_order_mismatch(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    answer = result["answers"]["objective_relevance"]
    answer["probabilities"] = dict(reversed(list(answer["probabilities"].items())))

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "score_level_order_mismatch"
    assert caught.value.details["expected"] == ["0", "1", "2"]


@pytest.mark.parametrize("score", [-0.1, 2.1, 3, True, None, math.nan])
def test_validate_result_score_outside_level_range_raises_score_out_of_range(
    score: Any, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    result["answers"]["objective_relevance"]["score"] = score

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "score_out_of_range"
    assert caught.value.details["question_id"] == "objective_relevance"


@pytest.mark.parametrize("score", [0.5, 0.1436, 2.0])
def test_validate_result_score_inconsistent_with_distribution_raises(
    score: float, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    result["answers"]["objective_relevance"]["score"] = score

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "score_inconsistent_with_distribution"
    assert caught.value.details["observed"] == score
    assert caught.value.details["expected"] == pytest.approx(0.1432)


def test_validate_result_score_within_rounding_of_expected_level_is_accepted(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    result["answers"]["objective_relevance"]["score"] = 0.1433 + contracts.ROUNDING_QUANTUM

    validated = _validate(result, question_set)

    assert validated.answers[0].score == pytest.approx(0.14335)


@pytest.mark.parametrize(
    ("extra_key", "extra_value"),
    [("probabilities", {"0": 0.5, "1": 0.5}), ("x_jev_confidence", 0.5), ("legend", {"0": "x"})],
)
def test_validate_result_noul_with_choice_style_field_raises_answer_unknown_field(
    extra_key: str, extra_value: Any, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    result["answers"]["promotional_framing"][extra_key] = extra_value

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "answer_unknown_field"
    assert caught.value.details["question_id"] == "promotional_framing"
    assert caught.value.details["unexpected"] == [extra_key]


@pytest.mark.parametrize("field", ["confidence", "answer_confidence"])
def test_validate_result_noul_confidence_not_max_of_p_and_complement_raises(
    field: str, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    result["answers"]["promotional_framing"][field] = 0.5

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "noul_confidence_inconsistent"
    assert caught.value.details["field"] == field
    assert caught.value.details["expected"] == pytest.approx(0.9974)


def test_validate_result_noul_confidence_within_rounding_is_accepted(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    nudge = 0.4 * contracts.ROUNDING_QUANTUM
    result["answers"]["promotional_framing"]["confidence"] = 0.9974 - nudge

    validated = _validate(result, question_set)

    assert validated.answers[4].noul == 0.0026


def test_validate_result_noul_probability_above_half_uses_the_probability_as_confidence(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    answer = result["answers"]["promotional_framing"]
    answer.update({"noul": 0.8, "confidence": 0.8, "answer_confidence": 0.8})

    validated = _validate(result, question_set)

    assert validated.answers[4].noul == 0.8


@pytest.mark.parametrize("question_id", ["theme", "objective_relevance", "promotional_framing"])
def test_validate_result_unknown_answer_field_raises_answer_unknown_field(
    question_id: str, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    result["answers"][question_id]["bonus"] = 1

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "answer_unknown_field"
    assert caught.value.details["unexpected"] == ["bonus"]


@pytest.mark.parametrize(
    ("question_id", "field"),
    [("theme", "choice"), ("objective_relevance", "legend"), ("promotional_framing", "noul")],
)
def test_validate_result_missing_answer_field_raises_answer_shape_invalid(
    question_id: str, field: str, question_set: QuestionSet
) -> None:
    result = _recorded_result(0)
    del result["answers"][question_id][field]

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "answer_shape_invalid"
    assert caught.value.details["missing"] == [field]


def test_validate_result_non_object_answer_raises_answer_shape_invalid(
    question_set: QuestionSet,
) -> None:
    result = _recorded_result(0)
    result["answers"]["theme"] = ["documentation"]

    with pytest.raises(DecisionProtocolError) as caught:
        _validate(result, question_set)

    assert caught.value.details["reason"] == "answer_shape_invalid"
    assert caught.value.details["question_id"] == "theme"


# ---------------------------------------------------------------------------------------
# 6. parse_runtime_manifest
# ---------------------------------------------------------------------------------------


def _manifest_payload() -> dict[str, Any]:
    return build_manifest().to_dict()


def test_parse_runtime_manifest_round_trips_built_manifest() -> None:
    manifest = build_manifest()

    parsed = parse_runtime_manifest(manifest.to_dict())

    assert parsed == manifest
    assert parsed.sha256() == manifest.sha256()


@pytest.mark.parametrize(
    ("path", "value", "field"),
    [
        pytest.param(("extra",), 1, "manifest", id="unknown_top_key"),
        pytest.param(("backend",), _DELETE, "manifest", id="missing_top_key"),
        pytest.param(("code", "extra"), 1, "code", id="unknown_code_key"),
        pytest.param(("code", "license"), _DELETE, "code", id="missing_code_key"),
        pytest.param(("checkpoint", "extra"), 1, "checkpoint", id="unknown_checkpoint_key"),
        pytest.param(("wire", "extra"), 1, "wire", id="unknown_wire_key"),
        pytest.param(("checkpoint", "files", 0, "extra"), 1, "checkpoint.files[0]", id="file_key"),
        pytest.param(("manifest_version",), 2, "manifest_version", id="version_two"),
        pytest.param(("manifest_version",), True, "manifest_version", id="version_bool"),
        pytest.param(("manifest_version",), "1", "manifest_version", id="version_text"),
        pytest.param(("backend",), "other", "backend", id="backend_other"),
        pytest.param(("code", "commit"), "ffc1420", "code.commit", id="short_commit"),
        pytest.param(("code", "commit"), "F" * 40, "code.commit", id="uppercase_commit"),
        pytest.param(("checkpoint", "revision"), "abc", "checkpoint.revision", id="short_revision"),
        pytest.param(("code", "repository"), "", "code.repository", id="empty_repository"),
        pytest.param(("wire", "health_path"), "health", "wire.health_path", id="relative_wire"),
        pytest.param(("wire", "batch_path"), "/v1/x?y=1", "wire.batch_path", id="wire_query"),
        pytest.param(("checkpoint", "files"), [], "checkpoint.files", id="no_files"),
        pytest.param(
            ("checkpoint", "files", 0, "sha256"),
            "A" * 64,
            "checkpoint.files[0].sha256",
            id="uppercase_digest",
        ),
        pytest.param(
            ("checkpoint", "files", 0, "sha256"),
            "a" * 63,
            "checkpoint.files[0].sha256",
            id="short_digest",
        ),
        pytest.param(
            ("checkpoint", "files", 0, "path"),
            "/etc/config.json",
            "checkpoint.files[0].path",
            id="absolute_path",
        ),
        pytest.param(
            ("checkpoint", "files", 0, "path"),
            "../config.json",
            "checkpoint.files[0].path",
            id="dotdot_path",
        ),
        pytest.param(
            ("checkpoint", "files", 0, "path"),
            "a//config.json",
            "checkpoint.files[0].path",
            id="empty_part_path",
        ),
        pytest.param(
            ("checkpoint", "files", 0, "path"),
            "a\\config.json",
            "checkpoint.files[0].path",
            id="backslash_path",
        ),
        pytest.param(
            ("checkpoint", "files", 0, "path"),
            "./config.json",
            "checkpoint.files[0].path",
            id="dot_part_path",
        ),
        pytest.param(
            ("checkpoint", "files", 0, "path"), "", "checkpoint.files[0].path", id="empty_path"
        ),
        pytest.param(
            ("checkpoint", "files", 1, "path"),
            "config.json",
            "checkpoint.files[1].path",
            id="duplicate_path",
        ),
        pytest.param(
            ("checkpoint", "files", 0, "bytes"), 0, "checkpoint.files[0].bytes", id="zero_bytes"
        ),
        pytest.param(
            ("checkpoint", "files", 0, "bytes"), True, "checkpoint.files[0].bytes", id="bool_bytes"
        ),
        pytest.param(
            ("checkpoint", "files", 0, "bytes"), -5, "checkpoint.files[0].bytes", id="negative"
        ),
    ],
)
def test_parse_runtime_manifest_invalid_field_raises_manifest_invalid(
    path: tuple[PathKey, ...], value: Any, field: str
) -> None:
    payload = _manifest_payload()
    _apply(payload, path, value)

    with pytest.raises(DecisionIdentityError) as caught:
        parse_runtime_manifest(payload)

    assert caught.value.details["reason"] == "manifest_invalid"
    assert caught.value.details["field"] == field


def test_parse_runtime_manifest_non_object_section_raises_manifest_invalid() -> None:
    payload = _manifest_payload()
    payload["wire"] = ["/health"]

    with pytest.raises(DecisionIdentityError) as caught:
        parse_runtime_manifest(payload)

    assert caught.value.details["reason"] == "manifest_invalid"
    assert caught.value.details["field"] == "wire"


# ---------------------------------------------------------------------------------------
# 7. CheckpointVerifier
# ---------------------------------------------------------------------------------------


def test_checkpoint_verifier_matching_files_verifies_with_client_digest_basis(
    tmp_path: Path,
) -> None:
    manifest = build_manifest()
    repo = str(write_checkpoint(tmp_path))

    verification = CheckpointVerifier(manifest).verify(repo)

    assert verification.basis == VERIFICATION_BASIS == "client_file_sha256"
    assert verification.repo == repo
    assert verification.files == manifest.checkpoint.files


def test_checkpoint_verifier_second_verify_returns_cached_object_without_rehashing(
    tmp_path: Path,
) -> None:
    verifier = CheckpointVerifier(build_manifest())
    repo = str(write_checkpoint(tmp_path))

    first = verifier.verify(repo)
    hashed_after_first = verifier.files_hashed
    second = verifier.verify(repo)

    assert second is first
    assert hashed_after_first == 2
    assert verifier.files_hashed == hashed_after_first


def test_checkpoint_verifier_relative_repo_raises_repo_not_absolute() -> None:
    with pytest.raises(DecisionIdentityError) as caught:
        CheckpointVerifier(build_manifest()).verify("relative/checkpoint")

    assert caught.value.details["reason"] == "checkpoint_repo_not_absolute"
    assert caught.value.details["repo"] == "relative/checkpoint"


def test_checkpoint_verifier_missing_directory_raises_repo_missing(tmp_path: Path) -> None:
    missing = str(tmp_path / "absent")

    with pytest.raises(DecisionIdentityError) as caught:
        CheckpointVerifier(build_manifest()).verify(missing)

    assert caught.value.details["reason"] == "checkpoint_repo_missing"
    assert caught.value.details["repo"] == missing


@pytest.mark.parametrize("probe", ["is_dir", "is_file"])
def test_checkpoint_verifier_filesystem_probe_failures_are_typed_and_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe: str
) -> None:
    repo = write_checkpoint(tmp_path)
    target = repo if probe == "is_dir" else repo / "config.json"
    original_probe: Callable[[Path], bool] = getattr(Path, probe)
    attempts = 0

    def deny_access(path: Path) -> bool:
        nonlocal attempts
        if path == target:
            attempts += 1
            raise PermissionError("checkpoint access denied")
        return original_probe(path)

    monkeypatch.setattr(Path, probe, deny_access)
    verifier = CheckpointVerifier(build_manifest())
    failures: list[DecisionIdentityError] = []
    for _ in range(2):
        with pytest.raises(DecisionIdentityError) as caught:
            verifier.verify(str(repo))
        failures.append(caught.value)

    assert failures[0] is failures[1]
    assert failures[0].details["reason"] == "checkpoint_file_unreadable"
    assert failures[0].details["error_type"] == "PermissionError"
    assert attempts == 1
    assert verifier.files_hashed == 0


def test_checkpoint_verifier_deleted_file_raises_file_missing(tmp_path: Path) -> None:
    repo = write_checkpoint(tmp_path)
    (repo / "tokenizer" / "tokenizer.json").unlink()

    with pytest.raises(DecisionIdentityError) as caught:
        CheckpointVerifier(build_manifest()).verify(str(repo))

    assert caught.value.details["reason"] == "checkpoint_file_missing"
    assert caught.value.details["path"] == "tokenizer/tokenizer.json"


def test_checkpoint_verifier_directory_in_place_of_file_raises_file_missing(
    tmp_path: Path,
) -> None:
    repo = write_checkpoint(tmp_path)
    (repo / "config.json").unlink()
    (repo / "config.json").mkdir()

    with pytest.raises(DecisionIdentityError) as caught:
        CheckpointVerifier(build_manifest()).verify(str(repo))

    assert caught.value.details["reason"] == "checkpoint_file_missing"
    assert caught.value.details["path"] == "config.json"


def test_checkpoint_verifier_changed_byte_same_size_raises_digest_mismatch(
    tmp_path: Path,
) -> None:
    manifest = build_manifest()
    repo = write_checkpoint(tmp_path)
    original = (repo / "config.json").read_bytes()
    (repo / "config.json").write_bytes(b"X" + original[1:])
    expected = manifest.checkpoint.files[0]

    with pytest.raises(DecisionIdentityError) as caught:
        CheckpointVerifier(manifest).verify(str(repo))

    assert caught.value.details["reason"] == "checkpoint_file_digest_mismatch"
    assert caught.value.details["path"] == "config.json"
    assert caught.value.details["expected"] == expected.sha256
    assert caught.value.details["observed"] != expected.sha256


def test_checkpoint_verifier_failed_verification_is_cached_and_reraised_without_rehashing(
    tmp_path: Path,
) -> None:
    verifier = CheckpointVerifier(build_manifest())
    repo = write_checkpoint(tmp_path)
    original = (repo / "config.json").read_bytes()
    (repo / "config.json").write_bytes(b"X" + original[1:])
    with pytest.raises(DecisionIdentityError) as first:
        verifier.verify(str(repo))
    hashed_after_first = verifier.files_hashed

    (repo / "config.json").write_bytes(original)
    with pytest.raises(DecisionIdentityError) as second:
        verifier.verify(str(repo))

    assert second.value is first.value
    assert hashed_after_first == 1
    assert verifier.files_hashed == hashed_after_first


def test_checkpoint_verifier_appended_byte_raises_size_mismatch_before_hashing(
    tmp_path: Path,
) -> None:
    manifest = build_manifest()
    verifier = CheckpointVerifier(manifest)
    repo = write_checkpoint(tmp_path)
    with (repo / "config.json").open("ab") as handle:
        handle.write(b"\n")
    expected = manifest.checkpoint.files[0]

    with pytest.raises(DecisionIdentityError) as caught:
        verifier.verify(str(repo))

    assert caught.value.details["reason"] == "checkpoint_file_size_mismatch"
    assert caught.value.details["expected"] == expected.bytes
    assert caught.value.details["observed"] == expected.bytes + 1
    assert verifier.files_hashed == 0


def test_checkpoint_verifier_symlink_outside_repo_raises_file_escapes_repo(
    tmp_path: Path,
) -> None:
    repo = write_checkpoint(tmp_path / "repo")
    outside = write_checkpoint(tmp_path / "outside")
    (repo / "config.json").unlink()
    (repo / "config.json").symlink_to(outside / "config.json")

    with pytest.raises(DecisionIdentityError) as caught:
        CheckpointVerifier(build_manifest()).verify(str(repo))

    assert caught.value.details["reason"] == "checkpoint_file_escapes_repo"
    assert caught.value.details["path"] == "config.json"


def test_checkpoint_verifier_symlink_inside_repo_is_accepted(tmp_path: Path) -> None:
    manifest = build_manifest()
    repo = write_checkpoint(tmp_path)
    real = repo / "config.real.json"
    (repo / "config.json").rename(real)
    (repo / "config.json").symlink_to(real)

    verification = CheckpointVerifier(manifest).verify(str(repo))

    assert verification.files == manifest.checkpoint.files


# ---------------------------------------------------------------------------------------
# 8. Question identity
# ---------------------------------------------------------------------------------------


def test_build_question_set_declares_six_questions_in_fixed_order(
    question_set: QuestionSet,
) -> None:
    assert question_set.question_ids() == (
        "objective_relevance",
        "evidence_type",
        "request_intent",
        "actionability",
        "promotional_framing",
        "theme",
    )
    assert [question.question_type for question in question_set.questions] == [
        QuestionType.SCORE,
        QuestionType.CHOICE,
        QuestionType.CHOICE,
        QuestionType.SCORE,
        QuestionType.NOUL,
        QuestionType.CHOICE,
    ]


def test_build_question_set_every_instruction_names_comment_and_attribution(
    question_set: QuestionSet,
) -> None:
    for question in question_set.questions:
        assert "`comment`" in question.instructions
        assert ATTRIBUTION in question.instructions


def test_build_question_set_theme_options_are_vocabulary_themes_then_uncertain(
    question_set: QuestionSet, vocabulary: DomainVocabulary
) -> None:
    theme = question_set.by_id()["theme"]
    assert isinstance(theme, contracts.ChoiceQuestion)

    assert theme.labels == (*vocabulary.theme_rules, contracts.OTHER_OR_UNCERTAIN)
    assert vocabulary.default_theme not in theme.labels
    assert question_set.vocabulary.themes == tuple(vocabulary.theme_rules)


def test_build_question_set_same_inputs_yield_same_sha256(vocabulary: DomainVocabulary) -> None:
    first = build_question_set(ObjectiveType.FRICTION_ANALYSIS, vocabulary)
    second = build_question_set(ObjectiveType.FRICTION_ANALYSIS, vocabulary)

    assert first.sha256() == second.sha256()


def _replace_question(qs: QuestionSet, question_id: str, **changes: Any) -> QuestionSet:
    questions = tuple(
        dataclasses.replace(item, **changes) if item.question_id == question_id else item
        for item in qs.questions
    )
    return dataclasses.replace(qs, questions=questions)


def _swap_two_choice_options(qs: QuestionSet) -> QuestionSet:
    question = qs.by_id()["evidence_type"]
    assert isinstance(question, contracts.ChoiceQuestion)
    options = list(question.options)
    options[0], options[1] = options[1], options[0]
    return _replace_question(qs, "evidence_type", options=tuple(options))


def _swap_two_score_levels(qs: QuestionSet) -> QuestionSet:
    question = qs.by_id()["actionability"]
    assert isinstance(question, contracts.ScoreQuestion)
    levels = list(question.levels)
    levels[0], levels[1] = levels[1], levels[0]
    return _replace_question(qs, "actionability", levels=tuple(levels))


def _reverse_option_order(qs: QuestionSet) -> QuestionSet:
    question = qs.by_id()["theme"]
    assert isinstance(question, contracts.ChoiceQuestion)
    return _replace_question(qs, "theme", options=tuple(reversed(question.options)))


def _reorder_questions(qs: QuestionSet) -> QuestionSet:
    questions = list(qs.questions)
    questions[0], questions[1] = questions[1], questions[0]
    return dataclasses.replace(qs, questions=tuple(questions))


def _reword_instructions(qs: QuestionSet) -> QuestionSet:
    return _replace_question(qs, "theme", instructions="Pick a theme.")


@pytest.mark.parametrize(
    "variant",
    [
        pytest.param(_swap_two_choice_options, id="choice_options_swapped"),
        pytest.param(_reverse_option_order, id="theme_options_reversed"),
        pytest.param(_swap_two_score_levels, id="score_levels_swapped"),
        pytest.param(_reorder_questions, id="questions_reordered"),
        pytest.param(_reword_instructions, id="instructions_changed"),
    ],
)
def test_question_set_sha256_changes_when_identity_content_changes(
    variant: Callable[[QuestionSet], QuestionSet], question_set: QuestionSet
) -> None:
    assert variant(question_set).sha256() != question_set.sha256()


def test_question_set_sha256_changes_when_objective_changes(
    question_set: QuestionSet, vocabulary: DomainVocabulary
) -> None:
    other = build_question_set(ObjectiveType.FEATURE_DEMAND, vocabulary)

    assert other.sha256() != question_set.sha256()
    assert other.objective.objective == "feature_demand"


def test_inference_settings_sha256_is_independent_of_question_changes(
    question_set: QuestionSet,
) -> None:
    before = PINNED_INFERENCE_SETTINGS.sha256()

    changed = _swap_two_choice_options(question_set)
    rebuilt = InferenceSettings(model="english", lang="en", max_len=512, head_max_len=256)

    assert changed.sha256() != question_set.sha256()
    assert PINNED_INFERENCE_SETTINGS.sha256() == before == rebuilt.sha256()


def test_inference_settings_sha256_changes_when_a_setting_changes() -> None:
    narrower = dataclasses.replace(PINNED_INFERENCE_SETTINGS, head_max_len=128)

    assert narrower.sha256() != PINNED_INFERENCE_SETTINGS.sha256()


@pytest.mark.parametrize(
    "changes",
    [
        {"head_max_len": 0},
        {"head_max_len": 513, "max_len": 513},
        {"head_max_len": 513},
        {"model": ""},
        {"lang": ""},
    ],
)
def test_inference_settings_invalid_values_raise_value_error(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        dataclasses.replace(PINNED_INFERENCE_SETTINGS, **changes)


def test_question_set_duplicate_question_ids_raise_value_error(question_set: QuestionSet) -> None:
    duplicated = (*question_set.questions, question_set.questions[0])

    with pytest.raises(ValueError):
        dataclasses.replace(question_set, questions=duplicated)


def test_question_set_choice_with_repeated_label_raises_value_error(
    question_set: QuestionSet,
) -> None:
    question = question_set.by_id()["evidence_type"]
    assert isinstance(question, contracts.ChoiceQuestion)

    with pytest.raises(ValueError):
        _replace_question(question_set, "evidence_type", options=(question.options[0],) * 2)


def test_build_question_set_theme_named_other_or_uncertain_raises_theme_vocabulary_invalid(
    vocabulary: DomainVocabulary,
) -> None:
    colliding = dataclasses.replace(
        vocabulary,
        theme_rules={**vocabulary.theme_rules, contracts.OTHER_OR_UNCERTAIN: ("unsure",)},
    )

    with pytest.raises(DecisionInputError) as caught:
        build_question_set(ObjectiveType.FRICTION_ANALYSIS, colliding)

    assert caught.value.details["reason"] == "theme_vocabulary_invalid"
    assert caught.value.details["theme"] == contracts.OTHER_OR_UNCERTAIN


def test_build_question_set_vocabulary_without_themes_raises_theme_vocabulary_invalid(
    vocabulary: DomainVocabulary,
) -> None:
    empty = dataclasses.replace(vocabulary, theme_rules={})

    with pytest.raises(DecisionInputError) as caught:
        build_question_set(ObjectiveType.FRICTION_ANALYSIS, empty)

    assert caught.value.details["reason"] == "theme_vocabulary_invalid"
    assert caught.value.details["domain"] == vocabulary.domain


def test_current_plan_for_fixture_thread_reproduces_recorded_request_body(
    question_set: QuestionSet,
) -> None:
    request_text, _payload = recorded_batch(PLACEHOLDER_REPO)
    thread = load_canonical_thread(CANONICAL_FIXTURE)

    states = build_comment_states(thread, PINNED_CONTEXT_POLICY)
    plans = plan_batches(states, question_set, PINNED_INFERENCE_SETTINGS)

    assert len(plans) == 1
    assert encode_request_body(plans[0].body).decode("utf-8") == request_text


# ---------------------------------------------------------------------------------------
# 9. build_comment_states / plan_batches
# ---------------------------------------------------------------------------------------


def test_build_comment_states_fixture_thread_gives_seven_sendable_states_in_order() -> None:
    thread = load_canonical_thread(CANONICAL_FIXTURE)

    states = build_comment_states(thread, PINNED_CONTEXT_POLICY)

    assert [item.index for item in states] == list(range(RECORDED_RESULT_COUNT))
    assert [item.comment_id for item in states] == [c.comment_id for c in thread.comments]
    assert all(item.state is not None and item.refusal_reasons == () for item in states)
    assert [item.comment_text_sha256 for item in states] == [
        comment_text_sha256(comment.body) for comment in thread.comments
    ]


def test_build_comment_states_fixture_state_carries_comment_and_title_only() -> None:
    thread = load_canonical_thread(CANONICAL_FIXTURE)

    first = build_comment_states(thread, PINNED_CONTEXT_POLICY)[0]

    assert first.state == {"comment": thread.comments[0].body, "thread_title": thread.title}
    assert first.windows == (ContextWindow("thread_title", len(thread.title), len(thread.title)),)
    assert first.parent_context == "none"
    assert first.parent_text_sha256 is None
    assert first.state_sha256() == contracts.canonical_sha256(first.state)


def test_build_comment_states_comment_over_budget_is_refused_not_shortened() -> None:
    limit = PINNED_CONTEXT_POLICY.comment_max_chars
    thread = _thread([_comment("c:1", "x" * (limit + 1))])

    states = build_comment_states(thread, PINNED_CONTEXT_POLICY)

    assert states[0].state is None
    assert states[0].refusal_reasons == ("comment_exceeds_client_char_budget",)
    assert states[0].state_chars == 0
    assert states[0].comment_text_sha256 == comment_text_sha256("x" * (limit + 1))


def test_build_comment_states_comment_exactly_at_budget_is_sent_whole() -> None:
    limit = PINNED_CONTEXT_POLICY.comment_max_chars
    thread = _thread([_comment("c:1", "y" * limit)])

    states = build_comment_states(thread, PINNED_CONTEXT_POLICY)

    assert states[0].refusal_reasons == ()
    assert states[0].state is not None
    assert states[0].state["comment"] == "y" * limit


def test_plan_batches_refused_comment_never_appears_in_a_plan(question_set: QuestionSet) -> None:
    too_long = "z" * (PINNED_CONTEXT_POLICY.comment_max_chars + 1)
    comments = [_comment("c:1", "first"), _comment("c:2", too_long), _comment("c:3", "last")]
    thread = _thread(comments)

    states = build_comment_states(thread, PINNED_CONTEXT_POLICY)
    plans = plan_batches(states, question_set, PINNED_INFERENCE_SETTINGS)

    assert len(plans) == 1
    assert plans[0].item_indexes == (0, 2)
    assert [state["comment"] for state in plans[0].body["states"]] == ["first", "last"]


def test_plan_batches_only_refused_comments_yield_no_plans(question_set: QuestionSet) -> None:
    too_long = "z" * (PINNED_CONTEXT_POLICY.comment_max_chars + 1)
    states = build_comment_states(_thread([_comment("c:1", too_long)]), PINNED_CONTEXT_POLICY)

    assert plan_batches(states, question_set, PINNED_INFERENCE_SETTINGS) == ()


def test_build_comment_states_long_thread_body_is_windowed_with_exact_coverage() -> None:
    thread_body = "b" * 5000
    comment = _comment("c:1", "short comment")
    thread = _thread([comment], body=thread_body)

    item = build_comment_states(thread, PINNED_CONTEXT_POLICY)[0]

    assert item.state is not None
    assert item.state["thread_body"] == thread_body[: PINNED_CONTEXT_POLICY.body_window_chars]
    assert item.state["comment"] == comment.body
    body_window = ContextWindow("thread_body", 5000, PINNED_CONTEXT_POLICY.body_window_chars)
    assert body_window in item.windows
    assert body_window.complete is False


def test_build_comment_states_long_title_is_windowed_with_exact_coverage() -> None:
    thread = _thread([_comment("c:1", "ok")], title="t" * 1000)

    item = build_comment_states(thread, PINNED_CONTEXT_POLICY)[0]

    assert item.state is not None
    assert item.state["thread_title"] == "t" * PINNED_CONTEXT_POLICY.title_window_chars
    assert item.windows[0] == ContextWindow(
        "thread_title", 1000, PINNED_CONTEXT_POLICY.title_window_chars
    )


def test_build_comment_states_short_thread_body_is_complete_window() -> None:
    thread = _thread([_comment("c:1", "ok")], body="short body")

    item = build_comment_states(thread, PINNED_CONTEXT_POLICY)[0]

    assert item.state is not None
    assert item.state["thread_body"] == "short body"
    assert ContextWindow("thread_body", 10, 10) in item.windows
    assert ContextWindow("thread_body", 10, 10).complete is True


def test_build_comment_states_parent_present_includes_windowed_parent_context() -> None:
    parent = _comment("c:1", "p" * 1000)
    child = _comment("c:2", "reply", parent="c:1")

    states = build_comment_states(_thread([parent, child]), PINNED_CONTEXT_POLICY)

    assert states[0].parent_context == "none"
    assert states[1].parent_context == "included"
    assert states[1].parent_comment_id == "c:1"
    assert states[1].parent_text_sha256 == comment_text_sha256(parent.body)
    assert states[1].state is not None
    assert states[1].state["parent_comment"] == "p" * PINNED_CONTEXT_POLICY.parent_window_chars
    assert states[1].state["comment"] == "reply"
    assert ContextWindow("parent_comment", 1000, PINNED_CONTEXT_POLICY.parent_window_chars) in (
        states[1].windows
    )


def test_build_comment_states_parent_absent_from_snapshot_is_marked_missing() -> None:
    orphan = _comment("c:1", "reply to nothing", parent="c:gone")

    item = build_comment_states(_thread([orphan]), PINNED_CONTEXT_POLICY)[0]

    assert item.parent_context == "missing_from_snapshot"
    assert item.parent_comment_id == "c:gone"
    assert item.parent_text_sha256 is None
    assert item.state is not None
    assert "parent_comment" not in item.state


def test_build_comment_states_duplicate_comment_ids_raise_duplicate_comment_id() -> None:
    thread = _thread([_comment("c:1", "one"), _comment("c:1", "other")])

    with pytest.raises(DecisionInputError) as caught:
        build_comment_states(thread, PINNED_CONTEXT_POLICY)

    assert caught.value.details["reason"] == "duplicate_comment_id"
    assert caught.value.details["comment_id"] == "c:1"


def test_build_comment_states_state_over_server_char_limit_is_refused() -> None:
    policy = dataclasses.replace(PINNED_CONTEXT_POLICY, comment_max_chars=60_000)
    thread = _thread([_comment("c:1", "w" * (contracts.MAX_STATE_CHARS + 1))])

    item = build_comment_states(thread, policy)[0]

    assert item.state is None
    assert item.refusal_reasons == ("state_exceeds_server_char_limit",)
    assert item.state_chars > contracts.MAX_STATE_CHARS
    assert item.windows != ()


def test_plan_batches_sixty_five_states_split_into_64_and_1_preserving_order(
    question_set: QuestionSet,
) -> None:
    comments = [_comment(f"c:{i}", f"comment {i}") for i in range(65)]

    states = build_comment_states(_thread(comments), PINNED_CONTEXT_POLICY)
    plans = plan_batches(states, question_set, PINNED_INFERENCE_SETTINGS)

    assert [plan.batch_index for plan in plans] == [0, 1]
    assert [len(plan.item_indexes) for plan in plans] == [64, 1]
    assert plans[0].item_indexes == tuple(range(64))
    assert plans[1].item_indexes == (64,)
    assert [state["comment"] for state in plans[1].body["states"]] == ["comment 64"]


def test_plan_batches_sixty_four_states_stay_in_one_plan(question_set: QuestionSet) -> None:
    comments = [_comment(f"c:{i}", f"comment {i}") for i in range(64)]

    states = build_comment_states(_thread(comments), PINNED_CONTEXT_POLICY)
    plans = plan_batches(states, question_set, PINNED_INFERENCE_SETTINGS)

    assert len(plans) == 1
    assert len(plans[0].body["states"]) == 64


def test_plan_batches_states_near_body_limit_split_by_bytes(question_set: QuestionSet) -> None:
    policy = ContextPolicy(
        version=1,
        comment_max_chars=45_000,
        title_window_chars=300,
        body_window_chars=600,
        parent_window_chars=400,
    )
    comments = [_comment(f"c:{i}", "q" * 45_000) for i in range(64)]

    states = build_comment_states(_thread(comments), policy)
    plans = plan_batches(states, question_set, PINNED_INFERENCE_SETTINGS)

    assert len(plans) > 1
    assert len(plans[0].item_indexes) < contracts.MAX_BATCH_STATES
    assert all(len(encode_request_body(plan.body)) <= contracts.MAX_BODY_BYTES for plan in plans)
    assert [index for plan in plans for index in plan.item_indexes] == list(range(64))


def test_plan_batches_bodies_never_carry_server_controls(question_set: QuestionSet) -> None:
    comments = [_comment(f"c:{i}", f"comment {i}") for i in range(65)]

    plans = plan_batches(
        build_comment_states(_thread(comments), PINNED_CONTEXT_POLICY),
        question_set,
        PINNED_INFERENCE_SETTINGS,
    )

    for plan in plans:
        assert list(plan.body) == [
            "model",
            "lang",
            "max_len",
            "head_max_len",
            "questions",
            "states",
        ]
        encoded = encode_request_body(plan.body).decode("utf-8")
        for control in ("sort_by_length", "batch_size", "min_confidence"):
            assert control not in plan.body
            assert f'"{control}"' not in encoded


# ---------------------------------------------------------------------------------------
# cross-boundary: client -> split -> validate -> verifier over the recorded exchange
# ---------------------------------------------------------------------------------------


def test_recorded_exchange_through_client_validation_and_verifier_end_to_end(
    tmp_path: Path, question_set: QuestionSet
) -> None:
    manifest = build_manifest()
    repo = write_checkpoint(tmp_path)
    request_text, payload = recorded_batch(repo)
    transport = routed_transport(
        lambda _request: json_response(HEALTH_PAYLOAD), lambda _request: json_response(payload)
    )

    with _client(transport) as client:
        health = validate_health(client.health().payload, WIRE)
        exchange = client.post_batch(json.loads(request_text))
    results = split_batch_results(exchange.payload, RECORDED_RESULT_COUNT)
    validated = [validate_result(item, question_set, PINNED_INFERENCE_SETTINGS) for item in results]
    verification = CheckpointVerifier(manifest).verify(validated[0].routing.repo)

    assert health.revision is None
    assert exchange.request_body == request_text
    assert {item.routing.repo for item in validated} == {str(repo)}
    assert verification.files == manifest.checkpoint.files
