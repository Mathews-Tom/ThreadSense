"""Consumer-visible existing-task capture and offline replay boundaries."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import Any

import httpx
import pytest

from threadsense.config import RuntimeConfig
from threadsense.decisions.artifact import decision_bytes
from threadsense.decisions.comparators import observe_generative_availability
from threadsense.decisions.evaluation_contracts import Partition
from threadsense.decisions.generative import (
    GenerativeInput,
    capture_generative_theme,
    generative_capture_bytes,
    replay_generative_theme,
)
from threadsense.domains.vocabulary import DomainVocabulary
from threadsense.errors import DecisionEvaluationError
from threadsense.models.canonical import AuthorRef, Comment, ProvenanceMetadata, SourceRef, Thread


@pytest.fixture
def arguments() -> dict[str, Any]:
    author = AuthorRef("author", None)
    comments = [
        Comment("t", cid, None, author, body, 1, 0.0, 0, "")
        for cid, body in (
            ("a", "A broken interface"),
            ("b", "Something new"),
            ("long", "x" * 301),
            ("excluded", "Not typed eligible"),
        )
    ]
    thread = Thread(
        "t",
        SourceRef("reddit", "test", "t", ""),
        "Title context",
        None,
        "",
        author,
        comments,
        len(comments),
        ProvenanceMetadata("", "0" * 64, 0.0, 0.0, 2, "canonical-thread-v2"),
    )
    vocabulary = DomainVocabulary(
        "test",
        "1",
        {"usability": ("interface",), "general_feedback": ()},
        (),
        (),
        (),
        "usability",
        "usability",
        "general_feedback",
    )
    return {
        "inputs": (GenerativeInput(thread, ("a", "b", "long"), Partition.EVALUATION),),
        "vocabulary": vocabulary,
        "bindings": {
            "dataset_manifest_sha256": "1" * 64,
            "labels_sha256": "2" * 64,
            "question_set_sha256": "3" * 64,
            "annotations": [{"thread_id": "t", "decision_sha256": "4" * 64}],
        },
        "runtime_config": RuntimeConfig(model="served", repair_retries=0),
    }


def completion(classifications: list[Any]) -> dict[str, Any]:
    return {
        "id": "reply",
        "object": "chat.completion",
        "model": "served-identity",
        "choices": [
            {
                "finish_reason": "stop",
                "message": {
                    "role": "assistant",
                    "content": json.dumps({"classifications": classifications}),
                },
            }
        ],
    }


def transport(rows: list[Any], requests: list[httpx.Request]) -> httpx.MockTransport:
    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": "served"}]})
        return httpx.Response(200, json=completion(rows))

    return httpx.MockTransport(handle)


def answers() -> list[dict[str, Any]]:
    return [
        {"comment_id": "b", "theme": "novel_theme", "confidence": 0.7},
        {"comment_id": "a", "theme": "usability", "confidence": 0.9},
    ]


def test_capture_association_full_body_and_offline_replay(
    arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[httpx.Request] = []
    result = capture_generative_theme(**arguments, transport=transport(answers(), requests))
    assert result.capture is not None
    assert [row["comment_id"] for row in result.rows] == ["a", "b"]
    assert result.rows[0]["status"] == "decided"
    assert result.rows[1]["label"] == "other_or_uncertain"
    assert result.rows[1]["reason"] == "new_theme"
    assert {row["comment_id"]: row["reason"] for row in result.unsupported} == {
        "long": "body_exceeds_300_characters",
        "excluded": "typed_ineligible_or_unsupported_language",
    }
    prompt = json.loads(requests[-1].content)["messages"][-1]["content"]
    assert "Title context" in prompt and "A broken interface" in prompt
    assert "x" * 301 not in prompt

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("offline replay attempted HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    content = generative_capture_bytes(result.capture)
    replay = replay_generative_theme(**arguments, content=content)
    assert replay.rows == result.rows
    assert replay.unsupported == result.unsupported
    assert replay.capture is not None
    assert replay.capture == result.capture
    assert generative_capture_bytes(replay.capture) == content
    assert replay.availability["status"] == "available"
    assert replay.availability["reason"] == "replayed_frozen_capture"
    assert replay.availability["method"] is None
    assert replay.availability["served_models"] is None
    assert replay.availability["captured_availability"] == result.availability
    assert result.availability["method"] is not None
    assert result.capture["availability"] == result.availability
    for field in ("provider", "enabled", "base_url", "model"):
        assert replay.availability[field] == result.availability[field]


@pytest.mark.parametrize(
    "rows",
    [
        [],
        answers() + [answers()[0]],
        [{"comment_id": "extra", "theme": "usability", "confidence": 1}],
        [{"comment_id": "a", "theme": "usability", "confidence": True}, answers()[0]],
        [{"comment_id": "a", "theme": "usability", "confidence": 1.1}, answers()[0]],
        [{"comment_id": "a", "theme": "usability", "confidence": 10**400}, answers()[0]],
        [42, answers()[0]],
    ],
)
def test_malformed_association_never_scores(arguments: dict[str, Any], rows: list[Any]) -> None:
    result = capture_generative_theme(**arguments, transport=transport(rows, []))
    assert result.availability["status"] == "unavailable"
    assert result.availability["reason"] == "inference_failed"
    assert result.capture is None and result.rows == ()


@pytest.mark.parametrize(
    ("location", "malformed"),
    [
        (
            "content",
            json.dumps(
                {
                    "classifications": [
                        {"comment_id": "a", "theme": "usability", "confidence": 10**400},
                        answers()[0],
                    ]
                }
            ),
        ),
        ("content", '{"classifications":[{"confidence":' + "9" * 5000 + "}]}"),
        ("content", '{"classifications":' + "[" * 2000 + "0" + "]" * 2000 + "}"),
        ("content", "{"),
        ("response", '{"unexpected":' + "9" * 5000 + "}"),
        ("response", '{"unexpected":' + "[" * 2000 + "0" + "]" * 2000 + "}"),
        ("response", "{"),
    ],
    ids=[
        "overflowing-confidence",
        "model-integer-decoder-limit",
        "deep-model-content",
        "malformed-model-json",
        "response-integer-decoder-limit",
        "deep-response",
        "malformed-response-json",
    ],
)
def test_invalid_external_response_fails_acquisition_and_replay(
    arguments: dict[str, Any], location: str, malformed: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    response_text = malformed
    if location == "content":
        body = completion([])
        body["choices"][0]["message"]["content"] = malformed
        response_text = json.dumps(body)
    calls: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": "served"}]})
        return httpx.Response(200, content=response_text.encode())

    result = capture_generative_theme(**arguments, transport=httpx.MockTransport(handle))
    assert calls == ["GET", "POST"]
    assert result.availability["status"] == "unavailable"
    assert result.availability["reason"] == "inference_failed"
    assert result.capture is None and result.rows == ()
    assert {row["comment_id"] for row in result.unsupported} == {"long", "excluded"}

    valid = capture_generative_theme(**arguments, transport=transport(answers(), []))
    assert valid.capture is not None
    altered = json.loads(generative_capture_bytes(valid.capture))
    altered["batches"][0]["exchanges"][0]["response_text"] = response_text
    altered.pop("capture_sha256")
    altered["capture_sha256"] = hashlib.sha256(decision_bytes(altered)).hexdigest()

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("invalid-response replay attempted HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    with pytest.raises(DecisionEvaluationError):
        replay_generative_theme(**arguments, content=generative_capture_bytes(altered))


@pytest.mark.parametrize(
    "content",
    [
        b'{"unexpected":' + b"9" * 5000 + b"}",
        b'{"unexpected":' + b"[" * 2000 + b"0" + b"]" * 2000 + b"}",
    ],
    ids=["capture-integer-decoder-limit", "deep-capture"],
)
def test_invalid_capture_json_has_bounded_failure(
    arguments: dict[str, Any], content: bytes
) -> None:
    with pytest.raises(DecisionEvaluationError):
        replay_generative_theme(**arguments, content=content)


def test_stale_and_tampered_capture_rejected(arguments: dict[str, Any]) -> None:
    result = capture_generative_theme(**arguments, transport=transport(answers(), []))
    assert result.capture is not None
    content = generative_capture_bytes(result.capture)
    changed = {**arguments, "bindings": {**arguments["bindings"], "labels_sha256": "9" * 64}}
    with pytest.raises(DecisionEvaluationError):
        replay_generative_theme(**changed, content=content)
    altered = json.loads(content)
    altered["batches"][0]["rows"][0]["label"] = "invented"
    with pytest.raises(DecisionEvaluationError):
        replay_generative_theme(**arguments, content=generative_capture_bytes(altered))
    # Even a recomputed envelope cannot make stored outcomes override raw responses.
    altered.pop("capture_sha256")
    altered["capture_sha256"] = hashlib.sha256(decision_bytes(altered)).hexdigest()
    with pytest.raises(DecisionEvaluationError):
        replay_generative_theme(**arguments, content=generative_capture_bytes(altered))


@pytest.mark.parametrize(
    ("runtime", "reason"),
    [
        (RuntimeConfig(enabled=False), "disabled_by_configuration"),
        (
            RuntimeConfig(enabled=False, base_url="http://localhost:11434", chat_path="//bad?"),
            "disabled_by_configuration",
        ),
        (RuntimeConfig(base_url="http://localhost:11434"), "endpoint_not_literal_loopback"),
        (RuntimeConfig(base_url="http://192.0.2.1:80"), "endpoint_not_literal_loopback"),
        (RuntimeConfig(base_url="http://127.0.0.1:bad"), "endpoint_invalid"),
    ],
)
def test_no_request_guards_are_not_observations(
    arguments: dict[str, Any], runtime: RuntimeConfig, reason: str
) -> None:
    def forbidden(request: httpx.Request) -> httpx.Response:
        pytest.fail("guard made a request")

    mock = httpx.MockTransport(forbidden)
    observed = observe_generative_availability(runtime, transport=mock)
    result = capture_generative_theme(**{**arguments, "runtime_config": runtime}, transport=mock)
    assert result.capture is None and result.rows == ()
    assert result.availability == observed
    assert observed["status"] == "not_observed"
    assert observed["reason"] == reason
    assert observed["method"] is None
    assert observed["served_models"] is None
    assert observed["provider"] == "local_openai_compatible"
    assert observed["enabled"] == runtime.enabled
    assert observed["base_url"] == runtime.base_url
    assert observed["model"] == runtime.model


@pytest.mark.parametrize(
    ("endpoint", "reason"),
    [
        ("http://127.0.0.1:9999/v1/chat/completions", "endpoint_invalid"),
        ("http://localhost:11434/v1/chat/completions", "endpoint_not_literal_loopback"),
        ("http://192.0.2.1:80/v1/chat/completions", "endpoint_not_literal_loopback"),
        ("http://[::1/v1/chat/completions", "endpoint_invalid"),
        ("http://127.0.0.1:bad/v1/chat/completions", "endpoint_invalid"),
        ("http://127.0.0.1:11434/v1/chat/completions?x=1", "endpoint_invalid"),
        ("http://127.0.0.1:11434/v1/chat/completions?", "endpoint_invalid"),
        ("http://127.0.0.1:11434/v1/chat/completions#part", "endpoint_invalid"),
        ("http://127.0.0.1:11434/v1/chat/completions#", "endpoint_invalid"),
        ("http://127.0.0.1:11434//remote/chat", "endpoint_invalid"),
        ("http://127.0.0.1:11434/v1/chat/\ncompletions", "endpoint_invalid"),
        ("http://127.0.0.1:11434/v1/chat/\x01completions", "endpoint_invalid"),
        ("http://127.0.0.1:11434/v1/chat/\x7fcompletions", "endpoint_invalid"),
    ],
)
def test_invalid_chat_endpoint_precedes_listing(
    arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch, endpoint: str, reason: str
) -> None:
    def forbidden(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid chat endpoint made a listing or generation request")

    monkeypatch.setattr(RuntimeConfig, "chat_endpoint", property(lambda self: endpoint))
    result = capture_generative_theme(**arguments, transport=httpx.MockTransport(forbidden))
    assert result.capture is None and result.rows == ()
    assert result.availability["status"] == "not_observed"
    assert result.availability["reason"] == reason
    assert result.availability["method"] is None
    assert result.availability["served_models"] is None
    assert result.availability["provider"] == "local_openai_compatible"
    runtime = arguments["runtime_config"]
    assert result.availability["enabled"] == runtime.enabled
    assert result.availability["base_url"] == runtime.base_url
    assert result.availability["model"] == runtime.model


def test_absent_served_model_and_redirect_are_unavailable(arguments: dict[str, Any]) -> None:
    absent = httpx.MockTransport(lambda request: httpx.Response(200, json={"data": []}))
    assert (
        capture_generative_theme(**arguments, transport=absent).availability["reason"]
        == "configured_model_not_served"
    )
    calls: list[str] = []

    def redirect(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(302, headers={"location": "http://192.0.2.1/remote"})

    result = capture_generative_theme(**arguments, transport=httpx.MockTransport(redirect))
    assert result.capture is None and len(calls) == 1


def test_proxy_environment_is_disabled(
    arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://192.0.2.1:8080")
    monkeypatch.setenv("ALL_PROXY", "http://192.0.2.1:8080")
    original = httpx.Client
    seen: list[bool] = []

    def client(*args: Any, **kwargs: Any) -> httpx.Client:
        seen.append(kwargs.get("trust_env", True))
        return original(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client)
    result = capture_generative_theme(**arguments, transport=transport(answers(), []))
    assert result.capture is not None and seen == [False, False]


def test_exact_300_characters_and_default_abstention(arguments: dict[str, Any]) -> None:
    item = arguments["inputs"][0]
    comment = replace(item.thread.comments[0], body="x" * 300)
    thread = replace(item.thread, comments=[comment], comment_count=1)
    arguments["inputs"] = (replace(item, thread=thread, comment_ids=("a",)),)
    result = capture_generative_theme(
        **arguments,
        transport=transport(
            [{"comment_id": "a", "theme": "general_feedback", "confidence": 0.5}], []
        ),
    )
    assert result.unsupported == ()
    assert result.rows[0]["status"] == "abstained"
    assert result.rows[0]["reason"] == "default_theme"


def test_replay_reconstructs_request_and_validates_all_exchanges(arguments: dict[str, Any]) -> None:
    result = capture_generative_theme(**arguments, transport=transport(answers(), []))
    assert result.capture is not None
    for mutation in ("request", "extra_exchange", "response_model"):
        altered = json.loads(generative_capture_bytes(result.capture))
        batch = altered["batches"][0]
        if mutation == "request":
            batch["exchanges"][0]["request"]["messages"][0]["content"] = "different prompt"
        elif mutation == "extra_exchange":
            batch["exchanges"].append(batch["exchanges"][0])
        else:
            batch["returned_model"] = "invented-model"
        altered.pop("capture_sha256")
        altered["capture_sha256"] = hashlib.sha256(decision_bytes(altered)).hexdigest()
        with pytest.raises(DecisionEvaluationError):
            replay_generative_theme(**arguments, content=generative_capture_bytes(altered))


def test_chat_redirect_does_not_follow_or_score(arguments: dict[str, Any]) -> None:
    calls: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": "served"}]})
        return httpx.Response(307, headers={"location": "http://192.0.2.1/inference"})

    result = capture_generative_theme(**arguments, transport=httpx.MockTransport(handle))
    assert calls == ["GET", "POST"]
    assert result.availability["status"] == "unavailable"
    assert result.availability["failure_details"]["reason"] == "chat_http_status"
    assert result.capture is None and result.rows == ()
