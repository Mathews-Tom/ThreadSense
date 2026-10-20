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
    replay = replay_generative_theme(**arguments, content=generative_capture_bytes(result.capture))
    assert replay == result


@pytest.mark.parametrize(
    "rows",
    [
        [],
        answers() + [answers()[0]],
        [{"comment_id": "extra", "theme": "usability", "confidence": 1}],
        [{"comment_id": "a", "theme": "usability", "confidence": True}, answers()[0]],
        [{"comment_id": "a", "theme": "usability", "confidence": 1.1}, answers()[0]],
        [42, answers()[0]],
    ],
)
def test_malformed_association_never_scores(arguments: dict[str, Any], rows: list[Any]) -> None:
    result = capture_generative_theme(**arguments, transport=transport(rows, []))
    assert result.availability["status"] == "unavailable"
    assert result.availability["reason"] == "inference_failed"
    assert result.capture is None and result.rows == ()


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
    "runtime",
    [
        RuntimeConfig(enabled=False),
        RuntimeConfig(base_url="http://localhost:11434"),
        RuntimeConfig(base_url="http://192.0.2.1:80"),
    ],
)
def test_no_request_guards_are_not_observations(runtime: RuntimeConfig) -> None:
    def forbidden(request: httpx.Request) -> httpx.Response:
        pytest.fail("guard made a request")

    observed = observe_generative_availability(runtime, transport=httpx.MockTransport(forbidden))
    assert observed["status"] == "not_observed"
    assert observed["method"] is None


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
