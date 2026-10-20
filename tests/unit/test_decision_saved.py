"""Strict loading and offline replay of saved typed decision documents.

Every saved document here is produced by the real ``annotate_canonical_thread`` with a real
``TypedDecisionClient`` and ``CheckpointVerifier``; only the HTTP transport serves the exchange
recorded from the pinned service. These are engineering tests of artifact integrity and
replay, never real-model evidence.
"""

from __future__ import annotations

import copy
import hashlib
import json
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from typed_decision_support import (
    CANONICAL_FIXTURE,
    ENDPOINT,
    HEALTH_PAYLOAD,
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
from threadsense.decisions.artifact import decision_bytes
from threadsense.decisions.client import TypedDecisionClient
from threadsense.decisions.contracts import (
    PINNED_CONTEXT_POLICY,
    PINNED_INFERENCE_SETTINGS,
    AggregateStatus,
    ItemStatus,
    QuestionSet,
    QuestionType,
    Requirement,
    RuntimeManifest,
)
from threadsense.decisions.evaluation_contracts import IdentityBindings, SavedDecision
from threadsense.decisions.questions import build_question_set
from threadsense.decisions.runtime import CheckpointVerifier
from threadsense.decisions.saved import load_saved_decision, replay_saved_decision
from threadsense.domains.loader import load_domain_vocabulary
from threadsense.errors import DecisionEvaluationError

OBJECTIVE = ObjectiveType.FRICTION_ANALYSIS
DOMAIN = "developer_tools"
COMMENT_COUNT = 7

# Builds the batch handler from the test root and the written checkpoint directory.
BatchFactory = Callable[[Path, Path], Handler]
PayloadEdit = Callable[[Path, dict[str, Any]], None]


@dataclass(frozen=True)
class Published:
    outcome: AnnotationOutcome
    manifest: RuntimeManifest
    document: dict[str, Any]


def _health_ok(_request: httpx.Request) -> httpx.Response:
    return json_response(HEALTH_PAYLOAD)


def _refuse_connection(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("connection refused", request=request)


def _question_set(objective: ObjectiveType = OBJECTIVE) -> QuestionSet:
    return build_question_set(objective, load_domain_vocabulary(DOMAIN))


def _edited_batch(edit: PayloadEdit) -> BatchFactory:
    """Serve the recorded batch response after ``edit`` changes its payload in place."""

    def factory(root: Path, checkpoint: Path) -> Handler:
        _request_text, payload = recorded_batch(checkpoint)
        edit(root, payload)

        def serve_batch(_request: httpx.Request) -> httpx.Response:
            return json_response(payload)

        return serve_batch

    return factory


def _unchanged(_root: Path, _payload: dict[str, Any]) -> None:
    return None


RECORDED_BATCH = _edited_batch(_unchanged)


def _publish(root: Path, *, health: Handler, batch: BatchFactory = RECORDED_BATCH) -> Published:
    input_path = root / "input" / "canonical.json"
    input_path.parent.mkdir()
    shutil.copyfile(CANONICAL_FIXTURE, input_path)
    checkpoint = write_checkpoint(root / "checkpoint")
    manifest = build_manifest()
    job = AnnotationJob(
        input_path=input_path,
        output_root=root / "out",
        protected_roots=(),
        requirement=Requirement.REQUIRED,
        objective=OBJECTIVE,
        domain=DOMAIN,
    )
    with TypedDecisionClient(
        ENDPOINT,
        wire=WIRE,
        timeout_seconds=30,
        transport=routed_transport(health, batch(root, checkpoint)),
    ) as client:
        outcome = annotate_canonical_thread(
            job,
            client=client,
            manifest=manifest,
            verifier=CheckpointVerifier(manifest),
            run_id="run-1",
        )
    document: dict[str, Any] = json.loads(outcome.decision_path.read_bytes())
    return Published(outcome=outcome, manifest=manifest, document=document)


@pytest.fixture
def published(tmp_path: Path) -> Published:
    return _publish(tmp_path, health=_health_ok)


def _republish(published: Published, document: dict[str, Any]) -> Path:
    """Write an edited document canonically under the sha256 of its new bytes."""
    content = decision_bytes(document)
    path = published.outcome.decision_path.parent / f"{hashlib.sha256(content).hexdigest()}.json"
    path.write_bytes(content)
    return path


def _edited(published: Published) -> dict[str, Any]:
    edited: dict[str, Any] = copy.deepcopy(published.document)
    return edited


def _load_error(path: Path) -> dict[str, Any]:
    with pytest.raises(DecisionEvaluationError) as caught:
        load_saved_decision(path)
    return caught.value.details


def _replay_error(saved: SavedDecision, question_set: QuestionSet) -> dict[str, Any]:
    with pytest.raises(DecisionEvaluationError) as caught:
        replay_saved_decision(saved, question_set, PINNED_INFERENCE_SETTINGS)
    return caught.value.details


# --- Loading --------------------------------------------------------------------------------


def test_load_saved_decision_published_document_exposes_bindings(published: Published) -> None:
    saved = load_saved_decision(published.outcome.decision_path)

    assert saved.decision_sha256 == published.outcome.decision_sha256
    assert saved.snapshot_sha256 == published.outcome.snapshot_sha256
    assert saved.bindings() == IdentityBindings(
        runtime_manifest_sha256=published.manifest.sha256(),
        checkpoint_revision="7b928d828b7b0e022f929d9bd2e44165aa270148",
        question_set_sha256=_question_set().sha256(),
        settings_sha256=PINNED_INFERENCE_SETTINGS.sha256(),
        context_policy_sha256=PINNED_CONTEXT_POLICY.sha256(),
        language="en",
    )
    assert saved.checkpoint_verified is True
    assert (saved.objective, saved.domain) == (OBJECTIVE.value, DOMAIN)
    assert saved.route_model == PINNED_INFERENCE_SETTINGS.model
    assert saved.requirement == Requirement.REQUIRED.value


def test_load_saved_decision_published_document_exposes_coverage(published: Published) -> None:
    saved = load_saved_decision(published.outcome.decision_path)

    assert saved.coverage == published.outcome.coverage
    assert saved.coverage.status is AggregateStatus.COMPLETE
    assert saved.comment_count == COMMENT_COUNT
    assert [item.index for item in saved.items] == list(range(COMMENT_COUNT))
    assert [item.comment_id for item in saved.items] == [
        item["comment_id"] for item in published.document["items"]
    ]
    assert {item.status for item in saved.items} == {ItemStatus.OK}


def test_load_saved_decision_published_document_exposes_answers(published: Published) -> None:
    saved = load_saved_decision(published.outcome.decision_path)
    question_ids = list(_question_set().question_ids())

    for item, raw_item in zip(saved.items, published.document["items"], strict=True):
        assert [answer.question_id for answer in item.answers] == question_ids
        for answer, raw_answer in zip(item.answers, raw_item["answers"], strict=True):
            assert answer.raw == raw_answer
            assert answer.question_type is QuestionType(raw_answer["type"])
            confidence = [
                quantity["value"]
                for quantity in raw_answer["confidence_quantities"]
                if quantity["name"] == "answer_confidence"
            ]
            assert [answer.answer_confidence] == confidence
            assert answer.probabilities == tuple(
                (label, value) for label, value in raw_answer["probabilities"]
            )
    relevance = saved.items[0].answer("objective_relevance")
    assert relevance is not None and relevance.score == relevance.raw["score"]
    assert relevance.choice is None and relevance.noul is None


def test_load_saved_decision_tampered_bytes_reports_digest_mismatch(
    published: Published,
) -> None:
    path = published.outcome.decision_path
    path.write_bytes(path.read_bytes() + b" ")

    details = _load_error(path)

    assert details["reason"] == "annotation_digest_mismatch"
    assert details["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_load_saved_decision_noncanonical_whitespace_reports_not_canonical(
    published: Published,
) -> None:
    compact = json.dumps(published.document, ensure_ascii=False).encode("utf-8")
    path = published.outcome.decision_path.parent / f"{hashlib.sha256(compact).hexdigest()}.json"
    path.write_bytes(compact)

    details = _load_error(path)

    assert details["reason"] == "annotation_not_canonical"


def test_load_saved_decision_inconsistent_coverage_reports_coverage_mismatch(
    published: Published,
) -> None:
    document = _edited(published)
    document["coverage"]["ok"] = COMMENT_COUNT - 1

    details = _load_error(_republish(published, document))

    assert details["reason"] == "annotation_coverage_mismatch"
    assert details["saved"]["ok"] == COMMENT_COUNT - 1
    assert details["derived"]["ok"] == COMMENT_COUNT


def test_load_saved_decision_detected_route_language_reports_invalid(
    published: Published,
) -> None:
    document = _edited(published)
    document["route"]["detection"] = {"language": "en", "confidence": 0.99}

    details = _load_error(_republish(published, document))

    assert details == {"reason": "annotation_invalid", "field": "route.detection"}


def test_load_saved_decision_unsupported_schema_version_reports_version(
    published: Published,
) -> None:
    document = _edited(published)
    document["schema_version"] = 2

    details = _load_error(_republish(published, document))

    assert details == {"reason": "annotation_version_unsupported", "schema_version": 2}


def test_load_saved_decision_unavailable_health_reports_unverified_checkpoint(
    tmp_path: Path,
) -> None:
    unavailable = _publish(tmp_path, health=_refuse_connection)

    saved = load_saved_decision(unavailable.outcome.decision_path)

    assert saved.checkpoint_verified is False
    assert saved.coverage.status is AggregateStatus.UNAVAILABLE
    assert {item.status for item in saved.items} == {ItemStatus.UNAVAILABLE}


# --- Replay ---------------------------------------------------------------------------------


def test_replay_saved_decision_recorded_exchange_reproduces_every_item(
    published: Published,
) -> None:
    saved = load_saved_decision(published.outcome.decision_path)

    report = replay_saved_decision(saved, _question_set(), PINNED_INFERENCE_SETTINGS)

    assert (report.replayed_items, report.matched_items) == (COMMENT_COUNT, COMMENT_COUNT)
    assert report.not_replayable == ()
    assert report.to_dict() == {
        "decision_sha256": published.outcome.decision_sha256,
        "status": "reproduced",
        "replayed_items": COMMENT_COUNT,
        "matched_items": COMMENT_COUNT,
        "not_replayable": [],
    }


def test_replay_saved_decision_edited_answer_probability_reports_mismatch(
    published: Published,
) -> None:
    document = _edited(published)
    target = document["items"][2]
    pair = target["answers"][1]["probabilities"][0]
    pair[1] = pair[1] / 2 + 0.001
    saved = load_saved_decision(_republish(published, document))

    details = _replay_error(saved, _question_set())

    assert details == {
        "reason": "replay_mismatch",
        "comment_id": target["comment_id"],
        "field": "answers",
    }


def test_replay_saved_decision_edited_response_body_reports_digest_mismatch(
    published: Published,
) -> None:
    document = _edited(published)
    exchange = document["exchanges"][0]
    exchange["response_body"] = exchange["response_body"] + " "
    saved = load_saved_decision(_republish(published, document))

    details = _replay_error(saved, _question_set())

    assert details == {
        "reason": "replay_exchange_digest_mismatch",
        "batch_index": exchange["batch_index"],
        "field": "response_body",
    }


def test_replay_saved_decision_other_checkpoint_repo_reports_mismatch(
    published: Published,
) -> None:
    document = _edited(published)
    document["backend"]["checkpoint_verification"]["repo"] = "/elsewhere/checkpoint"
    saved = load_saved_decision(_republish(published, document))

    details = _replay_error(saved, _question_set())

    assert details == {
        "reason": "replay_mismatch",
        "comment_id": document["items"][0]["comment_id"],
        "field": "status",
    }


def test_replay_saved_decision_unavailable_health_reports_not_replayable(
    tmp_path: Path,
) -> None:
    unavailable = _publish(tmp_path, health=_refuse_connection)
    saved = load_saved_decision(unavailable.outcome.decision_path)

    report = replay_saved_decision(saved, _question_set(), PINNED_INFERENCE_SETTINGS)

    assert unavailable.document["exchanges"] == []
    assert (report.replayed_items, report.matched_items) == (0, 0)
    assert report.not_replayable == tuple(
        (item["comment_id"], "runtime_health_unavailable") for item in unavailable.document["items"]
    )
    assert len(report.not_replayable) == COMMENT_COUNT


def test_replay_saved_decision_other_objective_reports_identity_mismatch(
    published: Published,
) -> None:
    saved = load_saved_decision(published.outcome.decision_path)
    other = _question_set(ObjectiveType.FEATURE_DEMAND)

    details = _replay_error(saved, other)

    assert details == {
        "reason": "replay_identity_mismatch",
        "field": "question_set_sha256",
        "saved": _question_set().sha256(),
        "current": other.sha256(),
    }


def test_load_saved_decision_edited_context_policy_value_reports_stale_digest(
    published: Published,
) -> None:
    document = _edited(published)
    document["context_policy"]["body_window_chars"] += 1

    details = _load_error(_republish(published, document))

    assert details == {"reason": "annotation_invalid", "field": "context_policy.sha256"}


def test_replay_saved_decision_edited_questions_object_reports_identity_mismatch(
    published: Published,
) -> None:
    document = _edited(published)
    document["questions"]["questions"].reverse()
    saved = load_saved_decision(_republish(published, document))

    details = _replay_error(saved, _question_set())

    assert details == {"reason": "replay_identity_mismatch", "field": "questions"}


def test_replay_saved_decision_unknown_exchange_outcome_reports_invalid(
    published: Published,
) -> None:
    document = _edited(published)
    document["exchanges"][0]["outcome"] = "bogus"
    saved = load_saved_decision(_republish(published, document))

    details = _replay_error(saved, _question_set())

    assert details == {"reason": "annotation_invalid", "field": "exchanges[0].outcome"}


# --- Replay over non-ok annotator output ----------------------------------------------------


def _truncate_first_result(_root: Path, payload: dict[str, Any]) -> None:
    payload["results"][0]["usage"]["truncated"] = True


def _break_first_choice(_root: Path, payload: dict[str, Any]) -> None:
    # The recorded choice is the most probable option; any other option is not maximal.
    payload["results"][0]["answers"]["evidence_type"]["choice"] = "question"


def _drop_last_result(_root: Path, payload: dict[str, Any]) -> None:
    payload["results"].pop()


def _second_checkpoint_repo(root: Path, payload: dict[str, Any]) -> None:
    payload["results"][1]["routing"]["repo"] = str(write_checkpoint(root / "checkpoint-b"))


def _missing_checkpoint_repo(root: Path, payload: dict[str, Any]) -> None:
    payload["results"][1]["routing"]["repo"] = str(root / "missing-checkpoint")


def _server_error(_root: Path, _checkpoint: Path) -> Handler:
    def serve_batch(_request: httpx.Request) -> httpx.Response:
        return json_response({"detail": "internal error"}, status_code=500)

    return serve_batch


def _replay(published: Published) -> tuple[SavedDecision, int, int, tuple[tuple[str, str], ...]]:
    saved = load_saved_decision(published.outcome.decision_path)
    report = replay_saved_decision(saved, _question_set(), PINNED_INFERENCE_SETTINGS)
    return saved, report.replayed_items, report.matched_items, report.not_replayable


def test_replay_saved_decision_server_truncation_reproduces_refusal(tmp_path: Path) -> None:
    refused = _publish(tmp_path, health=_health_ok, batch=_edited_batch(_truncate_first_result))

    saved, replayed, matched, not_replayable = _replay(refused)

    assert saved.items[0].status is ItemStatus.REFUSED
    assert saved.items[0].reasons == ("server_reported_truncation",)
    assert {item.status for item in saved.items[1:]} == {ItemStatus.OK}
    assert (replayed, matched, not_replayable) == (COMMENT_COUNT, COMMENT_COUNT, ())


def test_replay_saved_decision_invalid_typed_answer_reproduces_unavailable(
    tmp_path: Path,
) -> None:
    broken = _publish(tmp_path, health=_health_ok, batch=_edited_batch(_break_first_choice))

    saved, replayed, matched, not_replayable = _replay(broken)

    assert saved.items[0].status is ItemStatus.UNAVAILABLE
    assert saved.items[0].reasons == ("invalid_typed_answer",)
    assert broken.document["items"][0]["failure"]["reason"] == "choice_not_maximal"
    assert (replayed, matched, not_replayable) == (COMMENT_COUNT, COMMENT_COUNT, ())


def _other_reasons(item: dict[str, Any]) -> None:
    item["reasons"] = ["server_dropped_state_tokens"]


def _other_failure_message(item: dict[str, Any]) -> None:
    item["failure"]["message"] = item["failure"]["message"] + " (edited)"


@pytest.mark.parametrize(
    ("edit", "edit_item", "field"),
    [
        (_truncate_first_result, _other_reasons, "reasons"),
        (_break_first_choice, _other_failure_message, "failure"),
    ],
)
def test_replay_saved_decision_edited_non_ok_item_reports_mismatch(
    tmp_path: Path,
    edit: PayloadEdit,
    edit_item: Callable[[dict[str, Any]], None],
    field: str,
) -> None:
    source = _publish(tmp_path, health=_health_ok, batch=_edited_batch(edit))
    document = _edited(source)
    target = document["items"][0]
    edit_item(target)
    saved = load_saved_decision(_republish(source, document))

    details = _replay_error(saved, _question_set())

    assert details == {
        "reason": "replay_mismatch",
        "comment_id": target["comment_id"],
        "field": field,
    }


def test_replay_saved_decision_wrong_result_count_lists_not_replayable(tmp_path: Path) -> None:
    short = _publish(tmp_path, health=_health_ok, batch=_edited_batch(_drop_last_result))

    saved, replayed, matched, not_replayable = _replay(short)

    assert short.document["exchanges"][0]["outcome"] == "response"
    assert {item.reasons for item in saved.items} == {("invalid_response",)}
    assert {item["failure"]["reason"] for item in short.document["items"]} == {
        "result_count_mismatch"
    }
    assert (replayed, matched) == (0, 0)
    assert not_replayable == tuple((item.comment_id, "invalid_response") for item in saved.items)


@pytest.mark.parametrize(
    ("edit", "reason"),
    [
        (_second_checkpoint_repo, "checkpoint_repo_changed"),
        (_missing_checkpoint_repo, "checkpoint_identity_unverified"),
    ],
)
def test_replay_saved_decision_other_result_repo_reproduces_checkpoint_refusal(
    tmp_path: Path, edit: PayloadEdit, reason: str
) -> None:
    moved = _publish(tmp_path, health=_health_ok, batch=_edited_batch(edit))

    saved, replayed, matched, not_replayable = _replay(moved)

    assert saved.items[1].status is ItemStatus.UNAVAILABLE
    assert saved.items[1].reasons == (reason,)
    assert {saved.items[index].status for index in (0, *range(2, COMMENT_COUNT))} == {ItemStatus.OK}
    assert (replayed, matched, not_replayable) == (COMMENT_COUNT, COMMENT_COUNT, ())


def test_replay_saved_decision_http_500_batch_lists_not_replayable(tmp_path: Path) -> None:
    failed = _publish(tmp_path, health=_health_ok, batch=_server_error)

    saved, replayed, matched, not_replayable = _replay(failed)

    exchange = failed.document["exchanges"][0]
    assert (exchange["outcome"], exchange["status_code"], exchange["response_body"]) == (
        "error",
        None,
        None,
    )
    assert exchange["error"]["code"] == "decision_runtime_unavailable"
    assert {item["failure"]["reason"] for item in failed.document["items"]} == {"http_status"}
    assert (replayed, matched) == (0, 0)
    assert not_replayable == tuple(
        (item.comment_id, "runtime_request_failed") for item in saved.items
    )


def test_replay_saved_decision_http_500_edited_failure_reports_mismatch(tmp_path: Path) -> None:
    failed = _publish(tmp_path, health=_health_ok, batch=_server_error)
    document = _edited(failed)
    target = document["items"][3]
    target["failure"]["details"]["status_code"] = 502
    saved = load_saved_decision(_republish(failed, document))

    details = _replay_error(saved, _question_set())

    assert details == {
        "reason": "replay_mismatch",
        "comment_id": target["comment_id"],
        "field": "failure",
    }
