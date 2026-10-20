from __future__ import annotations

import itertools
import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from threadsense.action_signals import classify_finding_signal
from threadsense.errors import MonitoringInputError, MonitoringStateError
from threadsense.monitoring.feedback_records import finding_scope_key
from threadsense.monitoring.ranking import (
    DEFAULT_RANKING_POLICY,
    FindingFacts,
    RankingBasis,
    RankingInputs,
    RankingPolicy,
    load_ranking_policy_file,
    parse_ranking_policy,
    rank_entries,
    stored_ranking_policy,
)

COMPONENT_ORDER = ["severity", "objective", "novelty", "evidence", "preference", "semantic"]
THREAD_ID = "t1"
ISSUE_ID = "iss-" + "c" * 32
MISSING = object()


def entry_id(number: int) -> str:
    return f"ent-{number:032x}"


def policy_payload(path: tuple[str, ...] = (), value: object = MISSING) -> dict[str, Any]:
    """The default policy document, with the field at `path` set to `value` or removed."""
    payload: dict[str, Any] = json.loads(json.dumps(DEFAULT_RANKING_POLICY.to_dict()))
    if not path:
        return payload
    table = payload
    for key in path[:-1]:
        table = table[key]
    if value is MISSING:
        del table[path[-1]]
    else:
        table[path[-1]] = value
    return payload


def zeroed(table: dict[str, Any]) -> dict[str, Any]:
    return {key: zeroed(value) if isinstance(value, dict) else 0 for key, value in table.items()}


def zero_policy() -> RankingPolicy:
    """A valid policy that awards no points, so every order is the baseline order."""
    payload = zeroed(policy_payload())
    payload["policy_version"] = 1
    payload["name"] = "zero"
    return parse_ranking_policy(payload)


def finding(
    number: int,
    *,
    severity: str = "high",
    theme: str = "latency",
    novel: Sequence[str] = ("initial_observation",),
) -> dict[str, Any]:
    return {
        "kind": "finding",
        "entry_id": entry_id(number),
        "source_thread_id": THREAD_ID,
        "theme_key": theme,
        "severity": severity,
        "novel_evidence": [{"kind": kind} for kind in novel],
    }


def thread_update(
    number: int, *, thread: str = THREAD_ID, novel: Sequence[str] = ("initial_observation",)
) -> dict[str, Any]:
    return {
        "kind": "thread_update",
        "entry_id": entry_id(number),
        "source_thread_id": thread,
        "novel_comments": [{"kind": kind} for kind in novel],
    }


def preference(sequence: int, scope_kind: str, scope_key: str, value: str) -> dict[str, Any]:
    return {
        "feedback_id": f"fbk-{sequence}",
        "sequence": sequence,
        "scope_kind": scope_kind,
        "scope_key": scope_key,
        "value": value,
        "target_kind": "entry",
        "target_id": entry_id(sequence),
    }


def inputs_of(
    *,
    policy: RankingPolicy = DEFAULT_RANKING_POLICY,
    objective: str = "friction_analysis",
    preferences: tuple[dict[str, Any], ...] = (),
    sequence: int | None = None,
) -> RankingInputs:
    basis = RankingBasis(
        profile_revision=1,
        policy_id=policy.policy_id,
        preference_sequence=len(preferences) if sequence is None else sequence,
    )
    return RankingInputs(basis=basis, policy=policy, objective=objective, preferences=preferences)


def facts(issue: int = 0, request: int = 0, comments: int = 1) -> FindingFacts:
    return FindingFacts(
        issue_marker_count=issue, request_marker_count=request, comment_count=comments
    )


def component(item: dict[str, Any], name: str) -> dict[str, Any]:
    matches: list[dict[str, Any]] = [
        part for part in item["components"] if part["component"] == name
    ]
    [found] = matches
    return found


def only_item(
    entry: dict[str, Any],
    inputs: RankingInputs,
    finding_facts: FindingFacts | None = None,
    issue_ref: str | None = None,
) -> dict[str, Any]:
    known = {} if finding_facts is None else {str(entry["entry_id"]): finding_facts}
    refs = {} if issue_ref is None else {str(entry["entry_id"]): issue_ref}
    order: list[dict[str, Any]] = rank_entries([entry], inputs, known, refs)["order"]
    [item] = order
    return item


def test_parse_ranking_policy_default_document_round_trips_to_equal_policy() -> None:
    parsed = parse_ranking_policy(DEFAULT_RANKING_POLICY.to_dict())

    assert parsed == DEFAULT_RANKING_POLICY
    assert parsed.policy_id == DEFAULT_RANKING_POLICY.policy_id
    assert parsed.canonical_json() == DEFAULT_RANKING_POLICY.canonical_json()


@pytest.mark.parametrize(
    "path",
    [
        ("severity", "high"),
        ("objective", "friction_analysis", "issue"),
        ("objective", "general_survey", "discussion"),
        ("novelty", "edited"),
        ("evidence", "points_per_novel_comment"),
        ("evidence", "max_counted_comments"),
        ("preference", "useful"),
        ("preference", "irrelevant"),
        ("preference", "bound"),
    ],
)
def test_ranking_policy_id_any_weight_change_yields_new_id(path: tuple[str, ...]) -> None:
    original = policy_payload()
    table = original
    for key in path[:-1]:
        table = table[key]

    changed = parse_ranking_policy(policy_payload(path, int(table[path[-1]]) + 1))

    assert re.fullmatch(r"pol-[0-9a-f]{32}", DEFAULT_RANKING_POLICY.policy_id)
    assert re.fullmatch(r"pol-[0-9a-f]{32}", changed.policy_id)
    assert changed.policy_id != DEFAULT_RANKING_POLICY.policy_id


@pytest.mark.parametrize(
    ("path", "value", "field"),
    [
        pytest.param(("semantic",), {"embedding": 10}, "document", id="unknown-top-level"),
        pytest.param(("novelty",), MISSING, "novelty", id="missing-table"),
        pytest.param(("objective", "general_survey"), MISSING, "objective", id="missing-objective"),
        pytest.param(
            ("objective", "friction_analysis", "semantic"),
            5,
            "objective.friction_analysis",
            id="extra-signal-key",
        ),
        pytest.param(("severity", "high"), True, "severity.high", id="bool-weight"),
        pytest.param(("severity", "high"), 30.0, "severity.high", id="float-weight"),
        pytest.param(("severity", "high"), 101, "severity.high", id="severity-above-100"),
        pytest.param(("severity", "low"), -1, "severity.low", id="severity-negative"),
        pytest.param(
            ("objective", "feature_demand", "request"),
            -101,
            "objective.feature_demand.request",
            id="objective-below-minus-100",
        ),
        pytest.param(("novelty", "edited"), -1, "novelty.edited", id="novelty-negative"),
        pytest.param(("preference", "useful"), -1, "preference.useful", id="useful-negative"),
        pytest.param(
            ("preference", "irrelevant"), 1, "preference.irrelevant", id="irrelevant-positive"
        ),
        pytest.param(("preference", "bound"), 101, "preference.bound", id="bound-above-100"),
        pytest.param(
            ("evidence", "points_per_novel_comment"),
            21,
            "evidence.points_per_novel_comment",
            id="per-comment-above-20",
        ),
        pytest.param(
            ("evidence", "max_counted_comments"),
            51,
            "evidence.max_counted_comments",
            id="comment-cap-above-50",
        ),
        pytest.param(("policy_version",), 2, "policy_version", id="wrong-version"),
        pytest.param(("policy_version",), True, "policy_version", id="bool-version"),
        pytest.param(("name",), "Objective_Default", "name", id="name-not-slug"),
        pytest.param(("name",), "-leading", "name", id="name-leading-hyphen"),
        pytest.param(("name",), "", "name", id="name-empty"),
    ],
)
def test_parse_ranking_policy_invalid_document_raises_invalid_ranking_policy(
    path: tuple[str, ...], value: object, field: str
) -> None:
    payload = policy_payload(path, value)

    with pytest.raises(MonitoringInputError) as raised:
        parse_ranking_policy(payload)

    assert raised.value.details["reason"] == "invalid_ranking_policy"
    assert raised.value.details["field"] == field


def test_stored_ranking_policy_matching_canonical_json_returns_policy() -> None:
    policy = stored_ranking_policy(
        DEFAULT_RANKING_POLICY.policy_id, DEFAULT_RANKING_POLICY.canonical_json()
    )

    assert policy == DEFAULT_RANKING_POLICY


@pytest.mark.parametrize(
    "policy_json",
    [
        pytest.param(
            parse_ranking_policy(policy_payload(("severity", "high"), 31)).canonical_json(),
            id="other-policy",
        ),
        pytest.param(
            json.dumps(DEFAULT_RANKING_POLICY.to_dict(), indent=2), id="non-canonical-bytes"
        ),
        pytest.param("{", id="not-json"),
        pytest.param("[]", id="not-an-object"),
        pytest.param("{}", id="missing-required-fields"),
        pytest.param(
            json.dumps(policy_payload(("severity", "high"), 101)), id="out-of-range-weight"
        ),
    ],
)
def test_stored_ranking_policy_hash_mismatch_raises_ranking_policy_unreadable(
    policy_json: str,
) -> None:
    with pytest.raises(MonitoringStateError) as raised:
        stored_ranking_policy(DEFAULT_RANKING_POLICY.policy_id, policy_json)

    assert raised.value.details["reason"] == "ranking_policy_unreadable"
    assert raised.value.details["policy_id"] == DEFAULT_RANKING_POLICY.policy_id


def test_rank_entries_score_is_sum_of_ordered_components_and_semantic_not_eligible() -> None:
    entries = [
        finding(1, novel=("edited", "initial_observation")),
        thread_update(2, novel=("new_source_event",)),
    ]

    ranking = rank_entries(entries, inputs_of(), {entry_id(1): facts(issue=2)}, {})

    assert ranking["semantic"]["status"] == "not_eligible"
    assert ranking["corrections"] == "not_a_ranking_input"
    assert [item["rank"] for item in ranking["order"]] == [1, 2]
    for item in ranking["order"]:
        names = [part["component"] for part in item["components"]]
        semantic = component(item, "semantic")
        assert names == COMPONENT_ORDER
        assert item["score"] == sum(
            int(part["points"]) for part in item["components"] if part["points"] is not None
        )
        assert (semantic["points"], semantic["status"]) == (None, "not_eligible")


def test_rank_entries_basis_reports_recorded_inputs() -> None:
    inputs = inputs_of(objective="feature_demand")

    ranking = rank_entries([thread_update(1)], inputs, {}, {})

    assert ranking["basis"] == {
        "profile_revision": 1,
        "policy_id": DEFAULT_RANKING_POLICY.policy_id,
        "preference_sequence": 0,
        "objective": "feature_demand",
        "policy_name": DEFAULT_RANKING_POLICY.name,
    }


@pytest.mark.parametrize(
    ("objective", "severity", "counts", "signal_type"),
    [
        ("friction_analysis", "high", (2, 0, 3), "issue"),
        ("feature_demand", "medium", (0, 2, 2), "request"),
        ("competitive_intelligence", "high", (1, 1, 2), "mixed"),
        ("sentiment_mapping", "low", (0, 0, 4), "discussion"),
    ],
)
def test_rank_entries_objective_component_weights_finding_signal_type(
    objective: str, severity: str, counts: tuple[int, int, int], signal_type: str
) -> None:
    issue, request, comments = counts
    signal = classify_finding_signal("latency", severity, comments, issue, request)

    item = only_item(
        finding(1, severity=severity),
        inputs_of(objective=objective),
        facts(issue=issue, request=request, comments=comments),
    )

    part = component(item, "objective")
    assert signal.signal_type == signal_type
    assert part["points"] == DEFAULT_RANKING_POLICY.objective_points[objective][signal_type]
    assert (part["objective"], part["signal_type"]) == (objective, signal_type)
    assert (part["action_type"], part["recommended_owner"]) == (
        signal.action_type,
        signal.recommended_owner,
    )


def test_rank_entries_friction_issue_finding_scores_issue_weight() -> None:
    item = only_item(finding(1, severity="high"), inputs_of(), facts(issue=2, comments=3))

    part = component(item, "objective")
    assert part["points"] == 20
    assert (part["signal_type"], part["action_type"], part["recommended_owner"]) == (
        "issue",
        "fix",
        "engineering",
    )
    assert component(item, "severity")["points"] == 30


def test_rank_entries_thread_update_scores_no_severity_or_objective() -> None:
    item = only_item(thread_update(1), inputs_of())

    assert component(item, "severity")["points"] == 0
    assert component(item, "objective")["points"] == 0


@pytest.mark.parametrize(
    ("comment_count", "counted", "points"), [(0, 0, 0), (3, 3, 6), (5, 5, 10), (8, 5, 10)]
)
def test_rank_entries_evidence_component_caps_counted_comments(
    comment_count: int, counted: int, points: int
) -> None:
    item = only_item(thread_update(1, novel=["edited"] * comment_count), inputs_of())

    part = component(item, "evidence")
    assert (part["points"], part["novel_comments"], part["counted_comments"]) == (
        points,
        comment_count,
        counted,
    )


@pytest.mark.parametrize(
    ("kinds", "expected"),
    [
        (("initial_observation", "edited", "new_source_event"), "new_source_event"),
        (("newly_observed_historical", "edited", "initial_observation"), "edited"),
        (("initial_observation", "newly_observed_historical"), "newly_observed_historical"),
        (("initial_observation",), "initial_observation"),
    ],
)
def test_rank_entries_novelty_component_uses_highest_weight_kind_present(
    kinds: tuple[str, ...], expected: str
) -> None:
    item = only_item(thread_update(1, novel=kinds), inputs_of())

    part = component(item, "novelty")
    assert (part["novelty_kind"], part["points"]) == (
        expected,
        DEFAULT_RANKING_POLICY.novelty_points[expected],
    )


def test_rank_entries_no_novel_comments_scores_no_novelty() -> None:
    item = only_item(thread_update(1, novel=()), inputs_of())

    part = component(item, "novelty")
    assert part["points"] == 0
    assert "novelty_kind" not in part


def test_rank_entries_preference_latest_event_per_scope_wins() -> None:
    preferences = (
        preference(1, "thread", THREAD_ID, "useful"),
        preference(2, "thread", THREAD_ID, "irrelevant"),
    )

    item = only_item(thread_update(1), inputs_of(preferences=preferences))

    part = component(item, "preference")
    assert [(m["feedback_id"], m["value"]) for m in part["matched"]] == [("fbk-2", "irrelevant")]
    assert part["points"] == -40


def test_rank_entries_preference_after_basis_sequence_is_ignored() -> None:
    preferences = (
        preference(1, "thread", THREAD_ID, "useful"),
        preference(2, "thread", THREAD_ID, "irrelevant"),
        preference(3, "entry", entry_id(1), "useful"),
    )

    item = only_item(thread_update(1), inputs_of(preferences=preferences, sequence=1))

    part = component(item, "preference")
    assert [match["feedback_id"] for match in part["matched"]] == ["fbk-1"]
    assert part["points"] == 40


def test_rank_entries_preference_matches_entry_finding_thread_and_issue_scopes() -> None:
    preferences = (
        preference(1, "entry", entry_id(1), "useful"),
        preference(2, "finding", finding_scope_key(THREAD_ID, "latency"), "already_known"),
        preference(3, "thread", THREAD_ID, "wrong_audience"),
        preference(4, "issue", ISSUE_ID, "useful"),
        preference(5, "thread", "t2", "irrelevant"),
        preference(6, "entry", entry_id(2), "irrelevant"),
        preference(7, "finding", finding_scope_key(THREAD_ID, "other"), "irrelevant"),
    )

    item = only_item(finding(1), inputs_of(preferences=preferences), facts(), ISSUE_ID)

    part = component(item, "preference")
    scopes = [(m["scope_kind"], m["scope_key"], m["points"]) for m in part["matched"]]
    assert scopes == [
        ("entry", entry_id(1), 40),
        ("finding", finding_scope_key(THREAD_ID, "latency"), -25),
        ("thread", THREAD_ID, -30),
        ("issue", ISSUE_ID, 40),
    ]
    assert (part["points"], part["unbounded_points"]) == (25, 25)


@pytest.mark.parametrize(("value", "bounded"), [("useful", 60), ("irrelevant", -60)])
def test_rank_entries_preference_sum_is_clamped_to_bound(value: str, bounded: int) -> None:
    preferences = (
        preference(1, "entry", entry_id(1), value),
        preference(2, "thread", THREAD_ID, value),
        preference(3, "issue", ISSUE_ID, value),
    )
    unbounded = 3 * DEFAULT_RANKING_POLICY.preference_points[value]

    item = only_item(thread_update(1), inputs_of(preferences=preferences), None, ISSUE_ID)

    part = component(item, "preference")
    assert (part["points"], part["unbounded_points"], part["bound"]) == (
        bounded,
        unbounded,
        DEFAULT_RANKING_POLICY.preference_bound,
    )


def test_rank_entries_input_order_permutations_yield_identical_ranking() -> None:
    entries = [
        finding(4, theme="beta"),
        finding(3, theme="alpha"),
        thread_update(2, thread="t2"),
        thread_update(1, thread="t3"),
    ]
    known = {entry_id(4): facts(issue=1), entry_id(3): facts(issue=1)}

    rankings = [
        rank_entries(list(order), inputs_of(), known, {})
        for order in itertools.permutations(entries)
    ]

    assert all(ranking == rankings[0] for ranking in rankings)
    assert [item["entry_id"] for item in rankings[0]["order"]] == [
        entry_id(3),
        entry_id(4),
        entry_id(1),
        entry_id(2),
    ]
    assert rankings[0]["order"][0]["score"] == rankings[0]["order"][1]["score"]


def test_rank_entries_equal_scores_follow_baseline_finding_order() -> None:
    entries = [
        thread_update(0),
        finding(1, severity="medium", theme="a"),
        finding(2, theme="b"),
        finding(3, theme="b"),
        finding(4, theme="a"),
        finding(5, theme="z"),
        finding(6, theme="z"),
        finding(7, theme="z"),
    ]
    known = {
        entry_id(1): facts(issue=9, request=0, comments=9),
        entry_id(2): facts(issue=1, request=1, comments=2),
        entry_id(3): facts(issue=1, request=1, comments=2),
        entry_id(4): facts(issue=1, request=1, comments=2),
        entry_id(5): facts(issue=1, request=1, comments=9),
        entry_id(6): facts(issue=1, request=3, comments=1),
        entry_id(7): facts(issue=2, request=0, comments=1),
    }

    ranking = rank_entries(entries, inputs_of(policy=zero_policy()), known, {})

    assert {item["score"] for item in ranking["order"]} == {0}
    assert [item["entry_id"] for item in ranking["order"]] == [
        entry_id(7),
        entry_id(6),
        entry_id(5),
        entry_id(4),
        entry_id(2),
        entry_id(3),
        entry_id(1),
        entry_id(0),
    ]


def test_rank_entries_unknown_objective_raises_ranking_input_unreadable() -> None:
    with pytest.raises(MonitoringStateError) as raised:
        rank_entries([thread_update(1)], inputs_of(objective="market_sizing"), {}, {})

    assert raised.value.details["reason"] == "ranking_input_unreadable"
    assert raised.value.details["objective"] == "market_sizing"


def test_rank_entries_finding_without_facts_raises_ranking_input_unreadable() -> None:
    with pytest.raises(MonitoringStateError) as raised:
        rank_entries([finding(1)], inputs_of(), {}, {})

    assert raised.value.details["reason"] == "ranking_input_unreadable"
    assert (raised.value.details["entry_id"], raised.value.details["field"]) == (
        entry_id(1),
        "analysis",
    )


@pytest.mark.parametrize(
    ("entry", "field"),
    [
        ({**thread_update(1), "source_thread_id": None}, "source_thread_id"),
        ({**finding(1), "theme_key": None}, "theme_key"),
    ],
)
def test_rank_entries_entry_missing_scope_key_raises_ranking_input_unreadable(
    entry: dict[str, Any], field: str
) -> None:
    del entry[field]

    with pytest.raises(MonitoringStateError) as raised:
        rank_entries([entry], inputs_of(), {entry_id(1): facts()}, {})

    assert raised.value.details["reason"] == "ranking_input_unreadable"
    assert (raised.value.details["entry_id"], raised.value.details["field"]) == (
        entry_id(1),
        field,
    )


@pytest.mark.parametrize("content", [None, b"\xff\xfe not utf-8"])
def test_load_ranking_policy_file_unreadable_path_raises_policy_file_unreadable(
    tmp_path: Path, content: bytes | None
) -> None:
    path = tmp_path / "policy.toml"
    if content is None:
        path.mkdir()
    else:
        path.write_bytes(content)

    with pytest.raises(MonitoringInputError) as raised:
        load_ranking_policy_file(path)

    assert raised.value.details["reason"] == "policy_file_unreadable"
    assert raised.value.details["path"] == str(path)


def test_rank_entries_unknown_novelty_kind_raises_ranking_input_unreadable() -> None:
    entry = thread_update(1, novel=("initial_observation", "rumored"))

    with pytest.raises(MonitoringStateError) as raised:
        rank_entries([entry], inputs_of(), {}, {})

    assert raised.value.details["reason"] == "ranking_input_unreadable"
    assert raised.value.details["kinds"] == ["rumored"]
