"""Versioned, bounded presentation ranking of digest entries with a reason per component.

Ranking orders the entries one digest presents; it never selects, hides, rewrites or
counts evidence. The baseline is the existing deterministic finding order (severity, issue
markers, request markers, comment count, theme) plus the entry id, so ties never depend on
database iteration. An explicit, immutable policy adds bounded points for severity, the
profile objective's weight on the finding's existing action signal, novelty, a capped
evidence volume and the operator's effective preferences. Corrections are not an input.
No semantic component is scored: without an eligible semantic policy it is disclosed as
`not_eligible` and contributes nothing.
"""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from threadsense.action_signals import classify_finding_signal
from threadsense.contracts import ObjectiveType
from threadsense.errors import MonitoringInputError, MonitoringStateError
from threadsense.monitoring.feedback_records import PREFERENCE_VALUES, effective_preferences

RANKING_POLICY_VERSION = 1
RANKING_VERSION = 1
POLICY_ID_PATTERN = re.compile(r"^pol-[0-9a-f]{32}$")
POLICY_NAME_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?")
SEVERITIES = ("high", "medium", "low")
SIGNAL_TYPES = ("issue", "request", "mixed", "discussion")
NOVELTY_KINDS = ("new_source_event", "edited", "newly_observed_historical", "initial_observation")
OBJECTIVES = tuple(objective.value for objective in ObjectiveType)
SEMANTIC_COMPONENT = {
    "component": "semantic",
    "points": None,
    "status": "not_eligible",
    "reason": (
        "no eligible semantic ranking policy exists; typed annotations stay shadow-only and "
        "contribute no points"
    ),
}
_SEVERITY_RANK = {"high": 3, "medium": 2, "low": 1}
_KIND_RANK = {"finding": 0, "thread_update": 1}
_REQUIRED_ENTRY_KEYS = {
    "finding": ("entry_id", "source_thread_id", "theme_key"),
    "thread_update": ("entry_id", "source_thread_id"),
}
_POINT_RANGE = (-100, 100)
_POSITIVE_RANGE = (0, 100)
_NEGATIVE_RANGE = (-100, 0)
_PER_COMMENT_RANGE = (0, 20)
_COMMENT_CAP_RANGE = (0, 50)
_TOP_LEVEL = frozenset(
    {"policy_version", "name", "severity", "objective", "novelty", "evidence", "preference"}
)


@dataclass(frozen=True)
class RankingPolicy:
    """Explicit integer weights; every field is required and bounded."""

    name: str
    severity_points: Mapping[str, int]
    objective_points: Mapping[str, Mapping[str, int]]
    novelty_points: Mapping[str, int]
    points_per_novel_comment: int
    max_counted_comments: int
    preference_points: Mapping[str, int]
    preference_bound: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_version": RANKING_POLICY_VERSION,
            "name": self.name,
            "severity": dict(self.severity_points),
            "objective": {
                objective: dict(points) for objective, points in self.objective_points.items()
            },
            "novelty": dict(self.novelty_points),
            "evidence": {
                "points_per_novel_comment": self.points_per_novel_comment,
                "max_counted_comments": self.max_counted_comments,
            },
            "preference": {**self.preference_points, "bound": self.preference_bound},
        }

    def canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @property
    def policy_id(self) -> str:
        digest = hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
        return "pol-" + digest[:32]


@dataclass(frozen=True)
class RankingBasis:
    """The exact inputs a digest's order was materialized from, recorded with the digest."""

    profile_revision: int
    policy_id: str
    preference_sequence: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_revision": self.profile_revision,
            "policy_id": self.policy_id,
            "preference_sequence": self.preference_sequence,
        }


@dataclass(frozen=True)
class FindingFacts:
    """Existing analysis facts of one finding entry, read from its hash-verified artifact."""

    issue_marker_count: int
    request_marker_count: int
    comment_count: int


@dataclass(frozen=True)
class RankingInputs:
    basis: RankingBasis
    policy: RankingPolicy
    objective: str
    preferences: tuple[dict[str, Any], ...]


DEFAULT_RANKING_POLICY = RankingPolicy(
    name="objective-default",
    severity_points={"high": 30, "medium": 20, "low": 10},
    objective_points={
        "friction_analysis": {"issue": 20, "mixed": 15, "request": 5, "discussion": 0},
        "feature_demand": {"issue": 5, "mixed": 15, "request": 20, "discussion": 0},
        "sentiment_mapping": {"issue": 5, "mixed": 5, "request": 5, "discussion": 10},
        "competitive_intelligence": {"issue": 10, "mixed": 15, "request": 10, "discussion": 5},
        "general_survey": {"issue": 0, "mixed": 0, "request": 0, "discussion": 0},
    },
    novelty_points={
        "new_source_event": 10,
        "edited": 5,
        "newly_observed_historical": 2,
        "initial_observation": 0,
    },
    points_per_novel_comment=2,
    max_counted_comments=5,
    preference_points={
        "useful": 40,
        "irrelevant": -40,
        "already_known": -25,
        "wrong_audience": -30,
    },
    preference_bound=60,
)


def _invalid(field: str, message: str) -> MonitoringInputError:
    return MonitoringInputError(
        f"ranking policy {field} {message}",
        details={"reason": "invalid_ranking_policy", "field": field},
    )


def _table(payload: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    value = payload.get(field)
    if not isinstance(value, Mapping):
        raise _invalid(field, "must be a table")
    return value


def _exact_keys(table: Mapping[str, Any], field: str, keys: Sequence[str]) -> None:
    if set(table) != set(keys):
        raise _invalid(field, f"must have exactly the keys {sorted(keys)}")


def _bounded_int(value: object, field: str, bounds: tuple[int, int]) -> int:
    if type(value) is not int or not bounds[0] <= value <= bounds[1]:
        raise _invalid(field, f"must be an integer from {bounds[0]} to {bounds[1]}")
    return value


def _points(
    table: Mapping[str, Any], field: str, keys: Sequence[str], bounds: tuple[int, int]
) -> dict[str, int]:
    _exact_keys(table, field, keys)
    return {key: _bounded_int(table[key], f"{field}.{key}", bounds) for key in keys}


def parse_ranking_policy(payload: Mapping[str, Any]) -> RankingPolicy:
    """Validate a policy document strictly: unknown, missing or out-of-range fields fail."""
    unknown = set(payload) - _TOP_LEVEL
    if unknown:
        raise _invalid("document", f"has unsupported fields {sorted(unknown)}")
    if (
        payload.get("policy_version") != RANKING_POLICY_VERSION
        or type(payload.get("policy_version")) is not int
    ):
        raise _invalid("policy_version", f"must be {RANKING_POLICY_VERSION}")
    name = payload.get("name")
    if not isinstance(name, str) or POLICY_NAME_PATTERN.fullmatch(name) is None:
        raise _invalid("name", "must be a lowercase slug of at most 64 characters")
    objective = _table(payload, "objective")
    _exact_keys(objective, "objective", OBJECTIVES)
    evidence = _table(payload, "evidence")
    _exact_keys(evidence, "evidence", ("points_per_novel_comment", "max_counted_comments"))
    preference = _table(payload, "preference")
    _exact_keys(preference, "preference", (*PREFERENCE_VALUES, "bound"))
    return RankingPolicy(
        name=name,
        severity_points=_points(
            _table(payload, "severity"), "severity", SEVERITIES, _POSITIVE_RANGE
        ),
        objective_points={
            key: _points(_table(objective, key), f"objective.{key}", SIGNAL_TYPES, _POINT_RANGE)
            for key in OBJECTIVES
        },
        novelty_points=_points(
            _table(payload, "novelty"), "novelty", NOVELTY_KINDS, _POSITIVE_RANGE
        ),
        points_per_novel_comment=_bounded_int(
            evidence["points_per_novel_comment"],
            "evidence.points_per_novel_comment",
            _PER_COMMENT_RANGE,
        ),
        max_counted_comments=_bounded_int(
            evidence["max_counted_comments"], "evidence.max_counted_comments", _COMMENT_CAP_RANGE
        ),
        preference_points={
            "useful": _bounded_int(preference["useful"], "preference.useful", _POSITIVE_RANGE),
            **{
                value: _bounded_int(preference[value], f"preference.{value}", _NEGATIVE_RANGE)
                for value in PREFERENCE_VALUES
                if value != "useful"
            },
        },
        preference_bound=_bounded_int(preference["bound"], "preference.bound", _POSITIVE_RANGE),
    )


def load_ranking_policy_file(path: Path) -> RankingPolicy:
    try:
        with path.open("rb") as handle:
            payload = tomllib.load(handle)
    except FileNotFoundError as error:
        raise MonitoringInputError(
            "ranking policy file does not exist",
            details={"reason": "policy_file_missing", "path": str(path)},
        ) from error
    except tomllib.TOMLDecodeError as error:
        raise MonitoringInputError(
            "ranking policy file is not valid TOML",
            details={"reason": "invalid_ranking_policy", "path": str(path), "error": str(error)},
        ) from error
    except (OSError, UnicodeDecodeError) as error:
        raise MonitoringInputError(
            "ranking policy file cannot be read",
            details={"reason": "policy_file_unreadable", "path": str(path), "error": str(error)},
        ) from error
    return parse_ranking_policy(payload)


def stored_ranking_policy(policy_id: str, policy_json: str) -> RankingPolicy:
    """Re-validate a stored policy and prove it still hashes to its recorded id."""
    try:
        payload = json.loads(policy_json)
    except ValueError as error:
        raise _unreadable_policy(policy_id) from error
    if not isinstance(payload, dict):
        raise _unreadable_policy(policy_id)
    policy = parse_ranking_policy(payload)
    if policy.policy_id != policy_id or policy.canonical_json() != policy_json:
        raise _unreadable_policy(policy_id)
    return policy


def _unreadable_policy(policy_id: str) -> MonitoringStateError:
    return MonitoringStateError(
        "stored ranking policy does not match its recorded identity",
        details={"reason": "ranking_policy_unreadable", "policy_id": policy_id},
    )


def _entry_scopes(entry: Mapping[str, Any], issue_id: str | None) -> list[tuple[str, str]]:
    scopes = [("entry", str(entry["entry_id"]))]
    if entry["kind"] == "finding":
        scopes.append(("finding", f"{entry['source_thread_id']}/{entry['theme_key']}"))
    scopes.append(("thread", str(entry["source_thread_id"])))
    if issue_id is not None:
        scopes.append(("issue", issue_id))
    return scopes


def _novel_comments(entry: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    key = "novel_evidence" if entry["kind"] == "finding" else "novel_comments"
    comments = entry.get(key)
    if not isinstance(comments, list) or not all(isinstance(item, Mapping) for item in comments):
        raise _malformed_entry(entry, key)
    return comments


def _malformed_entry(entry: Mapping[str, Any], field: str) -> MonitoringStateError:
    return MonitoringStateError(
        "digest entry cannot be ranked from its recorded payload",
        details={
            "reason": "ranking_input_unreadable",
            "entry_id": entry.get("entry_id"),
            "field": field,
        },
    )


def _severity_component(entry: Mapping[str, Any], policy: RankingPolicy) -> dict[str, Any]:
    if entry["kind"] != "finding":
        return {"component": "severity", "points": 0, "reason": "thread updates carry no severity"}
    severity = entry.get("severity")
    if severity not in SEVERITIES:
        raise _malformed_entry(entry, "severity")
    return {
        "component": "severity",
        "points": policy.severity_points[str(severity)],
        "severity": severity,
        "reason": f"finding severity {severity}",
    }


def _objective_component(
    entry: Mapping[str, Any], facts: FindingFacts | None, objective: str, policy: RankingPolicy
) -> dict[str, Any]:
    if entry["kind"] != "finding" or facts is None:
        return {
            "component": "objective",
            "points": 0,
            "reason": "thread updates carry no finding signal",
        }
    try:
        signal = classify_finding_signal(
            str(entry["theme_key"]),
            str(entry["severity"]),
            facts.comment_count,
            facts.issue_marker_count,
            facts.request_marker_count,
        )
    except ValueError as error:
        raise MonitoringStateError(
            "finding signal cannot be classified",
            details={
                "reason": "ranking_input_unreadable",
                "entry_id": entry["entry_id"],
                "error": str(error),
            },
        ) from error
    return {
        "component": "objective",
        "points": policy.objective_points[objective][signal.signal_type],
        "objective": objective,
        "signal_type": signal.signal_type,
        "action_type": signal.action_type,
        "recommended_owner": signal.recommended_owner,
        "reason": f"objective {objective} weights {signal.signal_type} signals",
    }


def _novelty_component(
    comments: Sequence[Mapping[str, Any]], policy: RankingPolicy
) -> dict[str, Any]:
    kinds = {str(comment.get("kind")) for comment in comments}
    unknown = kinds - set(NOVELTY_KINDS)
    if unknown:
        raise MonitoringStateError(
            "novel comment kind is not rankable",
            details={"reason": "ranking_input_unreadable", "kinds": sorted(unknown)},
        )
    if not kinds:
        return {"component": "novelty", "points": 0, "reason": "no novel comments"}
    best = max(kinds, key=lambda kind: (policy.novelty_points[kind], -NOVELTY_KINDS.index(kind)))
    return {
        "component": "novelty",
        "points": policy.novelty_points[best],
        "novelty_kind": best,
        "reason": f"most novel evidence is {best}",
    }


def _evidence_component(
    comments: Sequence[Mapping[str, Any]], policy: RankingPolicy
) -> dict[str, Any]:
    counted = min(len(comments), policy.max_counted_comments)
    return {
        "component": "evidence",
        "points": counted * policy.points_per_novel_comment,
        "novel_comments": len(comments),
        "counted_comments": counted,
        "reason": (
            f"{counted} of {len(comments)} novel comments counted "
            f"(cap {policy.max_counted_comments})"
        ),
    }


def _preference_component(
    scopes: Sequence[tuple[str, str]],
    effective: Mapping[tuple[str, str], Mapping[str, Any]],
    policy: RankingPolicy,
) -> dict[str, Any]:
    matched = [
        {
            "feedback_id": effective[scope]["feedback_id"],
            "sequence": effective[scope]["sequence"],
            "value": effective[scope]["value"],
            "scope_kind": scope[0],
            "scope_key": scope[1],
            "points": policy.preference_points[str(effective[scope]["value"])],
        }
        for scope in scopes
        if scope in effective
    ]
    unbounded = sum(int(match["points"]) for match in matched)
    bound = policy.preference_bound
    points = max(-bound, min(bound, unbounded))
    if not matched:
        reason = "no operator preference applies"
    else:
        values = ", ".join(f"{match['value']} on {match['scope_kind']}" for match in matched)
        reason = f"operator preferences: {values}"
        if points != unbounded:
            reason += f" (bounded from {unbounded} to {points})"
    return {
        "component": "preference",
        "points": points,
        "unbounded_points": unbounded,
        "bound": bound,
        "matched": matched,
        "reason": reason,
    }


def _baseline(entry: Mapping[str, Any], facts: FindingFacts | None) -> dict[str, Any]:
    """The existing deterministic finding order, used only to break score ties."""
    is_finding = entry["kind"] == "finding" and facts is not None
    return {
        "kind": entry["kind"],
        "severity": entry.get("severity") if is_finding else None,
        "issue_markers": facts.issue_marker_count if facts is not None else 0,
        "request_markers": facts.request_marker_count if facts is not None else 0,
        "comment_count": facts.comment_count if facts is not None else 0,
        "theme_key": entry.get("theme_key") if is_finding else None,
        "entry_id": entry["entry_id"],
    }


def _sort_key(item: Mapping[str, Any]) -> tuple[int, int, int, int, int, int, str, str]:
    baseline = item["tie_break"]
    return (
        -int(item["score"]),
        _KIND_RANK[str(baseline["kind"])],
        -_SEVERITY_RANK.get(str(baseline["severity"]), 0),
        -int(baseline["issue_markers"]),
        -int(baseline["request_markers"]),
        -int(baseline["comment_count"]),
        str(baseline["theme_key"] or ""),
        str(baseline["entry_id"]),
    )


def rank_entries(
    entries: Sequence[Mapping[str, Any]],
    inputs: RankingInputs,
    finding_facts: Mapping[str, FindingFacts],
    issue_refs: Mapping[str, str],
) -> dict[str, Any]:
    """Rank one digest's entries; the result depends only on the given immutable inputs."""
    if inputs.objective not in OBJECTIVES:
        raise MonitoringStateError(
            "profile objective has no ranking weights",
            details={"reason": "ranking_input_unreadable", "objective": inputs.objective},
        )
    eligible = [
        event
        for event in inputs.preferences
        if int(event["sequence"]) <= inputs.basis.preference_sequence
    ]
    effective = {
        (str(item["scope_kind"]), str(item["scope_key"])): item
        for item in effective_preferences(eligible)
    }
    items: list[dict[str, Any]] = []
    for entry in entries:
        if entry.get("kind") not in _KIND_RANK:
            raise _malformed_entry(entry, "kind")
        for key in _REQUIRED_ENTRY_KEYS[str(entry["kind"])]:
            if key not in entry:
                raise _malformed_entry(entry, key)
        entry_id = str(entry["entry_id"])
        facts = finding_facts.get(entry_id)
        if entry["kind"] == "finding" and facts is None:
            raise _malformed_entry(entry, "analysis")
        comments = _novel_comments(entry)
        components = [
            _severity_component(entry, inputs.policy),
            _objective_component(entry, facts, inputs.objective, inputs.policy),
            _novelty_component(comments, inputs.policy),
            _evidence_component(comments, inputs.policy),
            _preference_component(
                _entry_scopes(entry, issue_refs.get(entry_id)), effective, inputs.policy
            ),
            dict(SEMANTIC_COMPONENT),
        ]
        score = sum(int(item["points"]) for item in components if item["points"] is not None)
        items.append(
            {
                "entry_id": entry_id,
                "kind": entry["kind"],
                "score": score,
                "components": components,
                "tie_break": _baseline(entry, facts),
            }
        )
    items.sort(key=_sort_key)
    return {
        "ranking_version": RANKING_VERSION,
        "basis": {
            **inputs.basis.to_dict(),
            "objective": inputs.objective,
            "policy_name": inputs.policy.name,
        },
        "semantic": {key: SEMANTIC_COMPONENT[key] for key in ("status", "reason")},
        "corrections": "not_a_ranking_input",
        "order": [{"rank": index, **item} for index, item in enumerate(items, start=1)],
    }
