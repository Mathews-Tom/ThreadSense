from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FindingSignal:
    signal_type: str
    recommended_owner: str
    action_type: str


def classify_finding_signal(
    theme_key: str,
    severity: str,
    comment_count: int,
    issue_marker_count: int,
    request_marker_count: int,
) -> FindingSignal:
    if comment_count < 0 or issue_marker_count < 0 or request_marker_count < 0:
        raise ValueError("marker and comment counts must be nonnegative")
    if severity not in {"high", "medium", "low"}:
        raise ValueError(f"unsupported severity: {severity}")
    if theme_key == "documentation" and (issue_marker_count > 0 or request_marker_count > 0):
        return FindingSignal(
            signal_type=signal_type_for_counts(issue_marker_count, request_marker_count),
            recommended_owner="docs",
            action_type="document",
        )
    if (
        request_marker_count > 0
        and request_marker_count >= issue_marker_count
        and issue_marker_count <= 1
    ):
        return FindingSignal(
            signal_type=signal_type_for_counts(issue_marker_count, request_marker_count),
            recommended_owner="product",
            action_type="design",
        )
    if severity == "low" and comment_count >= 2 and issue_marker_count <= request_marker_count:
        return FindingSignal(
            signal_type=signal_type_for_counts(issue_marker_count, request_marker_count),
            recommended_owner="research",
            action_type="monitor",
        )
    if issue_marker_count > request_marker_count and severity == "high":
        return FindingSignal(
            signal_type=signal_type_for_counts(issue_marker_count, request_marker_count),
            recommended_owner="engineering",
            action_type="fix",
        )
    if issue_marker_count > 0:
        return FindingSignal(
            signal_type=signal_type_for_counts(issue_marker_count, request_marker_count),
            recommended_owner="engineering",
            action_type="investigate",
        )
    if issue_marker_count == 0 and request_marker_count == 0:
        return FindingSignal(
            signal_type="discussion",
            recommended_owner="research",
            action_type="investigate",
        )
    raise ValueError(
        "finding signal classification is undefined for the provided marker combination"
    )


def signal_type_for_counts(issue_marker_count: int, request_marker_count: int) -> str:
    if issue_marker_count > 0 and request_marker_count > 0:
        return "mixed"
    if issue_marker_count > 0:
        return "issue"
    if request_marker_count > 0:
        return "request"
    return "discussion"
