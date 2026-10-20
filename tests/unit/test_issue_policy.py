from __future__ import annotations

from pathlib import Path

import pytest

from threadsense.config import (
    ISSUE_CANDIDATE_WINDOW_DAYS_RANGE,
    ISSUE_MAX_CANDIDATE_COMPARISONS_RANGE,
    ISSUE_MAX_SUGGESTIONS_RANGE,
    load_config,
)
from threadsense.errors import ConfigurationError, MonitoringInputError
from threadsense.monitoring.issue_records import (
    ISSUE_ID_PATTERN,
    LINEAGE_MIN_CHARS,
    REPORT_ID_PATTERN,
    SUGGESTION_ID_PATTERN,
    UPDATE_ID_PATTERN,
    UPDATE_KINDS,
    IssuePolicy,
    issue_id_for,
    report_id_for,
    suggestion_id_for,
    update_id_for,
)
from threadsense.monitoring.issues import (
    account_sha256,
    detect_update_cues,
    extract_links,
    issue_policy,
)

ENV_WINDOW = "THREADSENSE_ISSUES_CANDIDATE_WINDOW_DAYS"
ENV_COMPARISONS = "THREADSENSE_ISSUES_MAX_CANDIDATE_COMPARISONS"
ENV_SUGGESTIONS = "THREADSENSE_ISSUES_MAX_SUGGESTIONS_PER_THREAD"
POLICY_FIELDS = (
    ("candidate_window_days", ISSUE_CANDIDATE_WINDOW_DAYS_RANGE),
    ("max_candidate_comparisons", ISSUE_MAX_CANDIDATE_COMPARISONS_RANGE),
    ("max_suggestions_per_thread", ISSUE_MAX_SUGGESTIONS_RANGE),
)


def default_policy(**overrides: int) -> IssuePolicy:
    bounds = {"candidate_window_days": 30, "max_candidate_comparisons": 25}
    bounds["max_suggestions_per_thread"] = 3
    bounds.update(overrides)
    return IssuePolicy(**bounds)


def test_issue_id_for_same_inputs_is_deterministic_and_prefixed() -> None:
    first = issue_id_for("watch-a", "t3_abc")

    assert first == issue_id_for("watch-a", "t3_abc")
    assert ISSUE_ID_PATTERN.match(first)


def test_issue_id_for_different_watches_same_thread_differs() -> None:
    assert issue_id_for("watch-a", "t3_abc") != issue_id_for("watch-b", "t3_abc")


def test_report_and_update_ids_match_their_patterns() -> None:
    report_id = report_id_for("watch-a", "t3_abc", "c1")

    assert REPORT_ID_PATTERN.match(report_id)
    assert report_id == report_id_for("watch-a", "t3_abc", "c1")
    assert report_id != report_id_for("watch-a", "t3_abc", "c2")
    assert UPDATE_ID_PATTERN.match(update_id_for(report_id, "f" * 64, "workaround"))


def test_suggestion_id_for_different_policy_sha_differs() -> None:
    first = suggestion_id_for("watch-a", "t3_a", "t3_b", "a" * 64)
    second = suggestion_id_for("watch-a", "t3_a", "t3_b", "b" * 64)

    assert SUGGESTION_ID_PATTERN.match(first)
    assert first != second


def test_issue_policy_equal_bounds_have_stable_sha256() -> None:
    assert default_policy().sha256() == default_policy().sha256()
    assert len(default_policy().sha256()) == 64


@pytest.mark.parametrize(
    "override",
    [
        {"candidate_window_days": 31},
        {"max_candidate_comparisons": 26},
        {"max_suggestions_per_thread": 4},
    ],
)
def test_issue_policy_changed_bound_changes_sha256(override: dict[str, int]) -> None:
    assert default_policy(**override).sha256() != default_policy().sha256()


def test_issue_policy_to_dict_records_unverified_ungrouped_contract() -> None:
    record = default_policy().to_dict()

    assert record["semantic_grouping"]["status"] == "not_eligible"
    assert record["candidates"]["window_days"] == 30
    assert record["candidates"]["max_comparisons_per_thread"] == 25
    assert record["candidates"]["max_suggestions_per_thread"] == 3
    assert record["updates"]["claims_are_verified"] is False
    assert record["membership"]["exact_content_lineage"]["min_chars"] == LINEAGE_MIN_CHARS


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (field, bad)
        for field, (low, high) in POLICY_FIELDS
        for bad in (low - 1, high + 1, True, "3", 2.0)
    ],
)
def test_issue_policy_invalid_bound_raises_input_error(field: str, value: object) -> None:
    bounds: dict[str, object] = {name: low for name, (low, _) in POLICY_FIELDS}
    bounds[field] = value

    with pytest.raises(MonitoringInputError) as raised:
        IssuePolicy(**bounds)  # type: ignore[arg-type]

    assert raised.value.details["reason"] == "invalid_issue_policy"
    assert raised.value.details["field"] == field


def test_issue_policy_default_config_matches_documented_defaults() -> None:
    policy = issue_policy(load_config(None, env={}).issues)

    assert policy == default_policy()


def test_load_config_issue_env_vars_set_bounds() -> None:
    env = {ENV_WINDOW: "14", ENV_COMPARISONS: "40", ENV_SUGGESTIONS: "5"}

    issues = load_config(None, env=env).issues

    assert (
        issues.candidate_window_days,
        issues.max_candidate_comparisons,
        issues.max_suggestions_per_thread,
    ) == (14, 40, 5)


def test_load_config_issues_toml_section_sets_bounds(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "[issues]\n"
        "candidate_window_days = 7\n"
        "max_candidate_comparisons = 10\n"
        "max_suggestions_per_thread = 2\n",
        encoding="utf-8",
    )

    issues = load_config(config_path, env={}).issues

    assert issue_policy(issues) == default_policy(
        candidate_window_days=7, max_candidate_comparisons=10, max_suggestions_per_thread=2
    )


@pytest.mark.parametrize(
    ("env_name", "bounds"),
    [
        (ENV_WINDOW, ISSUE_CANDIDATE_WINDOW_DAYS_RANGE),
        (ENV_COMPARISONS, ISSUE_MAX_CANDIDATE_COMPARISONS_RANGE),
        (ENV_SUGGESTIONS, ISSUE_MAX_SUGGESTIONS_RANGE),
    ],
)
@pytest.mark.parametrize("edge", ["below", "above"])
def test_load_config_issue_value_out_of_range_raises(
    env_name: str, bounds: tuple[int, int], edge: str
) -> None:
    value = bounds[0] - 1 if edge == "below" else bounds[1] + 1

    with pytest.raises(ConfigurationError):
        load_config(None, env={env_name: str(value)})


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("Steps to reproduce: open the app", (("reproduction", "steps to reproduce"),)),
        ("I cannot reproduce this", (("contradiction", "cannot reproduce"),)),
        ("unable to reproduce on 2.3", (("contradiction", "unable to reproduce"),)),
        ("Workaround: restart the daemon", (("workaround", "workaround"),)),
        ("it is fixed in 2.4", (("resolution_claim", "fixed in"),)),
        ("I can’t reproduce it", (("contradiction", "can't reproduce"),)),
        ("Steps   to\nREPRODUCE it", (("reproduction", "steps to reproduce"),)),
    ],
)
def test_detect_update_cues_cue_text_yields_single_kind(
    body: str, expected: tuple[tuple[str, str], ...]
) -> None:
    assert detect_update_cues(body) == expected


@pytest.mark.parametrize(
    "body",
    ["There are many workarounds", "It was prefixed in the build", "The app crashes on start"],
)
def test_detect_update_cues_non_boundary_or_plain_text_returns_empty(body: str) -> None:
    assert detect_update_cues(body) == ()


def test_detect_update_cues_multiple_kinds_follow_update_kinds_order() -> None:
    body = "Fixed in 2.4. Workaround: restart. Repro steps: open app. I cannot reproduce it."

    kinds = [kind for kind, _ in detect_update_cues(body)]

    assert kinds == list(UPDATE_KINDS)


def test_extract_links_normalizes_scheme_host_and_keeps_path_case() -> None:
    body = "See https://Example.COM/Path/Foo)."

    assert extract_links(body) == ("https://example.com/Path/Foo",)


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("link https://a.com/x#frag, ok", ("https://a.com/x",)),
        ("link https://a.com/x/ ok", ("https://a.com/x",)),
        ("root https://a.com/ ok", ("https://a.com/",)),
        ("bare www.example.com/a here", ("https://www.example.com/a",)),
        ("no links at all", ()),
    ],
)
def test_extract_links_cleanup_rules(body: str, expected: tuple[str, ...]) -> None:
    assert extract_links(body) == expected


def test_extract_links_duplicates_collapse_and_results_sorted() -> None:
    body = "https://b.com/z https://a.com/y https://A.com/y/ (https://b.com/z)"

    assert extract_links(body) == ("https://a.com/y", "https://b.com/z")


@pytest.mark.parametrize("username", ["", "  ", "[deleted]", "[removed]"])
def test_account_sha256_blank_or_removed_account_returns_none(username: str) -> None:
    assert account_sha256("reddit", username) is None


def test_account_sha256_username_case_is_ignored() -> None:
    assert account_sha256("reddit", "Alice") == account_sha256("reddit", "aLICE")


def test_account_sha256_source_name_separates_accounts() -> None:
    reddit = account_sha256("reddit", "alice")
    other = account_sha256("hackernews", "alice")

    assert reddit is not None
    assert len(reddit) == 64
    assert int(reddit, 16) >= 0
    assert reddit != other
