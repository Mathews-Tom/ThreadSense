from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path

import pytest

from threadsense.models.canonical import AuthorRef, Comment, ProvenanceMetadata, SourceRef, Thread
from threadsense.monitoring.observations import (
    build_revision_snapshot,
    comment_text_sha256,
    diff_observation,
    revision_id_for,
    substantive_sha256,
    thread_text_sha256,
)
from threadsense.monitoring.sweep_records import (
    CommentChange,
    KnownComment,
    RevisionRef,
    ThreadHistory,
)

WATCH_ID = "sync-conflicts"
SOURCE_THREAD_ID = "sc1aaa"
THREAD_ID = f"reddit:{SOURCE_THREAD_ID}"
THREAD_TITLE = "Sync conflict after offline edit"
THREAD_BODY = "Two devices edited the same note offline and the merge kept both copies."
PRIOR_OBSERVED_AT = 1_800_000_000.0
RETRIEVED_AT = 1_800_000_500.0
OBSERVED_AT = 1_800_000_510.0
RAW_PATH = Path(
    "/state/monitoring/watches/sync-conflicts/threads/sc1aaa/acquisitions/acq-0123.raw.json"
)
RAW_SHA256 = "a" * 64


def make_comment(
    comment_id: str,
    body: str,
    *,
    created_utc: float,
    parent_comment_id: str | None = None,
    score: int = 1,
    permalink: str | None = None,
) -> Comment:
    return Comment(
        thread_id=THREAD_ID,
        comment_id=comment_id,
        parent_comment_id=parent_comment_id,
        author=AuthorRef(username=f"author-{comment_id}", source_author_id=None),
        body=body,
        score=score,
        created_utc=created_utc,
        depth=0 if parent_comment_id is None else 1,
        permalink=permalink or f"https://reddit.com/r/SyncTools/comments/sc1aaa/x/{comment_id}/",
    )


def make_thread(
    comments: list[Comment],
    *,
    title: str = THREAD_TITLE,
    body: str | None = THREAD_BODY,
) -> Thread:
    return Thread(
        thread_id=THREAD_ID,
        source=SourceRef(
            source_name="reddit",
            community="SyncTools",
            source_thread_id=SOURCE_THREAD_ID,
            thread_url="https://www.reddit.com/r/SyncTools/comments/sc1aaa/x.json",
        ),
        title=title,
        body=body,
        permalink="https://reddit.com/r/SyncTools/comments/sc1aaa/x/",
        author=AuthorRef(username="field_notes", source_author_id=None),
        comments=comments,
        comment_count=len(comments),
        provenance=ProvenanceMetadata(
            raw_artifact_path="/elsewhere/raw.json",
            raw_sha256="b" * 64,
            retrieved_at_utc=RETRIEVED_AT,
            normalized_at_utc=RETRIEVED_AT + 1.0,
            schema_version=2,
            normalization_version="canonical-thread-v2",
        ),
    )


def baseline_comments() -> list[Comment]:
    return [
        make_comment("reddit:ka1", "The merge tool keeps both versions.", created_utc=100.0),
        make_comment(
            "reddit:ka2",
            "Disabling background sync stopped the duplicates.",
            created_utc=160.0,
            parent_comment_id="reddit:ka1",
        ),
        make_comment("reddit:ka3", "Is there a prefer-newest setting?", created_utc=220.0),
    ]


def empty_history() -> ThreadHistory:
    return ThreadHistory(
        source_thread_id=SOURCE_THREAD_ID,
        known_comments={},
        current_revision=None,
        prior_observed_at_utc=None,
    )


def history_after(thread: Thread) -> ThreadHistory:
    known = {
        comment.comment_id: KnownComment(
            comment_id=comment.comment_id,
            text_sha256=comment_text_sha256(comment.body),
            source_created_utc=comment.created_utc,
            novelty="initial_observation",
            first_observed_at_utc=PRIOR_OBSERVED_AT,
        )
        for comment in thread.comments
    }
    revision = RevisionRef(
        revision_id="rev-" + "0" * 32,
        source_thread_id=SOURCE_THREAD_ID,
        sequence=1,
        snapshot_path=Path("/state/monitoring/revisions/rev-0/snapshot.json"),
        snapshot_sha256="c" * 64,
        thread_text_sha256=thread_text_sha256(thread.title, thread.body),
        created_at_utc=PRIOR_OBSERVED_AT,
    )
    return ThreadHistory(
        source_thread_id=SOURCE_THREAD_ID,
        known_comments=known,
        current_revision=revision,
        prior_observed_at_utc=PRIOR_OBSERVED_AT,
    )


def kinds_by_id(changes: tuple[CommentChange, ...]) -> dict[str, str]:
    return {change.comment_id: change.kind for change in changes}


def test_diff_observation_first_acquisition_marks_every_comment_initial_observation() -> None:
    acquired = make_thread(baseline_comments())

    diff = diff_observation(empty_history(), acquired)

    assert kinds_by_id(diff.changes) == {
        "reddit:ka1": "initial_observation",
        "reddit:ka2": "initial_observation",
        "reddit:ka3": "initial_observation",
    }


def test_diff_observation_first_acquisition_is_substantive_without_text_change() -> None:
    acquired = make_thread(baseline_comments())

    diff = diff_observation(empty_history(), acquired)

    assert diff.substantive is True
    assert diff.thread_text_changed is False


def test_diff_observation_changes_are_sorted_by_comment_id() -> None:
    acquired = make_thread(list(reversed(baseline_comments())))

    diff = diff_observation(empty_history(), acquired)

    assert [change.comment_id for change in diff.changes] == [
        "reddit:ka1",
        "reddit:ka2",
        "reddit:ka3",
    ]


def test_diff_observation_identical_acquisition_has_no_changes() -> None:
    baseline = make_thread(baseline_comments())

    diff = diff_observation(history_after(baseline), make_thread(baseline_comments()))

    assert diff.changes == ()
    assert diff.substantive is False


def test_diff_observation_comment_created_after_prior_observation_is_new_source_event() -> None:
    baseline = make_thread(baseline_comments())
    reply = make_comment("reddit:ka4", "Version 2.4 fixed it.", created_utc=PRIOR_OBSERVED_AT + 5)

    diff = diff_observation(history_after(baseline), make_thread([*baseline_comments(), reply]))

    assert kinds_by_id(diff.changes) == {"reddit:ka4": "new_source_event"}


@pytest.mark.parametrize(
    "created_utc",
    [PRIOR_OBSERVED_AT, PRIOR_OBSERVED_AT - 3600.0],
    ids=["at_prior_observation", "before_prior_observation"],
)
def test_diff_observation_comment_not_after_prior_observation_is_newly_observed_historical(
    created_utc: float,
) -> None:
    baseline = make_thread(baseline_comments())
    late = make_comment("reddit:ka5", "Only within the same minute.", created_utc=created_utc)

    diff = diff_observation(history_after(baseline), make_thread([*baseline_comments(), late]))

    assert kinds_by_id(diff.changes) == {"reddit:ka5": "newly_observed_historical"}


def test_diff_observation_known_comment_with_new_text_is_edited_with_previous_hash() -> None:
    baseline = make_thread(baseline_comments())
    original = baseline_comments()[2]
    edited_body = "Is there a prefer-newest setting? Found it under advanced."
    acquired = make_thread([*baseline_comments()[:2], replace(original, body=edited_body)])

    diff = diff_observation(history_after(baseline), acquired)

    assert len(diff.changes) == 1
    change = diff.changes[0]
    assert change.comment_id == "reddit:ka3"
    assert change.kind == "edited"
    assert change.text_sha256 == comment_text_sha256(edited_body)
    assert change.previous_text_sha256 == comment_text_sha256(original.body)


def test_comment_change_edited_to_dict_leaves_source_edit_time_unknown() -> None:
    change = CommentChange(
        comment_id="reddit:ka3",
        kind="edited",
        text_sha256="d" * 64,
        previous_text_sha256="e" * 64,
        source_created_utc=220.0,
    )

    payload = change.to_dict()

    assert payload["source_edited_at_utc"] is None


def test_diff_observation_absent_known_comment_is_not_a_change() -> None:
    baseline = make_thread(baseline_comments())
    acquired = make_thread([baseline_comments()[0], baseline_comments()[2]])

    diff = diff_observation(history_after(baseline), acquired)

    assert diff.changes == ()
    assert diff.substantive is False


def test_diff_observation_absent_known_comment_is_reported_as_absent() -> None:
    baseline = make_thread(baseline_comments())
    acquired = make_thread([baseline_comments()[0], baseline_comments()[2]])

    diff = diff_observation(history_after(baseline), acquired)

    assert diff.absent_known_comment_ids == ("reddit:ka2",)


def test_diff_observation_score_order_and_permalink_change_is_not_substantive() -> None:
    baseline = make_thread(baseline_comments())
    reshuffled = [
        replace(comment, score=comment.score + 40, permalink=f"{comment.permalink}?context=3")
        for comment in reversed(baseline_comments())
    ]

    diff = diff_observation(history_after(baseline), make_thread(reshuffled))

    assert diff.changes == ()
    assert diff.substantive is False


@pytest.mark.parametrize(
    ("title", "body"),
    [
        ("Sync conflict after offline edit (solved)", THREAD_BODY),
        (THREAD_TITLE, f"{THREAD_BODY} Update: fixed in 2.4."),
    ],
    ids=["title", "body"],
)
def test_diff_observation_thread_text_change_is_substantive(title: str, body: str) -> None:
    baseline = make_thread(baseline_comments())

    diff = diff_observation(
        history_after(baseline), make_thread(baseline_comments(), title=title, body=body)
    )

    assert diff.thread_text_changed is True
    assert diff.substantive is True
    assert diff.thread_text_sha256 == thread_text_sha256(title, body)


def test_substantive_sha256_ignores_score_and_order() -> None:
    text_sha = thread_text_sha256(THREAD_TITLE, THREAD_BODY)
    rescored = [replace(comment, score=comment.score + 9) for comment in baseline_comments()]

    original = substantive_sha256(SOURCE_THREAD_ID, text_sha, baseline_comments())
    shuffled = substantive_sha256(SOURCE_THREAD_ID, text_sha, list(reversed(rescored)))

    assert original == shuffled


def test_substantive_sha256_comment_text_change_changes_digest() -> None:
    text_sha = thread_text_sha256(THREAD_TITLE, THREAD_BODY)
    comments = baseline_comments()
    edited = [*comments[:2], replace(comments[2], body="A different question.")]

    original = substantive_sha256(SOURCE_THREAD_ID, text_sha, comments)
    changed = substantive_sha256(SOURCE_THREAD_ID, text_sha, edited)

    assert original != changed


def test_revision_id_for_same_transition_returns_same_id() -> None:
    first = revision_id_for(WATCH_ID, SOURCE_THREAD_ID, None, "1" * 64)

    second = revision_id_for(WATCH_ID, SOURCE_THREAD_ID, None, "1" * 64)

    assert first == second
    assert re.fullmatch(r"rev-[0-9a-f]{32}", first) is not None


def test_revision_id_for_a_b_a_chain_returns_distinct_ids() -> None:
    content_a = "a" * 64
    content_b = "b" * 64

    rev_a = revision_id_for(WATCH_ID, SOURCE_THREAD_ID, None, content_a)
    rev_b = revision_id_for(WATCH_ID, SOURCE_THREAD_ID, rev_a, content_b)
    rev_a_again = revision_id_for(WATCH_ID, SOURCE_THREAD_ID, rev_b, content_a)

    assert len({rev_a, rev_b, rev_a_again}) == 3


def snapshot_inputs() -> tuple[Thread, Thread]:
    previous = make_thread(
        [
            make_comment("reddit:ka1", "The merge tool keeps both versions.", created_utc=100.0),
            make_comment("reddit:kd2", "Disabling background sync helped.", created_utc=200.0),
            make_comment("reddit:kc3", "Is there a prefer-newest setting?", created_utc=150.0),
        ]
    )
    acquired = make_thread(
        [
            make_comment(
                "reddit:ka1", "The merge tool keeps both versions.", created_utc=100.0, score=30
            ),
            make_comment("reddit:kc3", "Is there a prefer-newest setting?", created_utc=150.0),
            make_comment("reddit:kb4", "Version 2.4 fixed it.", created_utc=150.0),
        ]
    )
    return previous, acquired


def build_snapshot() -> Thread:
    previous, acquired = snapshot_inputs()
    return build_revision_snapshot(
        previous_snapshot=previous,
        acquired=acquired,
        raw_artifact_path=RAW_PATH,
        raw_sha256=RAW_SHA256,
        observed_at_utc=OBSERVED_AT,
    )


def test_build_revision_snapshot_keeps_absent_known_comments() -> None:
    snapshot = build_snapshot()

    kept = [comment for comment in snapshot.comments if comment.comment_id == "reddit:kd2"]

    assert len(kept) == 1
    assert kept[0].body == "Disabling background sync helped."


def test_build_revision_snapshot_present_comment_uses_acquired_version() -> None:
    snapshot = build_snapshot()

    present = next(comment for comment in snapshot.comments if comment.comment_id == "reddit:ka1")

    assert present.score == 30


def test_build_revision_snapshot_sorts_comments_by_created_then_id() -> None:
    snapshot = build_snapshot()

    ordered = [comment.comment_id for comment in snapshot.comments]

    assert ordered == ["reddit:ka1", "reddit:kb4", "reddit:kc3", "reddit:kd2"]


def test_build_revision_snapshot_comment_count_equals_union_size() -> None:
    snapshot = build_snapshot()

    assert snapshot.comment_count == len(snapshot.comments) == 4


def test_build_revision_snapshot_provenance_points_to_given_raw_artifact() -> None:
    snapshot = build_snapshot()

    provenance = snapshot.provenance

    assert provenance.raw_artifact_path == str(RAW_PATH)
    assert provenance.raw_sha256 == RAW_SHA256
    assert provenance.retrieved_at_utc == RETRIEVED_AT
    assert provenance.normalized_at_utc == OBSERVED_AT
    assert provenance.schema_version == 2
    assert provenance.normalization_version == "canonical-thread-v2"


def test_build_revision_snapshot_without_previous_contains_only_acquired_comments() -> None:
    _, acquired = snapshot_inputs()

    snapshot = build_revision_snapshot(
        previous_snapshot=None,
        acquired=acquired,
        raw_artifact_path=RAW_PATH,
        raw_sha256=RAW_SHA256,
        observed_at_utc=OBSERVED_AT,
    )

    assert {comment.comment_id for comment in snapshot.comments} == {
        "reddit:ka1",
        "reddit:kb4",
        "reddit:kc3",
    }
