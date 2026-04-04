from __future__ import annotations

from threadsense.models.canonical import AuthorRef, Comment
from threadsense.pipeline.tree import (
    build_reply_tree,
    compute_tree_metrics,
    detect_conversation_patterns,
    extract_reply_chains,
    score_subtrees,
)


def _comment(
    comment_id: str,
    parent_id: str | None = None,
    depth: int = 0,
    score: int = 1,
    author: str = "user",
) -> Comment:
    return Comment(
        thread_id="test",
        comment_id=comment_id,
        parent_comment_id=parent_id,
        author=AuthorRef(username=author, source_author_id=None),
        body="test body",
        score=score,
        created_utc=1.0,
        depth=depth,
        permalink="https://example.com",
    )


def test_build_reply_tree_groups_by_parent() -> None:
    comments = [
        _comment("c1", None, depth=0),
        _comment("c2", None, depth=0),
        _comment("c1a", "c1", depth=1),
        _comment("c1b", "c1", depth=1),
        _comment("c1a1", "c1a", depth=2),
    ]
    tree = build_reply_tree(comments)
    assert len(tree[None]) == 2
    assert len(tree["c1"]) == 2
    assert len(tree["c1a"]) == 1
    assert "c2" not in tree  # c2 has no children


def test_compute_tree_metrics_on_hierarchical_thread() -> None:
    comments = [
        _comment("c1", None, depth=0),
        _comment("c2", None, depth=0),
        _comment("c1a", "c1", depth=1),
        _comment("c1b", "c1", depth=1),
        _comment("c1a1", "c1a", depth=2),
    ]
    metrics = compute_tree_metrics(comments)
    assert metrics.max_depth == 2
    assert metrics.top_level_count == 2
    assert metrics.total_replies == 3
    assert metrics.branching_factors["c1"] == 2
    assert metrics.branching_factors["c1a"] == 1
    assert metrics.branching_factors["c2"] == 0
    assert metrics.subtree_sizes["c1"] == 4  # c1 + c1a + c1b + c1a1
    assert metrics.subtree_sizes["c1a"] == 2  # c1a + c1a1
    assert metrics.subtree_sizes["c1a1"] == 1  # leaf
    assert metrics.subtree_sizes["c2"] == 1  # leaf


def test_compute_tree_metrics_on_flat_thread() -> None:
    comments = [
        _comment("c1", None, depth=0),
        _comment("c2", None, depth=0),
        _comment("c3", None, depth=0),
    ]
    metrics = compute_tree_metrics(comments)
    assert metrics.max_depth == 0
    assert metrics.top_level_count == 3
    assert metrics.total_replies == 0
    assert all(bf == 0 for bf in metrics.branching_factors.values())
    assert all(size == 1 for size in metrics.subtree_sizes.values())


def test_compute_tree_metrics_on_empty_thread() -> None:
    metrics = compute_tree_metrics([])
    assert metrics.max_depth == 0
    assert metrics.top_level_count == 0
    assert metrics.total_replies == 0


# ---------------------------------------------------------------------------
# Reply Chain Extraction
# ---------------------------------------------------------------------------


def test_extract_reply_chains_finds_linear_sequences() -> None:
    comments = [
        _comment("c1", None, depth=0),
        _comment("c1a", "c1", depth=1),
        _comment("c1a1", "c1a", depth=2),
        _comment("c1a1a", "c1a1", depth=3),
        _comment("c2", None, depth=0),
    ]
    chains = extract_reply_chains(comments, min_length=3)
    assert len(chains) == 1
    assert chains[0].length == 4
    assert chains[0].comment_ids == ["c1", "c1a", "c1a1", "c1a1a"]


def test_extract_reply_chains_excludes_branching() -> None:
    comments = [
        _comment("c1", None, depth=0),
        _comment("c1a", "c1", depth=1),
        _comment("c1b", "c1", depth=1),  # Branching — breaks chain
    ]
    chains = extract_reply_chains(comments, min_length=2)
    assert len(chains) == 0


def test_extract_reply_chains_respects_min_length() -> None:
    comments = [
        _comment("c1", None, depth=0),
        _comment("c1a", "c1", depth=1),
    ]
    assert extract_reply_chains(comments, min_length=3) == []
    chains = extract_reply_chains(comments, min_length=2)
    assert len(chains) == 1
    assert chains[0].length == 2


# ---------------------------------------------------------------------------
# Subtree Scoring
# ---------------------------------------------------------------------------


def test_score_subtrees_ranks_by_engagement() -> None:
    comments = [
        _comment("c1", None, depth=0, score=10),
        _comment("c1a", "c1", depth=1, score=5),
        _comment("c1a1", "c1a", depth=2, score=3),
        _comment("c2", None, depth=0, score=1),
        _comment("c2a", "c2", depth=1, score=1),
    ]
    scored = score_subtrees(comments, min_subtree_size=2)
    assert len(scored) >= 2
    assert scored[0].root_comment_id == "c1"  # Higher engagement


def test_score_subtrees_excludes_small_subtrees() -> None:
    comments = [
        _comment("c1", None, depth=0, score=100),
        _comment("c2", None, depth=0, score=1),
        _comment("c2a", "c2", depth=1, score=1),
    ]
    scored = score_subtrees(comments, min_subtree_size=2)
    # c1 has subtree_size=1, excluded; c2 has subtree_size=2, included
    root_ids = [s.root_comment_id for s in scored]
    assert "c1" not in root_ids
    assert "c2" in root_ids


# ---------------------------------------------------------------------------
# Conversation Pattern Detection
# ---------------------------------------------------------------------------


def test_detect_conversation_patterns_identifies_controversy() -> None:
    comments = [
        _comment("c1", None, depth=0, score=50, author="alice"),
        _comment("c1a", "c1", depth=1, score=-10, author="bob"),
        _comment("c1b", "c1", depth=1, score=30, author="carol"),
        _comment("c1c", "c1", depth=1, score=-5, author="dave"),
    ]
    patterns = detect_conversation_patterns(comments, min_subtree_size=3)
    controversial = [p for p in patterns if p.pattern_type == "controversy"]
    assert len(controversial) >= 1


def test_detect_conversation_patterns_identifies_monologue() -> None:
    comments = [
        _comment("c1", None, depth=0, score=5),
        _comment("c1a", "c1", depth=1, score=3),
        _comment("c1a1", "c1a", depth=2, score=2),
    ]
    # Default author is "user" for all — single author → monologue
    patterns = detect_conversation_patterns(comments, min_subtree_size=3)
    monologue = [p for p in patterns if p.pattern_type == "monologue"]
    assert len(monologue) >= 1


def test_detect_conversation_patterns_identifies_consensus() -> None:
    comments = [
        _comment("c1", None, depth=0, score=5, author="alice"),
        _comment("c1a", "c1", depth=1, score=6, author="bob"),
        _comment("c1b", "c1", depth=1, score=5, author="carol"),
    ]
    patterns = detect_conversation_patterns(comments, min_subtree_size=3)
    consensus = [p for p in patterns if p.pattern_type == "consensus"]
    assert len(consensus) >= 1
