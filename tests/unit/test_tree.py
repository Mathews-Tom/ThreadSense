from __future__ import annotations

from threadsense.models.canonical import AuthorRef, Comment
from threadsense.pipeline.tree import build_reply_tree, compute_tree_metrics


def _comment(
    comment_id: str,
    parent_id: str | None = None,
    depth: int = 0,
    score: int = 1,
) -> Comment:
    return Comment(
        thread_id="test",
        comment_id=comment_id,
        parent_comment_id=parent_id,
        author=AuthorRef(username="user", source_author_id=None),
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
