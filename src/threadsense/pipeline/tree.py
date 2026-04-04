from __future__ import annotations

from dataclasses import dataclass

from threadsense.models.canonical import Comment


@dataclass(frozen=True)
class TreeMetrics:
    max_depth: int
    total_replies: int
    top_level_count: int
    branching_factors: dict[str, int]  # comment_id -> number of direct replies
    subtree_sizes: dict[str, int]  # comment_id -> total descendants (inclusive)


def build_reply_tree(comments: list[Comment]) -> dict[str | None, list[Comment]]:
    """Reconstruct adjacency list from parent_comment_id.

    Returns a dict mapping parent_comment_id -> list of direct child comments.
    Key None holds top-level comments (replies to the post).
    """
    adjacency: dict[str | None, list[Comment]] = {}
    for comment in comments:
        adjacency.setdefault(comment.parent_comment_id, []).append(comment)
    return adjacency


def compute_tree_metrics(comments: list[Comment]) -> TreeMetrics:
    """Compute structural metrics from the comment tree."""
    if not comments:
        return TreeMetrics(
            max_depth=0,
            total_replies=0,
            top_level_count=0,
            branching_factors={},
            subtree_sizes={},
        )

    adjacency = build_reply_tree(comments)
    top_level = adjacency.get(None, [])

    branching_factors: dict[str, int] = {}
    for comment in comments:
        children = adjacency.get(comment.comment_id, [])
        branching_factors[comment.comment_id] = len(children)

    subtree_sizes: dict[str, int] = {}
    _compute_subtree_sizes(adjacency, subtree_sizes, None)

    max_depth = max(comment.depth for comment in comments)
    total_replies = sum(1 for comment in comments if comment.parent_comment_id is not None)

    return TreeMetrics(
        max_depth=max_depth,
        total_replies=total_replies,
        top_level_count=len(top_level),
        branching_factors=branching_factors,
        subtree_sizes=subtree_sizes,
    )


def _compute_subtree_sizes(
    adjacency: dict[str | None, list[Comment]],
    sizes: dict[str, int],
    parent_id: str | None,
) -> int:
    """Recursively compute subtree sizes. Returns total count for this subtree."""
    children = adjacency.get(parent_id, [])
    total = 0
    for child in children:
        child_size = 1 + _compute_subtree_sizes(adjacency, sizes, child.comment_id)
        sizes[child.comment_id] = child_size
        total += child_size
    return total
