from __future__ import annotations

import math
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


# ---------------------------------------------------------------------------
# Reply Chain Extraction
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReplyChain:
    comment_ids: list[str]
    authors: list[str]
    depth_range: tuple[int, int]  # (min_depth, max_depth)
    length: int


def extract_reply_chains(
    comments: list[Comment],
    min_length: int = 3,
) -> list[ReplyChain]:
    """Extract linear reply chains from the comment tree.

    A reply chain is a maximal path where each node has exactly one reply.
    Chains shorter than *min_length* are excluded.
    """
    adjacency = build_reply_tree(comments)
    comment_index = {c.comment_id: c for c in comments}
    chains: list[ReplyChain] = []

    visited: set[str] = set()

    for comment in comments:
        if comment.comment_id in visited:
            continue
        # A comment is a chain *start* unless it is the sole child of its parent.
        parent_children = adjacency.get(comment.parent_comment_id, [])
        is_chain_continuation = len(parent_children) == 1 and comment.parent_comment_id is not None
        if is_chain_continuation:
            continue

        # Trace the chain forward.
        chain_ids: list[str] = []
        chain_authors: list[str] = []
        current = comment
        while True:
            chain_ids.append(current.comment_id)
            chain_authors.append(current.author.username)
            visited.add(current.comment_id)
            children = adjacency.get(current.comment_id, [])
            if len(children) != 1:
                break
            current = children[0]

        if len(chain_ids) >= min_length:
            depths = [comment_index[cid].depth for cid in chain_ids]
            chains.append(
                ReplyChain(
                    comment_ids=chain_ids,
                    authors=chain_authors,
                    depth_range=(min(depths), max(depths)),
                    length=len(chain_ids),
                )
            )

    return sorted(chains, key=lambda c: -c.length)


# ---------------------------------------------------------------------------
# Subtree Scoring
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScoredSubtree:
    root_comment_id: str
    root_author: str
    root_score: int
    subtree_size: int
    max_depth_below: int
    engagement_score: float  # root_score + log2(subtree_size) * max_depth_below


def score_subtrees(
    comments: list[Comment],
    min_subtree_size: int = 2,
) -> list[ScoredSubtree]:
    """Score comment subtrees by combining root score with structural depth.

    ``engagement_score = root_score + log2(subtree_size) * max_depth_below``
    """
    adjacency = build_reply_tree(comments)
    metrics = compute_tree_metrics(comments)

    scored: list[ScoredSubtree] = []
    for comment in comments:
        size = metrics.subtree_sizes.get(comment.comment_id, 1)
        if size < min_subtree_size:
            continue

        depth_below = _max_depth_below(adjacency, comment.comment_id, comment.depth)
        engagement = max(comment.score, 0) + math.log2(size) * depth_below

        scored.append(
            ScoredSubtree(
                root_comment_id=comment.comment_id,
                root_author=comment.author.username,
                root_score=comment.score,
                subtree_size=size,
                max_depth_below=depth_below,
                engagement_score=round(engagement, 2),
            )
        )

    return sorted(scored, key=lambda s: -s.engagement_score)


def _max_depth_below(
    adjacency: dict[str | None, list[Comment]],
    root_id: str,
    root_depth: int,
) -> int:
    """Return the maximum depth in the subtree rooted at *root_id*."""
    max_d = 0
    queue = [root_id]
    while queue:
        current_id = queue.pop()
        for child in adjacency.get(current_id, []):
            depth_below = child.depth - root_depth
            if depth_below > max_d:
                max_d = depth_below
            queue.append(child.comment_id)
    return max_d


# ---------------------------------------------------------------------------
# Conversation Pattern Detection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConversationPattern:
    pattern_type: str  # "consensus", "controversy", "monologue"
    root_comment_id: str
    comment_count: int
    score_mean: float
    score_variance: float
    distinct_authors: int


def detect_conversation_patterns(
    comments: list[Comment],
    min_subtree_size: int = 3,
) -> list[ConversationPattern]:
    """Detect conversation patterns in comment subtrees.

    - **consensus**: low score variance, multiple authors
    - **controversy**: high score variance, polarized reactions
    - **monologue**: single author dominating the subtree
    """
    adjacency = build_reply_tree(comments)
    metrics = compute_tree_metrics(comments)
    comment_index = {c.comment_id: c for c in comments}

    patterns: list[ConversationPattern] = []
    for comment in comments:
        size = metrics.subtree_sizes.get(comment.comment_id, 1)
        if size < min_subtree_size:
            continue

        subtree_comments = _collect_subtree(adjacency, comment_index, comment.comment_id)
        if len(subtree_comments) < min_subtree_size:
            continue

        scores = [c.score for c in subtree_comments]
        authors = {c.author.username for c in subtree_comments}
        mean = sum(scores) / len(scores)
        variance = sum((s - mean) ** 2 for s in scores) / len(scores)

        if len(authors) == 1:
            pattern_type = "monologue"
        elif variance > 10:
            pattern_type = "controversy"
        else:
            pattern_type = "consensus"

        patterns.append(
            ConversationPattern(
                pattern_type=pattern_type,
                root_comment_id=comment.comment_id,
                comment_count=len(subtree_comments),
                score_mean=round(mean, 2),
                score_variance=round(variance, 2),
                distinct_authors=len(authors),
            )
        )

    return sorted(patterns, key=lambda p: (-p.comment_count, p.root_comment_id))


def _collect_subtree(
    adjacency: dict[str | None, list[Comment]],
    comment_index: dict[str, Comment],
    root_id: str,
) -> list[Comment]:
    """Collect all comments in the subtree rooted at *root_id* (inclusive)."""
    root_comment = comment_index.get(root_id)
    if root_comment is None:
        return []

    result: list[Comment] = [root_comment]
    queue = [root_id]
    while queue:
        current_id = queue.pop()
        for child in adjacency.get(current_id, []):
            result.append(child)
            queue.append(child.comment_id)
    return result
