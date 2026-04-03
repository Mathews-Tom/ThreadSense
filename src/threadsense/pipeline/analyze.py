from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from time import time

from threadsense.errors import AnalysisBoundaryError
from threadsense.models.analysis import (
    ANALYSIS_ENGINE_VERSION,
    ANALYSIS_SCHEMA_VERSION,
    AnalysisFinding,
    AnalysisProvenance,
    DuplicateGroup,
    RepresentativeQuote,
    ThreadAnalysis,
)
from threadsense.models.canonical import Comment, Thread
from threadsense.pipeline.storage import calculate_sha256, load_normalized_artifact

STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "but",
        "by",
        "for",
        "from",
        "has",
        "have",
        "i",
        "if",
        "in",
        "is",
        "it",
        "its",
        "of",
        "on",
        "or",
        "that",
        "the",
        "this",
        "to",
        "was",
        "with",
        "you",
        "your",
    }
)
ISSUE_MARKERS = ("bug", "broken", "confusing", "crash", "error", "fail", "issue", "lag", "slow")
REQUEST_MARKERS = ("can you", "could you", "please", "should", "would love", "need", "want")
SEVERITY_LEVELS = ("low", "medium", "high")
THEME_RULES = {
    "documentation": ("doc", "docs", "guide", "guides", "onboarding", "quickstart", "tutorial"),
    "performance": ("fast", "lag", "latency", "performance", "slow", "speed"),
    "reliability": ("bug", "broken", "crash", "error", "fail", "failure", "retry"),
    "workflow": ("automation", "batch", "dashboard", "export", "workflow"),
    "usability": ("confusing", "discover", "discoverability", "hard", "ux", "ui"),
}


@dataclass(frozen=True)
class CommentSignal:
    comment: Comment
    cleaned_text: str
    canonical_text: str
    tokens: tuple[str, ...]
    issue_marker_count: int
    request_marker_count: int
    theme_hits: dict[str, int]


@dataclass(frozen=True)
class DuplicateCluster:
    canonical_text: str
    comment_ids: list[str]


def analyze_thread_file(normalized_artifact_path: Path) -> ThreadAnalysis:
    thread = load_normalized_artifact(normalized_artifact_path)
    return analyze_thread(thread, normalized_artifact_path)


def analyze_thread(thread: Thread, normalized_artifact_path: Path) -> ThreadAnalysis:
    signals = [build_comment_signal(comment) for comment in thread.comments]
    duplicate_clusters = detect_duplicate_clusters(signals)
    duplicate_index = build_duplicate_index(duplicate_clusters)
    findings = build_findings(signals, duplicate_index)
    top_quotes = select_representative_quotes(signals, limit=5)
    return ThreadAnalysis(
        thread_id=thread.thread_id,
        source_name=thread.source.source_name,
        title=thread.title,
        total_comments=thread.comment_count,
        distinct_comment_count=count_distinct_comments(signals, duplicate_index),
        duplicate_group_count=len(duplicate_clusters),
        top_phrases=extract_top_phrases(signals, limit=8),
        findings=findings,
        duplicate_groups=[
            DuplicateGroup(
                canonical_text=cluster.canonical_text,
                comment_ids=cluster.comment_ids,
                count=len(cluster.comment_ids),
            )
            for cluster in duplicate_clusters
        ],
        top_quotes=top_quotes,
        provenance=AnalysisProvenance(
            normalized_artifact_path=str(normalized_artifact_path),
            normalized_sha256=calculate_sha256(normalized_artifact_path),
            source_thread_id=thread.source.source_thread_id,
            analyzed_at_utc=time(),
            schema_version=ANALYSIS_SCHEMA_VERSION,
            analysis_version=ANALYSIS_ENGINE_VERSION,
        ),
    )


def build_comment_signal(comment: Comment) -> CommentSignal:
    cleaned_text = clean_text(comment.body)
    canonical_text = canonicalize_text(cleaned_text)
    if not canonical_text:
        raise AnalysisBoundaryError(
            "comment body cannot be reduced to empty analysis text",
            details={"comment_id": comment.comment_id},
        )
    tokens = tokenize_text(cleaned_text)
    theme_hits = {
        theme: sum(1 for token in tokens if token in keywords)
        for theme, keywords in THEME_RULES.items()
    }
    return CommentSignal(
        comment=comment,
        cleaned_text=cleaned_text,
        canonical_text=canonical_text,
        tokens=tokens,
        issue_marker_count=count_markers(canonical_text, ISSUE_MARKERS),
        request_marker_count=count_markers(canonical_text, REQUEST_MARKERS),
        theme_hits=theme_hits,
    )


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def canonicalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text.lower())).strip()


def tokenize_text(text: str) -> tuple[str, ...]:
    canonical_text = canonicalize_text(text)
    if not canonical_text:
        return ()
    return tuple(token for token in canonical_text.split(" ") if token)


def count_markers(canonical_text: str, markers: tuple[str, ...]) -> int:
    return sum(canonical_text.count(marker) for marker in markers)


def extract_top_phrases(signals: list[CommentSignal], limit: int) -> list[str]:
    counter: Counter[str] = Counter()
    phrase_weight: Counter[str] = Counter()
    seen_canonical_texts: set[str] = set()
    for signal in signals:
        if signal.canonical_text in seen_canonical_texts:
            continue
        seen_canonical_texts.add(signal.canonical_text)
        filtered = [token for token in signal.tokens if token not in STOPWORDS and len(token) > 2]
        for size in (2, 3):
            for index in range(0, len(filtered) - size + 1):
                phrase = " ".join(filtered[index : index + size])
                counter[phrase] += 1
                phrase_weight[phrase] += max(signal.comment.score, 0)
    ranked = sorted(
        counter.items(),
        key=lambda item: (-item[1], -phrase_weight[item[0]], item[0]),
    )
    return [phrase for phrase, _count in ranked[:limit]]


def detect_duplicate_clusters(signals: list[CommentSignal]) -> list[DuplicateCluster]:
    clusters: list[DuplicateCluster] = []
    seen: set[str] = set()
    ordered_signals = sorted(signals, key=lambda signal: signal.comment.comment_id)
    for signal in ordered_signals:
        if signal.comment.comment_id in seen:
            continue
        cluster_ids = [signal.comment.comment_id]
        seen.add(signal.comment.comment_id)
        for candidate in ordered_signals:
            if candidate.comment.comment_id in seen:
                continue
            if are_near_duplicates(signal, candidate):
                cluster_ids.append(candidate.comment.comment_id)
                seen.add(candidate.comment.comment_id)
        if len(cluster_ids) > 1:
            clusters.append(
                DuplicateCluster(
                    canonical_text=signal.canonical_text,
                    comment_ids=sorted(cluster_ids),
                )
            )
    return clusters


def are_near_duplicates(left: CommentSignal, right: CommentSignal) -> bool:
    if left.canonical_text == right.canonical_text:
        return True
    left_tokens = set(left.tokens)
    right_tokens = set(right.tokens)
    if not left_tokens or not right_tokens:
        return False
    union = left_tokens | right_tokens
    overlap = left_tokens & right_tokens
    return len(overlap) / len(union) >= 0.88


def build_duplicate_index(clusters: list[DuplicateCluster]) -> dict[str, str]:
    index: dict[str, str] = {}
    for cluster in clusters:
        representative_id = cluster.comment_ids[0]
        for comment_id in cluster.comment_ids:
            index[comment_id] = representative_id
    return index


def count_distinct_comments(
    signals: list[CommentSignal],
    duplicate_index: dict[str, str],
) -> int:
    distinct_ids = {
        duplicate_index.get(
            signal.comment.comment_id,
            signal.comment.comment_id,
        )
        for signal in signals
    }
    return len(distinct_ids)


def build_findings(
    signals: list[CommentSignal],
    duplicate_index: dict[str, str],
) -> list[AnalysisFinding]:
    grouped: dict[str, list[CommentSignal]] = {}
    for signal in signals:
        grouped.setdefault(classify_theme(signal), []).append(signal)

    findings: list[AnalysisFinding] = []
    for theme_key, theme_signals in grouped.items():
        evidence = dedupe_signals(theme_signals, duplicate_index)
        issue_marker_count = sum(signal.issue_marker_count for signal in evidence)
        request_marker_count = sum(signal.request_marker_count for signal in evidence)
        findings.append(
            AnalysisFinding(
                theme_key=theme_key,
                theme_label=theme_key.replace("_", " "),
                severity=score_severity(evidence, issue_marker_count, request_marker_count),
                comment_count=len(evidence),
                issue_marker_count=issue_marker_count,
                request_marker_count=request_marker_count,
                key_phrases=extract_top_phrases(evidence, limit=5),
                evidence_comment_ids=sorted(signal.comment.comment_id for signal in evidence),
                quotes=select_representative_quotes(evidence, limit=3),
            )
        )

    return sorted(
        findings,
        key=lambda finding: (
            -SEVERITY_LEVELS.index(finding.severity),
            -finding.issue_marker_count,
            -finding.request_marker_count,
            -finding.comment_count,
            finding.theme_key,
        ),
    )


def classify_theme(signal: CommentSignal) -> str:
    ranked_theme = max(
        signal.theme_hits.items(),
        key=lambda item: (item[1], item[0]),
    )
    if ranked_theme[1] > 0:
        return ranked_theme[0]
    if signal.issue_marker_count > 0:
        return "reliability"
    if signal.request_marker_count > 0:
        return "workflow"
    return "general_feedback"


def dedupe_signals(
    signals: list[CommentSignal],
    duplicate_index: dict[str, str],
) -> list[CommentSignal]:
    by_representative: dict[str, CommentSignal] = {}
    for signal in signals:
        representative_id = duplicate_index.get(
            signal.comment.comment_id,
            signal.comment.comment_id,
        )
        existing = by_representative.get(representative_id)
        if existing is None or rank_comment_signal(signal) > rank_comment_signal(existing):
            by_representative[representative_id] = signal
    return sorted(by_representative.values(), key=lambda signal: signal.comment.comment_id)


def score_severity(
    signals: list[CommentSignal],
    issue_marker_count: int,
    request_marker_count: int,
) -> str:
    weighted_score = (
        issue_marker_count * 3
        + request_marker_count
        + sum(max(signal.comment.score, 0) for signal in signals)
    )
    if weighted_score >= 15:
        return "high"
    if weighted_score >= 6:
        return "medium"
    return "low"


def rank_comment_signal(signal: CommentSignal) -> tuple[int, int, int, str]:
    weighted_score = (
        signal.issue_marker_count * 3
        + signal.request_marker_count * 2
        + max(signal.comment.score, 0)
    )
    return (
        weighted_score,
        len(signal.cleaned_text),
        -signal.comment.depth,
        signal.comment.comment_id,
    )


def select_representative_quotes(
    signals: list[CommentSignal],
    limit: int,
) -> list[RepresentativeQuote]:
    ranked = sorted(
        signals,
        key=lambda signal: (
            -rank_comment_signal(signal)[0],
            -rank_comment_signal(signal)[1],
            rank_comment_signal(signal)[2],
            signal.comment.comment_id,
        ),
    )
    return [to_quote(signal) for signal in ranked[:limit]]


def to_quote(signal: CommentSignal) -> RepresentativeQuote:
    excerpt = signal.cleaned_text[:220]
    if len(signal.cleaned_text) > 220:
        excerpt = f"{excerpt.rstrip()}..."
    return RepresentativeQuote(
        comment_id=signal.comment.comment_id,
        permalink=signal.comment.permalink,
        author=signal.comment.author.username,
        body_excerpt=excerpt,
        score=signal.comment.score,
    )
