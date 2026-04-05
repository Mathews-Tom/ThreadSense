from __future__ import annotations

import hashlib
import re
from collections import Counter, deque
from dataclasses import dataclass

from threadsense.contracts import AbstractionLevel, AnalysisContract, ObjectiveType
from threadsense.domains import DomainVocabulary, load_domain_vocabulary
from threadsense.models.analysis import (
    AnalysisFinding,
    DuplicateGroup,
    RepresentativeQuote,
)
from threadsense.models.canonical import Comment, Thread
from threadsense.pipeline.strategies import AnalysisResult

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
_PLATFORM_NOISE = frozenset(
    {
        "lol",
        "imo",
        "tbh",
        "iirc",
        "fwiw",
        "afaik",
        "tldr",
        "edit",
        "deleted",
        "removed",
        "http",
        "https",
        "www",
        "com",
        "org",
    }
)
STOPWORDS = STOPWORDS | _PLATFORM_NOISE
DEFAULT_DOMAIN_VOCABULARY = load_domain_vocabulary("developer_tools")


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


class KeywordHeuristicStrategy:
    """Deterministic keyword-matching heuristic baseline."""

    def __init__(
        self,
        duplicate_threshold: float = 0.88,
        vocabulary: DomainVocabulary = DEFAULT_DOMAIN_VOCABULARY,
    ) -> None:
        self._duplicate_threshold = duplicate_threshold
        self._vocabulary = vocabulary

    def analyze(self, thread: Thread, contract: AnalysisContract) -> AnalysisResult:
        all_signals = [
            signal
            for signal in (
                build_comment_signal(
                    comment,
                    theme_rules=self._vocabulary.theme_rules,
                    issue_markers=self._vocabulary.issue_markers,
                    request_markers=self._vocabulary.request_markers,
                )
                for comment in thread.comments
            )
            if signal is not None
        ]
        signals = [s for s in all_signals if not is_noise_signal(s)]
        noise_count = len(all_signals) - len(signals)
        duplicate_clusters = detect_duplicate_clusters(signals, self._duplicate_threshold)
        duplicate_index = build_duplicate_index(duplicate_clusters)
        findings = build_findings(
            signals,
            duplicate_index,
            contract=contract,
            severity_levels=self._vocabulary.severity_levels,
            issue_fallback_theme=self._vocabulary.issue_fallback_theme,
            request_fallback_theme=self._vocabulary.request_fallback_theme,
            default_theme=self._vocabulary.default_theme,
        )
        findings = route_findings_by_abstraction(findings, contract)
        top_quotes = select_representative_quotes(
            signals,
            limit=quote_limit_for_contract(contract),
        )
        if contract.abstraction_level is AbstractionLevel.STRATEGIC:
            top_quotes = [quote for quote in top_quotes if quote.score >= 0][:3]
        empty_text_count = thread.comment_count - len(all_signals)
        return AnalysisResult(
            filtered_comment_count=empty_text_count + noise_count,
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
        )


def build_comment_signal(
    comment: Comment,
    *,
    theme_rules: dict[str, tuple[str, ...]] = DEFAULT_DOMAIN_VOCABULARY.theme_rules,
    issue_markers: tuple[str, ...] = DEFAULT_DOMAIN_VOCABULARY.issue_markers,
    request_markers: tuple[str, ...] = DEFAULT_DOMAIN_VOCABULARY.request_markers,
) -> CommentSignal | None:
    cleaned_text = clean_text(comment.body)
    canonical_text = canonicalize_text(cleaned_text)
    if not canonical_text:
        return None
    tokens = tokenize_text(cleaned_text)
    theme_hits = {
        theme: sum(1 for token in tokens if token in keywords)
        for theme, keywords in theme_rules.items()
    }
    return CommentSignal(
        comment=comment,
        cleaned_text=cleaned_text,
        canonical_text=canonical_text,
        tokens=tokens,
        issue_marker_count=count_markers(canonical_text, issue_markers),
        request_marker_count=count_markers(canonical_text, request_markers),
        theme_hits=theme_hits,
    )


_URL_PATTERN = re.compile(r"https?://\S+|www\.\S+")

# ---------------------------------------------------------------------------
# Noise detection — low-signal comments excluded before classification
# ---------------------------------------------------------------------------

_NOISE_MIN_BODY_LENGTH = 50
_NOISE_MIN_UNIQUE_TOKENS = 3
_NOISE_ACK_PATTERNS = frozenset(
    {
        "thanks",
        "thank you",
        "this",
        "same",
        "agreed",
        "interesting",
        "cool",
        "nice",
    }
)
_NOISE_BOT_PATTERN = re.compile(
    r"remind\s*me.*\d+\s*(day|week|month|hour)"
    r"|i will be messaging you"
    r"|i'm a bot"
    r"|bot account",
    re.IGNORECASE,
)


def is_noise_signal(signal: CommentSignal) -> bool:
    """Return True if the signal carries too little content for classification."""
    if _NOISE_BOT_PATTERN.search(signal.cleaned_text):
        return True
    stripped = signal.canonical_text.strip()
    if stripped in _NOISE_ACK_PATTERNS:
        return True
    unique_tokens = len(set(signal.tokens) - STOPWORDS)
    if (
        len(signal.cleaned_text) < _NOISE_MIN_BODY_LENGTH
        and unique_tokens < _NOISE_MIN_UNIQUE_TOKENS
        and signal.issue_marker_count == 0
        and signal.request_marker_count == 0
    ):
        return True
    return False


def clean_text(text: str) -> str:
    text = _URL_PATTERN.sub("", text)
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


_UNIGRAM_MIN_LENGTH = 5
_UNIGRAM_MIN_FREQUENCY = 3


def extract_top_phrases(signals: list[CommentSignal], limit: int) -> list[str]:
    counter: Counter[str] = Counter()
    phrase_weight: Counter[str] = Counter()
    unigram_counter: Counter[str] = Counter()
    unigram_weight: Counter[str] = Counter()
    seen_canonical_texts: set[str] = set()
    for signal in signals:
        if signal.canonical_text in seen_canonical_texts:
            continue
        seen_canonical_texts.add(signal.canonical_text)
        filtered = [token for token in signal.tokens if token not in STOPWORDS and len(token) > 2]
        score = max(signal.comment.score, 0)
        for size in (2, 3):
            for index in range(0, len(filtered) - size + 1):
                phrase = " ".join(filtered[index : index + size])
                counter[phrase] += 1
                phrase_weight[phrase] += score
        for token in filtered:
            if len(token) >= _UNIGRAM_MIN_LENGTH:
                unigram_counter[token] += 1
                unigram_weight[token] += score
    for token, count in unigram_counter.items():
        if count >= _UNIGRAM_MIN_FREQUENCY:
            counter[token] = count
            phrase_weight[token] = unigram_weight[token]
    ranked = sorted(
        counter.items(),
        key=lambda item: (-item[1], -phrase_weight[item[0]], item[0]),
    )
    return [phrase for phrase, _count in ranked[:limit]]


# ---------------------------------------------------------------------------
# MinHash / LSH constants — tuned for threshold ~0.88
# With b=10 bands and r=5 rows per band (50 hash functions total):
#   P(candidate | jaccard=0.88) ≈ 1 - (1 - 0.88^5)^10 ≈ 0.9997
#   P(candidate | jaccard=0.50) ≈ 1 - (1 - 0.50^5)^10 ≈ 0.28
# ---------------------------------------------------------------------------
MINHASH_NUM_HASHES = 50
MINHASH_BANDS = 10
MINHASH_ROWS_PER_BAND = MINHASH_NUM_HASHES // MINHASH_BANDS
_HASH_SEEDS: tuple[int, ...] = tuple(range(MINHASH_NUM_HASHES))
_MINHASH_SIZE_THRESHOLD = 50


def _token_shingles(tokens: tuple[str, ...], shingle_size: int = 3) -> set[str]:
    """Generate character n-gram shingles from tokens."""
    text = " ".join(tokens)
    if len(text) < shingle_size:
        return {text}
    return {text[i : i + shingle_size] for i in range(len(text) - shingle_size + 1)}


def _minhash_signature(shingles: set[str]) -> tuple[int, ...]:
    """Compute MinHash signature with *k* hash functions."""
    if not shingles:
        return tuple(0 for _ in range(MINHASH_NUM_HASHES))
    signature: list[int] = []
    for seed in _HASH_SEEDS:
        min_hash = float("inf")
        for shingle in shingles:
            h = int(
                hashlib.md5(f"{seed}:{shingle}".encode(), usedforsecurity=False).hexdigest()[:8],
                16,
            )
            if h < min_hash:
                min_hash = h
        signature.append(int(min_hash))
    return tuple(signature)


def _band_hashes(signature: tuple[int, ...]) -> tuple[int, ...]:
    """Split signature into bands and hash each band."""
    bands: list[int] = []
    for band_idx in range(MINHASH_BANDS):
        start = band_idx * MINHASH_ROWS_PER_BAND
        end = start + MINHASH_ROWS_PER_BAND
        bands.append(hash(signature[start:end]))
    return tuple(bands)


def _build_candidate_pairs(
    signals: list[CommentSignal],
) -> set[tuple[str, str]]:
    """Use MinHash + LSH to identify candidate near-duplicate pairs."""
    band_map: dict[str, tuple[int, ...]] = {}
    for signal in signals:
        shingles = _token_shingles(signal.tokens)
        sig = _minhash_signature(shingles)
        band_map[signal.comment.comment_id] = _band_hashes(sig)

    buckets: dict[tuple[int, int], list[str]] = {}
    for comment_id, bands in band_map.items():
        for band_idx, band_hash in enumerate(bands):
            bucket_key = (band_idx, band_hash)
            buckets.setdefault(bucket_key, []).append(comment_id)

    candidates: set[tuple[str, str]] = set()
    for bucket_members in buckets.values():
        if len(bucket_members) < 2:
            continue
        for i in range(len(bucket_members)):
            for j in range(i + 1, len(bucket_members)):
                pair = (
                    min(bucket_members[i], bucket_members[j]),
                    max(bucket_members[i], bucket_members[j]),
                )
                candidates.add(pair)
    return candidates


# ---------------------------------------------------------------------------
# Duplicate cluster detection — dispatcher + implementations
# ---------------------------------------------------------------------------


def detect_duplicate_clusters(
    signals: list[CommentSignal], threshold: float
) -> list[DuplicateCluster]:
    if len(signals) < _MINHASH_SIZE_THRESHOLD:
        return _detect_duplicate_clusters_bruteforce(signals, threshold)
    return _detect_duplicate_clusters_minhash(signals, threshold)


def _detect_duplicate_clusters_bruteforce(
    signals: list[CommentSignal], threshold: float
) -> list[DuplicateCluster]:
    """O(n²) brute-force — efficient for small sets."""
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
            if are_near_duplicates(signal, candidate, threshold):
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


def _detect_duplicate_clusters_minhash(
    signals: list[CommentSignal], threshold: float
) -> list[DuplicateCluster]:
    """MinHash-accelerated duplicate detection for large comment sets."""
    signal_index = {signal.comment.comment_id: signal for signal in signals}
    candidates = _build_candidate_pairs(signals)

    # Build adjacency from verified candidates
    adjacency: dict[str, set[str]] = {}
    for id_a, id_b in candidates:
        sig_a = signal_index[id_a]
        sig_b = signal_index[id_b]
        if are_near_duplicates(sig_a, sig_b, threshold):
            adjacency.setdefault(id_a, set()).add(id_b)
            adjacency.setdefault(id_b, set()).add(id_a)

    # Exact canonical-text duplicates (guaranteed matches MinHash might band differently)
    canonical_groups: dict[str, list[str]] = {}
    for signal in signals:
        canonical_groups.setdefault(signal.canonical_text, []).append(signal.comment.comment_id)
    for group_ids in canonical_groups.values():
        if len(group_ids) > 1:
            for i in range(len(group_ids)):
                for j in range(i + 1, len(group_ids)):
                    adjacency.setdefault(group_ids[i], set()).add(group_ids[j])
                    adjacency.setdefault(group_ids[j], set()).add(group_ids[i])

    # Connected components via BFS
    clusters: list[DuplicateCluster] = []
    visited: set[str] = set()
    for signal in sorted(signals, key=lambda s: s.comment.comment_id):
        cid = signal.comment.comment_id
        if cid in visited or cid not in adjacency:
            continue
        component: list[str] = []
        queue: deque[str] = deque([cid])
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            component.append(current)
            for neighbor in adjacency.get(current, set()):
                if neighbor not in visited:
                    queue.append(neighbor)
        if len(component) > 1:
            representative = signal_index[sorted(component)[0]]
            clusters.append(
                DuplicateCluster(
                    canonical_text=representative.canonical_text,
                    comment_ids=sorted(component),
                )
            )
    return clusters


def are_near_duplicates(left: CommentSignal, right: CommentSignal, threshold: float) -> bool:
    if left.canonical_text == right.canonical_text:
        return True
    left_tokens = set(left.tokens)
    right_tokens = set(right.tokens)
    if not left_tokens or not right_tokens:
        return False
    union = left_tokens | right_tokens
    overlap = left_tokens & right_tokens
    return len(overlap) / len(union) >= threshold


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
        representative_comment_id(signal.comment.comment_id, duplicate_index) for signal in signals
    }
    return len(distinct_ids)


_FEEDBACK_DECOMPOSE_RATIO = 0.50
_FEEDBACK_CLUSTER_THRESHOLD = 0.30
_FEEDBACK_MIN_CLUSTER_SIZE = 3


def decompose_catch_all(
    signals: list[CommentSignal],
    total_signal_count: int,
    default_theme: str,
    *,
    ratio_threshold: float = _FEEDBACK_DECOMPOSE_RATIO,
    similarity_threshold: float = _FEEDBACK_CLUSTER_THRESHOLD,
    min_cluster_size: int = _FEEDBACK_MIN_CLUSTER_SIZE,
) -> list[tuple[str, list[CommentSignal]]]:
    """Sub-cluster the default theme when it dominates the signal set.

    Returns a list of (sub_theme_key, signals) pairs. Signals that don't
    form clusters large enough stay in the original default theme bucket.
    """
    if len(signals) / max(total_signal_count, 1) < ratio_threshold:
        return [(default_theme, signals)]

    clusters = _cluster_signals(signals, similarity_threshold, min_cluster_size)
    if not clusters:
        return [(default_theme, signals)]

    assigned_ids: set[str] = set()
    result: list[tuple[str, list[CommentSignal]]] = []
    for cluster_signals in clusters:
        label = _cluster_label(cluster_signals)
        sub_key = f"{default_theme}.{label}"
        result.append((sub_key, cluster_signals))
        assigned_ids.update(s.comment.comment_id for s in cluster_signals)

    remainder = [s for s in signals if s.comment.comment_id not in assigned_ids]
    if remainder:
        result.append((default_theme, remainder))
    return result


def _cluster_signals(
    signals: list[CommentSignal],
    threshold: float,
    min_size: int,
) -> list[list[CommentSignal]]:
    """Single-linkage clustering via Jaccard similarity on token sets."""
    adjacency: dict[str, set[str]] = {}
    for i, left in enumerate(signals):
        left_tokens = set(left.tokens)
        if not left_tokens:
            continue
        for right in signals[i + 1 :]:
            right_tokens = set(right.tokens)
            if not right_tokens:
                continue
            union = left_tokens | right_tokens
            overlap = left_tokens & right_tokens
            if len(overlap) / len(union) >= threshold:
                adjacency.setdefault(left.comment.comment_id, set()).add(right.comment.comment_id)
                adjacency.setdefault(right.comment.comment_id, set()).add(left.comment.comment_id)

    signal_index = {s.comment.comment_id: s for s in signals}
    visited: set[str] = set()
    clusters: list[list[CommentSignal]] = []
    for signal in signals:
        cid = signal.comment.comment_id
        if cid in visited or cid not in adjacency:
            continue
        component: list[str] = []
        queue: deque[str] = deque([cid])
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            component.append(current)
            for neighbor in adjacency.get(current, set()):
                if neighbor not in visited:
                    queue.append(neighbor)
        if len(component) >= min_size:
            clusters.append([signal_index[c] for c in sorted(component)])
    return clusters


def _cluster_label(signals: list[CommentSignal]) -> str:
    phrases = extract_top_phrases(signals, limit=1)
    if phrases:
        return phrases[0].replace(" ", "_")
    return "misc"


def build_findings(
    signals: list[CommentSignal],
    duplicate_index: dict[str, str],
    *,
    contract: AnalysisContract,
    severity_levels: tuple[str, ...] = DEFAULT_DOMAIN_VOCABULARY.severity_levels,
    issue_fallback_theme: str = DEFAULT_DOMAIN_VOCABULARY.issue_fallback_theme,
    request_fallback_theme: str = DEFAULT_DOMAIN_VOCABULARY.request_fallback_theme,
    default_theme: str = DEFAULT_DOMAIN_VOCABULARY.default_theme,
) -> list[AnalysisFinding]:
    grouped: dict[str, list[CommentSignal]] = {}
    for signal in signals:
        grouped.setdefault(
            classify_theme(
                signal,
                issue_fallback_theme=issue_fallback_theme,
                request_fallback_theme=request_fallback_theme,
                default_theme=default_theme,
            ),
            [],
        ).append(signal)

    if default_theme in grouped:
        sub_groups = decompose_catch_all(
            grouped.pop(default_theme),
            total_signal_count=len(signals),
            default_theme=default_theme,
        )
        for sub_key, sub_signals in sub_groups:
            grouped[sub_key] = sub_signals

    findings: list[AnalysisFinding] = []
    for theme_key, theme_signals in grouped.items():
        evidence = dedupe_signals(theme_signals, duplicate_index)
        issue_marker_count = sum(signal.issue_marker_count for signal in evidence)
        request_marker_count = sum(signal.request_marker_count for signal in evidence)
        findings.append(
            AnalysisFinding(
                theme_key=theme_key,
                theme_label=theme_key.replace("_", " "),
                severity=score_severity(
                    evidence,
                    issue_marker_count,
                    request_marker_count,
                    contract=contract,
                    severity_levels=severity_levels,
                ),
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
            -severity_levels.index(finding.severity),
            -finding.issue_marker_count,
            -finding.request_marker_count,
            -finding.comment_count,
            finding.theme_key,
        ),
    )


def classify_theme(
    signal: CommentSignal,
    *,
    issue_fallback_theme: str = DEFAULT_DOMAIN_VOCABULARY.issue_fallback_theme,
    request_fallback_theme: str = DEFAULT_DOMAIN_VOCABULARY.request_fallback_theme,
    default_theme: str = DEFAULT_DOMAIN_VOCABULARY.default_theme,
) -> str:
    ranked_theme = max(
        signal.theme_hits.items(),
        key=lambda item: (item[1], item[0]),
    )
    if ranked_theme[1] > 0:
        return ranked_theme[0]
    if signal.issue_marker_count > 0:
        return issue_fallback_theme
    if signal.request_marker_count > 0:
        return request_fallback_theme
    return default_theme


def dedupe_signals(
    signals: list[CommentSignal],
    duplicate_index: dict[str, str],
) -> list[CommentSignal]:
    by_representative: dict[str, CommentSignal] = {}
    for signal in signals:
        representative_id = representative_comment_id(signal.comment.comment_id, duplicate_index)
        existing = by_representative.get(representative_id)
        if existing is None or rank_comment_signal(signal) > rank_comment_signal(existing):
            by_representative[representative_id] = signal
    return sorted(by_representative.values(), key=lambda signal: signal.comment.comment_id)


_SEVERITY_VOLUME_CAP = 10


def score_severity(
    signals: list[CommentSignal],
    issue_marker_count: int,
    request_marker_count: int,
    *,
    contract: AnalysisContract,
    severity_levels: tuple[str, ...] = DEFAULT_DOMAIN_VOCABULARY.severity_levels,
) -> str:
    raw_score = (
        issue_marker_count * 3
        + request_marker_count
        + sum(max(signal.comment.score, 0) for signal in signals)
    )
    comment_count = max(len(signals), 1)
    density = raw_score / comment_count
    weighted_score = density * min(comment_count, _SEVERITY_VOLUME_CAP)
    high_threshold = 15
    medium_threshold = 6
    if contract.objective is ObjectiveType.FEATURE_DEMAND:
        high_threshold += 2
    if contract.abstraction_level is AbstractionLevel.STRATEGIC:
        high_threshold += 6
        medium_threshold += 4
    elif contract.abstraction_level is AbstractionLevel.ARCHITECTURAL:
        high_threshold += 2
        medium_threshold += 2
    if weighted_score >= high_threshold:
        return severity_levels[2]
    if weighted_score >= medium_threshold:
        return severity_levels[1]
    return severity_levels[0]


def quote_limit_for_contract(contract: AnalysisContract) -> int:
    if contract.abstraction_level is AbstractionLevel.STRATEGIC:
        return 3
    if contract.abstraction_level is AbstractionLevel.ARCHITECTURAL:
        return 4
    return 5


def route_findings_by_abstraction(
    findings: list[AnalysisFinding],
    contract: AnalysisContract,
) -> list[AnalysisFinding]:
    if contract.abstraction_level is AbstractionLevel.OPERATIONAL:
        return findings
    if contract.abstraction_level is AbstractionLevel.ARCHITECTURAL:
        return merge_architectural_findings(findings)
    return [finding for finding in findings if finding.severity == "high"][:3]


def merge_architectural_findings(findings: list[AnalysisFinding]) -> list[AnalysisFinding]:
    if not findings:
        return []
    merged_groups: dict[str, list[AnalysisFinding]] = {}
    for finding in findings:
        merged_groups.setdefault(_architectural_group(finding.theme_key), []).append(finding)

    merged: list[AnalysisFinding] = []
    for theme_key, group in merged_groups.items():
        comment_count = sum(finding.comment_count for finding in group)
        issue_count = sum(finding.issue_marker_count for finding in group)
        request_count = sum(finding.request_marker_count for finding in group)
        quotes = sorted(
            [quote for finding in group for quote in finding.quotes],
            key=lambda quote: (-quote.score, quote.comment_id),
        )[:3]
        key_phrases = list(
            dict.fromkeys(phrase for finding in group for phrase in finding.key_phrases)
        )[:5]
        evidence_ids = sorted(
            set(comment_id for finding in group for comment_id in finding.evidence_comment_ids)
        )
        severity = "high" if any(f.severity == "high" for f in group) else "medium"
        merged.append(
            AnalysisFinding(
                theme_key=theme_key,
                theme_label=theme_key.replace("_", " "),
                severity=severity,
                comment_count=comment_count,
                issue_marker_count=issue_count,
                request_marker_count=request_count,
                key_phrases=key_phrases,
                evidence_comment_ids=evidence_ids,
                quotes=quotes,
            )
        )
    return sorted(
        merged,
        key=lambda finding: (
            -finding.comment_count,
            -finding.issue_marker_count,
            finding.theme_key,
        ),
    )


def _architectural_group(theme_key: str) -> str:
    if theme_key in {"performance", "reliability"}:
        return "system_health"
    if theme_key in {"documentation", "workflow", "usability"}:
        return "experience_design"
    return theme_key


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


def representative_comment_id(comment_id: str, duplicate_index: dict[str, str]) -> str:
    return duplicate_index.get(comment_id, comment_id)


_QUOTE_SCORE_CAP = 20
_QUOTE_LENGTH_CAP = 500
_QUOTE_TOKEN_CAP = 30


def representative_quote_sort_key(signal: CommentSignal) -> tuple[float, int, str]:
    score_normalized = min(max(signal.comment.score, 0), _QUOTE_SCORE_CAP) / _QUOTE_SCORE_CAP
    length_normalized = min(len(signal.cleaned_text), _QUOTE_LENGTH_CAP) / _QUOTE_LENGTH_CAP
    unique_tokens = len(set(signal.tokens) - STOPWORDS)
    density_normalized = min(unique_tokens, _QUOTE_TOKEN_CAP) / _QUOTE_TOKEN_CAP
    composite = score_normalized * 0.3 + length_normalized * 0.35 + density_normalized * 0.35
    return (-composite, -len(signal.cleaned_text), signal.comment.comment_id)


def select_representative_quotes(
    signals: list[CommentSignal],
    limit: int,
) -> list[RepresentativeQuote]:
    ranked = sorted(signals, key=representative_quote_sort_key)
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
