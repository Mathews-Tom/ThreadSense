from __future__ import annotations

from threadsense.models.canonical import AuthorRef, Comment
from threadsense.pipeline.strategies.keyword_heuristic import (
    _band_hashes,
    _build_candidate_pairs,
    _detect_duplicate_clusters_bruteforce,
    _detect_duplicate_clusters_minhash,
    _minhash_signature,
    _token_shingles,
    build_comment_signal,
    detect_duplicate_clusters,
)


def _comment(comment_id: str, body: str, score: int = 1) -> Comment:
    return Comment(
        thread_id="test",
        comment_id=comment_id,
        parent_comment_id=None,
        author=AuthorRef(username="user", source_author_id=None),
        body=body,
        score=score,
        created_utc=1.0,
        depth=0,
        permalink="https://example.com",
    )


def test_token_shingles_generates_character_ngrams() -> None:
    shingles = _token_shingles(("hello", "world"), shingle_size=3)
    assert "hel" in shingles
    assert "llo" in shingles
    assert "o w" in shingles
    assert "wor" in shingles


def test_token_shingles_short_text_returns_full_text() -> None:
    shingles = _token_shingles(("ab",), shingle_size=3)
    assert shingles == {"ab"}


def test_minhash_signature_length() -> None:
    shingles = _token_shingles(("test", "document"))
    sig = _minhash_signature(shingles)
    assert len(sig) == 50


def test_minhash_signature_empty_shingles_returns_zeros() -> None:
    sig = _minhash_signature(set())
    assert sig == tuple(0 for _ in range(50))


def test_identical_texts_produce_identical_signatures() -> None:
    shingles = _token_shingles(("same", "text", "here"))
    sig1 = _minhash_signature(shingles)
    sig2 = _minhash_signature(shingles)
    assert sig1 == sig2


def test_band_hashes_produces_correct_count() -> None:
    shingles = _token_shingles(("test",))
    sig = _minhash_signature(shingles)
    bands = _band_hashes(sig)
    assert len(bands) == 10


def test_build_candidate_pairs_finds_duplicates() -> None:
    comments = [
        _comment("c1", "the docs are slow and confusing"),
        _comment("c2", "the docs are slow and confusing"),
        _comment("c3", "performance is great keep going"),
    ]
    signals = [build_comment_signal(c) for c in comments]
    pairs = _build_candidate_pairs(signals)
    assert ("c1", "c2") in pairs or ("c2", "c1") in pairs


def test_bruteforce_and_minhash_agree_on_exact_duplicates() -> None:
    comments = [
        _comment("c1", "the docs are slow and confusing"),
        _comment("c2", "the docs are slow and confusing"),
        _comment("c3", "performance is great keep going"),
        _comment("c4", "need better error messages please"),
        _comment("c5", "need better error messages please"),
    ]
    signals = [build_comment_signal(c) for c in comments]

    bruteforce = _detect_duplicate_clusters_bruteforce(signals, 0.88)
    minhash = _detect_duplicate_clusters_minhash(signals, 0.88)

    bf_sets = {frozenset(c.comment_ids) for c in bruteforce}
    mh_sets = {frozenset(c.comment_ids) for c in minhash}
    assert bf_sets == mh_sets


def test_detect_duplicate_clusters_small_set_uses_bruteforce() -> None:
    comments = [
        _comment("c1", "duplicate text here"),
        _comment("c2", "duplicate text here"),
        _comment("c3", "unique comment body"),
    ]
    signals = [build_comment_signal(c) for c in comments]
    clusters = detect_duplicate_clusters(signals, 0.88)
    assert len(clusters) == 1
    assert set(clusters[0].comment_ids) == {"c1", "c2"}


def test_no_duplicates_yields_empty_clusters() -> None:
    comments = [
        _comment("c1", "first unique comment"),
        _comment("c2", "second different comment"),
        _comment("c3", "third unrelated text"),
    ]
    signals = [build_comment_signal(c) for c in comments]
    clusters = detect_duplicate_clusters(signals, 0.88)
    assert clusters == []


def test_minhash_path_with_large_set() -> None:
    # Generate 60 comments (above threshold) with 5 pairs of duplicates
    comments: list[Comment] = []
    for i in range(50):
        comments.append(_comment(f"u{i:03d}", f"unique comment number {i} about topic {i}"))
    for i in range(5):
        body = f"this is a repeated comment about error handling group {i}"
        comments.append(_comment(f"d{i}a", body))
        comments.append(_comment(f"d{i}b", body))
    signals = [build_comment_signal(c) for c in comments]
    clusters = detect_duplicate_clusters(signals, 0.88)
    cluster_sets = {frozenset(c.comment_ids) for c in clusters}
    for i in range(5):
        assert frozenset({f"d{i}a", f"d{i}b"}) in cluster_sets
