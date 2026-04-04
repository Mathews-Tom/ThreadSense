from __future__ import annotations

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from threadsense.connectors.reddit import normalize_url
from threadsense.errors import RedditInputError
from threadsense.pipeline.strategies.keyword_heuristic import (
    canonicalize_text,
    clean_text,
    tokenize_text,
)

# ---------------------------------------------------------------------------
# normalize_url: arbitrary strings must never crash with an unhandled exception
# ---------------------------------------------------------------------------


@given(url=st.text(min_size=1, max_size=500))
@settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
def test_normalize_url_never_crashes_on_arbitrary_input(url: str) -> None:
    try:
        result = normalize_url(url)
        assert isinstance(result, str)
        assert result.endswith(".json")
    except RedditInputError:
        pass  # Expected for invalid URLs


# ---------------------------------------------------------------------------
# canonicalize_text: idempotence and output character constraints
# ---------------------------------------------------------------------------


@given(text=st.text(min_size=0, max_size=1000))
@settings(max_examples=200)
def test_canonicalize_text_is_idempotent(text: str) -> None:
    first = canonicalize_text(text)
    second = canonicalize_text(first)
    assert first == second


@given(text=st.text(min_size=1, max_size=1000))
@settings(max_examples=200)
def test_canonicalize_text_produces_lowercase_alphanumeric(text: str) -> None:
    result = canonicalize_text(text)
    if result:  # empty is valid
        assert result == result.lower()
        # Only alphanumeric and spaces
        assert all(c.isalnum() or c == " " for c in result)


# ---------------------------------------------------------------------------
# clean_text: whitespace normalization
# ---------------------------------------------------------------------------


@given(text=st.text(min_size=0, max_size=1000))
@settings(max_examples=200)
def test_clean_text_has_no_leading_trailing_whitespace(text: str) -> None:
    result = clean_text(text)
    assert result == result.strip()


@given(text=st.text(min_size=0, max_size=1000))
@settings(max_examples=200)
def test_clean_text_has_no_consecutive_whitespace(text: str) -> None:
    result = clean_text(text)
    assert "  " not in result


# ---------------------------------------------------------------------------
# tokenize_text: token consistency
# ---------------------------------------------------------------------------


@given(text=st.text(min_size=1, max_size=500))
@settings(max_examples=200)
def test_tokenize_text_produces_non_empty_tokens(text: str) -> None:
    tokens = tokenize_text(text)
    for token in tokens:
        assert token  # no empty tokens
        assert " " not in token  # no spaces in individual tokens
