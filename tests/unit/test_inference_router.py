from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from threadsense.config import load_config
from threadsense.contracts import DomainType
from threadsense.domains import load_domain_vocabulary, merge_vocabulary_expansion
from threadsense.errors import InferenceBoundaryError
from threadsense.inference import InferenceRouter, InferenceTask
from threadsense.inference.contracts import validate_task_output
from threadsense.inference.prompts import render_analysis_payload
from threadsense.inference.router import InferenceClient
from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.models.canonical import (
    AuthorRef,
    Comment,
    ProvenanceMetadata,
    SourceRef,
    Thread,
    load_canonical_thread,
)
from threadsense.models.corpus import TrendPeriod
from threadsense.pipeline.analyze import analyze_thread
from threadsense.pipeline.corpus import build_corpus_analysis, build_corpus_manifest


def load_analysis_fixture(tmp_path: Path) -> Path:
    canonical_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    thread = load_canonical_thread(canonical_path)
    analysis = analyze_thread(thread, canonical_path)
    analysis_path = tmp_path / "analysis.json"
    analysis_path.write_text(json.dumps(analysis.to_dict()), encoding="utf-8")
    return analysis_path


def test_validate_task_output_accepts_analysis_summary_shape() -> None:
    payload = validate_task_output(
        InferenceTask.ANALYSIS_SUMMARY,
        {
            "headline": "Performance dominates",
            "summary": "Latency and docs issues lead the thread.",
            "cited_theme_keys": ["performance", "documentation"],
            "cited_comment_ids": ["reddit:c3", "reddit:c1"],
            "next_steps": ["Profile search", "Expand onboarding docs"],
        },
    )

    assert payload["headline"] == "Performance dominates"


def test_router_returns_deterministic_fallback_when_runtime_is_disabled(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    config = load_config(
        env={
            "THREADSENSE_RUNTIME_ENABLED": "false",
            "THREADSENSE_RUNTIME_MODEL": "local-model",
        }
    )

    response = InferenceRouter(config).run_analysis_task(
        analysis=analysis,
        task=InferenceTask.ANALYSIS_SUMMARY,
        required=False,
    )

    assert response.used_fallback is True
    assert response.provider == "deterministic_fallback"
    assert response.output["cited_theme_keys"]


def test_router_fails_when_runtime_is_disabled_for_required_task(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    config = load_config(env={"THREADSENSE_RUNTIME_ENABLED": "false"})

    with pytest.raises(InferenceBoundaryError):
        InferenceRouter(config).run_analysis_task(
            analysis=analysis,
            task=InferenceTask.ANALYSIS_SUMMARY,
            required=True,
        )


def test_router_falls_back_when_client_errors_for_optional_task(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    config = load_config(env={})

    class FailingClient:
        def complete(self, request: object, **kwargs: object) -> object:
            raise InferenceBoundaryError("runtime failed")

    def failing_client_factory(app_config: object) -> InferenceClient:
        return cast(InferenceClient, FailingClient())

    response = InferenceRouter(
        config,
        client_factory=failing_client_factory,
    ).run_analysis_task(
        analysis=analysis,
        task=InferenceTask.ANALYSIS_SUMMARY,
        required=False,
    )

    assert response.degraded is True
    assert response.failure_reason == "inference_error: runtime failed"


def test_validate_task_output_strips_hallucinated_citations(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)

    valid_theme_keys = {finding.theme_key for finding in analysis.findings}
    valid_comment_ids: set[str] = set()
    for finding in analysis.findings:
        valid_comment_ids.update(finding.evidence_comment_ids)

    real_theme = next(iter(valid_theme_keys))
    real_comment_id = next(iter(valid_comment_ids))

    payload = validate_task_output(
        InferenceTask.ANALYSIS_SUMMARY,
        {
            "headline": "Test headline",
            "summary": "Test summary",
            "cited_theme_keys": [real_theme, "nonexistent_theme"],
            "cited_comment_ids": [real_comment_id, "reddit:fake_id"],
            "next_steps": ["Review performance"],
        },
        analysis=analysis,
    )

    assert "nonexistent_theme" not in payload["cited_theme_keys"]
    assert "reddit:fake_id" not in payload["cited_comment_ids"]
    assert real_theme in payload["cited_theme_keys"]
    assert real_comment_id in payload["cited_comment_ids"]


def test_validate_report_summary_strips_hallucinated_theme_keys(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)

    valid_theme_keys = {finding.theme_key for finding in analysis.findings}
    real_theme = next(iter(valid_theme_keys))

    payload = validate_task_output(
        InferenceTask.REPORT_SUMMARY,
        {
            "executive_summary": "Test executive summary",
            "caveats": ["Caveat one"],
            "cited_theme_keys": [real_theme, "hallucinated_key"],
        },
        analysis=analysis,
    )

    assert "hallucinated_key" not in payload["cited_theme_keys"]
    assert real_theme in payload["cited_theme_keys"]


def test_validate_task_output_without_analysis_preserves_all_citations() -> None:
    payload = validate_task_output(
        InferenceTask.ANALYSIS_SUMMARY,
        {
            "headline": "Test headline",
            "summary": "Test summary",
            "cited_theme_keys": ["anything", "goes"],
            "cited_comment_ids": ["fake:1", "fake:2"],
            "next_steps": ["Step one"],
        },
    )

    assert payload["cited_theme_keys"] == ["anything", "goes"]
    assert payload["cited_comment_ids"] == ["fake:1", "fake:2"]


def test_validate_corpus_synthesis_strips_hallucinated_thread_ids(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    manifest = build_corpus_manifest(
        name="Corpus",
        description="Deterministic corpus.",
        domain=DomainType.DEVELOPER_TOOLS,
        analysis_paths=[analysis_path],
        source_filter=None,
    )
    corpus = build_corpus_analysis(
        manifest,
        manifest_path=tmp_path / "manifest.json",
        evidence_limit=2,
        period=TrendPeriod.MONTH,
    )

    payload = validate_task_output(
        InferenceTask.CORPUS_SYNTHESIS,
        {
            "headline": "Cross-thread summary",
            "key_patterns": ["Performance repeats across threads."],
            "cited_thread_ids": [
                corpus.cross_thread_findings[0].top_evidence[0].thread_id,
                "missing:1",
            ],
            "recommended_actions": ["Review latency evidence"],
            "confidence_note": "Built from a small corpus.",
        },
        corpus=corpus,
    )

    assert payload["cited_thread_ids"] == [
        corpus.cross_thread_findings[0].top_evidence[0].thread_id
    ]


def test_router_returns_corpus_fallback_when_runtime_is_disabled(tmp_path: Path) -> None:
    analysis_path = load_analysis_fixture(tmp_path)
    manifest = build_corpus_manifest(
        name="Corpus",
        description="Deterministic corpus.",
        domain=DomainType.DEVELOPER_TOOLS,
        analysis_paths=[analysis_path],
        source_filter=None,
    )
    corpus = build_corpus_analysis(
        manifest,
        manifest_path=tmp_path / "manifest.json",
        evidence_limit=2,
        period=TrendPeriod.MONTH,
    )
    config = load_config(env={"THREADSENSE_RUNTIME_ENABLED": "false"})

    response = InferenceRouter(config).run_corpus_task(
        corpus=corpus,
        task=InferenceTask.CORPUS_SYNTHESIS,
        required=False,
    )

    assert response.used_fallback is True
    assert response.output["headline"]


# ---------------------------------------------------------------------------
# Vocabulary expansion
# ---------------------------------------------------------------------------


def test_vocabulary_expansion_fallback_when_runtime_disabled() -> None:
    config = load_config(env={"THREADSENSE_RUNTIME_ENABLED": "false"})
    vocabulary = load_domain_vocabulary("developer_tools")
    thread = load_canonical_thread(Path("tests/fixtures/analysis/canonical_feedback_thread.json"))

    response = InferenceRouter(config).run_vocabulary_expansion(thread, vocabulary)

    assert response.used_fallback is True
    assert response.output == {"existing_themes": {}, "new_themes": {}}


def test_validate_vocabulary_expansion_output_normalizes_keywords() -> None:
    payload = validate_task_output(
        InferenceTask.VOCABULARY_EXPANSION,
        {
            "existing_themes": {
                "performance": ["memory", "GPU", "  cpu  "],
            },
            "new_themes": {
                "tooling": ["Obsidian", "Notion"],
                "infra": ["docker", "kubernetes"],
            },
        },
    )

    assert payload["existing_themes"]["performance"] == ["memory", "gpu", "cpu"]
    assert payload["new_themes"]["tooling"] == ["obsidian", "notion"]


def test_merge_vocabulary_expansion_adds_keywords_to_existing_themes() -> None:
    base = load_domain_vocabulary("developer_tools")
    expansion = {
        "existing_themes": {"performance": ["vram", "throughput"]},
        "new_themes": {"knowledge_management": ["zettelkasten", "pkm"]},
    }

    merged = merge_vocabulary_expansion(base, expansion)

    assert "vram" in merged.theme_rules["performance"]
    assert "throughput" in merged.theme_rules["performance"]
    assert "knowledge_management" in merged.theme_rules
    assert merged.theme_rules["knowledge_management"] == ("zettelkasten", "pkm")
    assert merged.version.endswith("+expanded")


def test_merge_vocabulary_expansion_skips_default_theme_as_new() -> None:
    base = load_domain_vocabulary("developer_tools")
    expansion = {
        "existing_themes": {},
        "new_themes": {"general_feedback": ["misc", "other"]},
    }

    merged = merge_vocabulary_expansion(base, expansion)

    assert "general_feedback" not in merged.theme_rules


# ---------------------------------------------------------------------------
# Summary prompt enrichment
# ---------------------------------------------------------------------------


def _make_thread_with_comments() -> Thread:
    return Thread(
        thread_id="reddit:enrichment",
        source=SourceRef(
            source_name="reddit",
            community="test",
            source_thread_id="enrichment",
            thread_url="https://example.com",
        ),
        title="Test enrichment thread",
        permalink="https://example.com",
        author=AuthorRef(username="op", source_author_id=None),
        comments=[
            Comment(
                thread_id="reddit:enrichment",
                comment_id=f"reddit:e{i}",
                parent_comment_id=None,
                author=AuthorRef(username=f"user{i}", source_author_id=None),
                body=f"Top-level comment number {i} about performance and docs",
                score=10 - i,
                created_utc=float(i),
                depth=0,
                permalink=f"https://example.com/e{i}",
            )
            for i in range(5)
        ],
        comment_count=5,
        provenance=ProvenanceMetadata(
            raw_artifact_path="/tmp/raw.json",
            raw_sha256="sha",
            retrieved_at_utc=1.0,
            normalized_at_utc=2.0,
            schema_version=1,
            normalization_version="reddit-to-canonical-v1",
        ),
    )


def test_render_analysis_payload_includes_thread_context(tmp_path: Path) -> None:
    canonical_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    thread = _make_thread_with_comments()
    analysis = analyze_thread(thread, canonical_path)

    payload_without = json.loads(render_analysis_payload(analysis))
    payload_with = json.loads(render_analysis_payload(analysis, thread=thread))

    assert "top_comments" not in payload_without
    assert "top_comments" in payload_with
    assert len(payload_with["top_comments"]) <= 3
    assert "conversation_structure" in payload_with
    assert payload_with["conversation_structure"]["total_comments"] == 5
