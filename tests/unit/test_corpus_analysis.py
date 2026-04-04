from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from threadsense.contracts import DomainType
from threadsense.models.canonical import Thread, load_canonical_thread
from threadsense.models.corpus import TrendPeriod, load_corpus_analysis_file
from threadsense.pipeline.analyze import analyze_thread
from threadsense.pipeline.corpus import build_corpus_analysis, build_corpus_manifest


def build_variant_thread(base: Thread) -> Thread:
    comments = []
    for index, comment in enumerate(base.comments, start=1):
        comments.append(
            replace(
                comment,
                thread_id="reddit:analysis456",
                comment_id=f"reddit:v{index}",
                created_utc=comment.created_utc + 31 * 24 * 60 * 60,
                permalink=f"https://reddit.com/comments/analysis456/v{index}",
            )
        )
    return replace(
        base,
        thread_id="reddit:analysis456",
        title="Deterministic analysis fixture thread follow-up",
        permalink="https://reddit.com/comments/analysis456",
        source=replace(
            base.source,
            source_thread_id="analysis456",
            thread_url="https://reddit.com/comments/analysis456",
        ),
        comments=comments,
    )


def test_build_corpus_analysis_aggregates_cross_thread_findings(tmp_path: Path) -> None:
    fixture_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    base_thread = load_canonical_thread(fixture_path)
    variant_thread = build_variant_thread(base_thread)
    variant_path = tmp_path / "variant-thread.json"
    variant_path.write_text(json.dumps(variant_thread.to_dict()), encoding="utf-8")

    analysis_one = analyze_thread(base_thread, fixture_path)
    analysis_two = analyze_thread(variant_thread, variant_path)
    analysis_path_one = tmp_path / "one.json"
    analysis_path_two = tmp_path / "two.json"
    analysis_path_one.write_text(json.dumps(analysis_one.to_dict()), encoding="utf-8")
    analysis_path_two.write_text(json.dumps(analysis_two.to_dict()), encoding="utf-8")

    manifest = build_corpus_manifest(
        name="Deterministic Corpus",
        description="Cross-thread regression corpus.",
        domain=DomainType.DEVELOPER_TOOLS,
        analysis_paths=[analysis_path_one, analysis_path_two],
        source_filter=None,
    )
    corpus = build_corpus_analysis(
        manifest,
        manifest_path=tmp_path / "manifest.json",
        evidence_limit=2,
        period=TrendPeriod.MONTH,
    )

    assert corpus.thread_count == 2
    assert corpus.theme_frequency["performance"] == 2
    assert corpus.cross_thread_findings[0].thread_count == 2
    assert {trend.period for trend in corpus.temporal_trends} == {"2024-03", "2024-04"}


def test_corpus_analysis_persists_loadable_artifact(tmp_path: Path) -> None:
    fixture_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    thread = load_canonical_thread(fixture_path)
    analysis = analyze_thread(thread, fixture_path)
    analysis_path = tmp_path / "analysis.json"
    analysis_path.write_text(json.dumps(analysis.to_dict()), encoding="utf-8")

    manifest = build_corpus_manifest(
        name="Single Corpus",
        description="One thread corpus.",
        domain=DomainType.DEVELOPER_TOOLS,
        analysis_paths=[analysis_path],
        source_filter=None,
    )
    corpus = build_corpus_analysis(
        manifest,
        manifest_path=tmp_path / "manifest.json",
        evidence_limit=1,
        period=TrendPeriod.MONTH,
    )
    persisted = tmp_path / "corpus-analysis.json"
    persisted.write_text(json.dumps(corpus.to_dict()), encoding="utf-8")

    reloaded = load_corpus_analysis_file(persisted)

    assert reloaded.corpus_id == corpus.corpus_id
    assert reloaded.cross_thread_findings[0].top_evidence
