from __future__ import annotations

import json
from pathlib import Path

from threadsense.inference.contracts import InferenceResponse, InferenceTask
from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.models.canonical import load_canonical_thread
from threadsense.models.report import load_report_artifact_file
from threadsense.pipeline.analyze import analyze_thread
from threadsense.reporting import (
    build_thread_report,
    render_report_html,
    render_report_json,
    render_report_markdown,
)


def build_analysis_artifact(tmp_path: Path) -> Path:
    canonical_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    thread = load_canonical_thread(canonical_path)
    analysis = analyze_thread(thread, canonical_path)
    analysis_path = tmp_path / "analysis.json"
    analysis_path.write_text(json.dumps(analysis.to_dict()), encoding="utf-8")
    return analysis_path


def test_build_thread_report_uses_local_summary_and_quality_checks(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    summary_response = InferenceResponse(
        task=InferenceTask.ANALYSIS_SUMMARY,
        provider="local_openai_compatible",
        model="local-model",
        finish_reason="stop",
        output={
            "headline": "Performance and docs dominate the thread",
            "summary": "Latency and onboarding gaps are the main concerns.",
            "cited_theme_keys": ["performance", "documentation"],
            "cited_comment_ids": ["reddit:c3", "reddit:c1"],
            "next_steps": ["Profile search latency", "Expand onboarding quickstart"],
        },
        used_fallback=False,
        degraded=False,
        failure_reason=None,
    )

    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=summary_response,
    )

    assert report.executive_summary.provider == "local_openai_compatible"
    assert report.findings
    assert report.conversation_structure.max_depth == 0
    assert report.conversation_structure.top_level_count == 7
    assert report.provenance.analysis_artifact_path == str(analysis_path)
    assert any(check.code == "coverage_gap" for check in report.quality_checks)


def test_render_report_markdown_contains_permalinks_and_sections(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=None,
    )

    markdown = render_report_markdown(report)

    assert "# Deterministic analysis fixture thread" in markdown
    assert "## Conversation Structure" in markdown
    assert "## Findings" in markdown
    assert "https://reddit.com/comments/analysis123/c3" in markdown


def test_render_report_json_round_trips_report_artifact(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=None,
    )
    report_path = tmp_path / "report.json"
    report_path.write_text(render_report_json(report), encoding="utf-8")

    loaded = load_report_artifact_file(report_path)

    assert loaded.thread_id == report.thread_id
    assert loaded.executive_summary.headline == report.executive_summary.headline
    assert loaded.conversation_structure.max_depth == report.conversation_structure.max_depth


def test_render_report_html_contains_findings_and_caveats(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=None,
    )

    html = render_report_html(report)

    assert "<!doctype html>" in html.lower()
    assert "Next Steps" in html
    assert "Findings" in html
    assert "Caveats" in html


def test_coverage_gap_auto_resolution_appends_uncovered_themes(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    summary_response = InferenceResponse(
        task=InferenceTask.ANALYSIS_SUMMARY,
        provider="local_openai_compatible",
        model="local-model",
        finish_reason="stop",
        output={
            "headline": "Performance leads",
            "summary": "Only performance discussed.",
            "cited_theme_keys": ["performance"],
            "cited_comment_ids": ["reddit:c3"],
            "next_steps": ["Profile search latency"],
        },
        used_fallback=False,
        degraded=False,
        failure_reason=None,
    )

    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=summary_response,
    )

    assert any("additional themes" in step.lower() for step in report.executive_summary.next_steps)
    assert any("documentation" in step for step in report.executive_summary.next_steps)
    assert any("reliability" in step for step in report.executive_summary.next_steps)


def test_coverage_gap_resolution_is_noop_when_all_themes_cited(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    all_theme_keys = [finding.theme_key for finding in analysis.findings]
    summary_response = InferenceResponse(
        task=InferenceTask.ANALYSIS_SUMMARY,
        provider="local_openai_compatible",
        model="local-model",
        finish_reason="stop",
        output={
            "headline": "Full coverage",
            "summary": "All themes covered.",
            "cited_theme_keys": all_theme_keys,
            "cited_comment_ids": ["reddit:c1"],
            "next_steps": ["Done"],
        },
        used_fallback=False,
        degraded=False,
        failure_reason=None,
    )

    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=summary_response,
    )

    has_gap_step = any(
        "additional themes" in step.lower() for step in report.executive_summary.next_steps
    )
    assert not has_gap_step


def test_render_report_markdown_includes_metadata_section(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=None,
    )

    markdown = render_report_markdown(report)

    assert "## Metadata" in markdown
    assert "Summary Provider:" in markdown
    assert "General Feedback Ratio:" in markdown


def test_weighted_quote_selection_prefers_dense_comments() -> None:
    """A longer, token-rich comment with moderate score should rank above a short high-voted one."""
    from threadsense.models.canonical import AuthorRef, Comment
    from threadsense.pipeline.strategies.keyword_heuristic import (
        build_comment_signal,
        select_representative_quotes,
    )

    short_high_score = Comment(
        thread_id="t",
        comment_id="short",
        parent_comment_id=None,
        author=AuthorRef(username="u1", source_author_id=None),
        body="yes same",
        score=20,
        created_utc=1.0,
        depth=0,
        permalink="https://example.com/short",
    )
    long_moderate_score = Comment(
        thread_id="t",
        comment_id="long",
        parent_comment_id=None,
        author=AuthorRef(username="u2", source_author_id=None),
        body=(
            "I have been running something close to what you describe for about a year. "
            "Two Obsidian vaults, work and personal. OCR documents from a self-hosted "
            "Paperless instance. Everything chunked, embedded on a local AMD GPU. "
            "The architecture uses a knowledge graph with vector embeddings for retrieval."
        ),
        score=5,
        created_utc=2.0,
        depth=0,
        permalink="https://example.com/long",
    )
    signals = [
        s
        for c in [short_high_score, long_moderate_score]
        if (s := build_comment_signal(c)) is not None
    ]

    quotes = select_representative_quotes(signals, limit=2)

    assert quotes[0].comment_id == "long"
