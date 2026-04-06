from __future__ import annotations

import json
from pathlib import Path

import pytest

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
            "priority": "high",
            "confidence": 0.81,
            "why_now": "The highest-signal comments cluster around latency and docs.",
            "cited_theme_keys": ["performance", "documentation"],
            "cited_comment_ids": ["reddit:c3", "reddit:c1"],
            "next_steps": ["Profile search latency", "Expand onboarding quickstart"],
            "recommended_owner": "engineering",
            "action_type": "fix",
            "expected_outcome": "Reduce the main adoption blockers in the thread.",
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
    assert report.executive_summary.priority == "high"
    assert report.findings
    assert report.findings[0].issue_marker_count is not None
    assert report.findings[0].issue_marker_count >= 0
    assert report.findings[0].action_type
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
    assert "Issue Markers:" in markdown
    assert "Signal Type:" in markdown
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
    assert loaded.executive_summary.action_type == report.executive_summary.action_type
    assert loaded.findings[0].signal_type == report.findings[0].signal_type
    assert loaded.conversation_structure.max_depth == report.conversation_structure.max_depth


def test_load_report_artifact_migrates_older_summary_contracts(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=None,
    )
    payload = report.to_dict()
    payload["schema_version"] = 1
    payload["report_version"] = "report-v1.1"
    summary_payload = payload["report"]["executive_summary"]
    del summary_payload["priority"]
    del summary_payload["confidence"]
    del summary_payload["why_now"]
    del summary_payload["recommended_owner"]
    del summary_payload["action_type"]
    del summary_payload["expected_outcome"]
    payload["report"]["provenance"]["schema_version"] = 1
    payload["report"]["provenance"]["report_version"] = "report-v1.1"
    report_path = tmp_path / "report-v1.json"
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = load_report_artifact_file(report_path)

    assert loaded.executive_summary.priority in {"high", "medium", "low"}
    assert loaded.executive_summary.action_type
    assert loaded.provenance.schema_version == 3
    assert loaded.findings[0].signal_type == "issue"
    assert loaded.findings[0].recommended_owner == "engineering"
    assert loaded.findings[0].issue_marker_count == 2


def test_build_thread_report_classifies_finding_actions(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)

    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=None,
    )

    findings = {finding.theme_key: finding for finding in report.findings}

    assert findings["performance"].signal_type == "issue"
    assert findings["performance"].recommended_owner == "engineering"
    assert findings["performance"].action_type == "investigate"
    assert findings["documentation"].signal_type == "mixed"
    assert findings["documentation"].recommended_owner == "docs"
    assert findings["documentation"].action_type == "document"
    assert findings["workflow"].signal_type == "request"
    assert findings["workflow"].recommended_owner == "product"
    assert findings["workflow"].action_type == "design"
    assert findings["reliability"].signal_type == "issue"
    assert findings["reliability"].recommended_owner == "engineering"
    assert findings["reliability"].action_type == "investigate"


def test_load_report_artifact_rejects_invalid_action_signal_state(tmp_path: Path) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=None,
    )
    payload = report.to_dict()
    payload["report"]["findings"][0]["signal_type"] = "discussion"
    report_path = tmp_path / "invalid-report.json"
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(Exception, match="deterministic action signal|signal_type"):
        load_report_artifact_file(report_path)


def test_load_report_artifact_rejects_migration_when_analysis_hash_mismatches(
    tmp_path: Path,
) -> None:
    analysis_path = build_analysis_artifact(tmp_path)
    analysis = load_analysis_artifact_file(analysis_path)
    report = build_thread_report(
        analysis=analysis,
        analysis_artifact_path=str(analysis_path),
        summary_response=None,
    )
    payload = report.to_dict()
    payload["schema_version"] = 2
    payload["report_version"] = "report-v1.2"
    payload["report"]["provenance"]["schema_version"] = 2
    payload["report"]["provenance"]["report_version"] = "report-v1.2"
    payload["report"]["provenance"]["analysis_sha256"] = "bad-sha"
    report_path = tmp_path / "migrate-report.json"
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(Exception, match="hash mismatch"):
        load_report_artifact_file(report_path)


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
    assert "Issue Markers:" in html
    assert "Action:" in html


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
            "priority": "medium",
            "confidence": 0.73,
            "why_now": "Performance is the clearest issue in the current summary.",
            "cited_theme_keys": ["performance"],
            "cited_comment_ids": ["reddit:c3"],
            "next_steps": ["Profile search latency"],
            "recommended_owner": "engineering",
            "action_type": "investigate",
            "expected_outcome": "Clarify the latency bottleneck before a fix.",
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
            "priority": "medium",
            "confidence": 0.7,
            "why_now": "The summary already covers the main findings.",
            "cited_theme_keys": all_theme_keys,
            "cited_comment_ids": ["reddit:c1"],
            "next_steps": ["Done"],
            "recommended_owner": "research",
            "action_type": "monitor",
            "expected_outcome": "Keep tracking whether these themes persist.",
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
    assert "Recommended Owner:" in markdown
    assert "Expected Outcome:" in markdown


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
