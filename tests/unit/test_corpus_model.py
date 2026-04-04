from __future__ import annotations

import json
from pathlib import Path

from threadsense.contracts import DomainType
from threadsense.models.analysis import RepresentativeQuote
from threadsense.models.corpus import (
    CorpusAnalysis,
    CorpusManifest,
    CorpusProvenance,
    CrossThreadEvidence,
    CrossThreadFinding,
    TemporalTrend,
    load_corpus_analysis_file,
    load_corpus_manifest_file,
)


def test_corpus_manifest_round_trips(tmp_path: Path) -> None:
    manifest = CorpusManifest(
        corpus_id="demo-corpus",
        name="Demo Corpus",
        description="A deterministic corpus fixture.",
        source_filter="reddit",
        domain=DomainType.DEVELOPER_TOOLS,
        created_at_utc=1.0,
        thread_ids=["reddit:1"],
        analysis_artifact_paths=["/tmp/a.json"],
    )
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest.to_dict()), encoding="utf-8")

    reloaded = load_corpus_manifest_file(path)

    assert reloaded.corpus_id == "demo-corpus"
    assert reloaded.domain.value == "developer_tools"


def test_corpus_analysis_round_trips(tmp_path: Path) -> None:
    analysis = CorpusAnalysis(
        corpus_id="demo-corpus",
        name="Demo Corpus",
        domain=DomainType.DEVELOPER_TOOLS,
        thread_count=1,
        total_comments=4,
        cross_thread_findings=[
            CrossThreadFinding(
                theme_key="performance",
                theme_label="performance",
                severity="medium",
                thread_count=1,
                total_comment_count=2,
                top_evidence=[
                    CrossThreadEvidence(
                        thread_id="reddit:1",
                        thread_title="Fixture",
                        finding_severity="medium",
                        comment_count=2,
                        top_quote=RepresentativeQuote(
                            comment_id="reddit:c1",
                            permalink="https://example.com/c1",
                            author="user",
                            body_excerpt="Search is slow.",
                            score=4,
                        ),
                    )
                ],
            )
        ],
        theme_frequency={"performance": 1},
        temporal_trends=[
            TemporalTrend(
                theme_key="performance",
                period="1970-01",
                thread_count=1,
                severity_distribution={"medium": 1},
            )
        ],
        provenance=CorpusProvenance(
            manifest_path="/tmp/manifest.json",
            input_analysis_paths=["/tmp/a.json"],
            generated_at_utc=2.0,
            schema_version=1,
            corpus_version="corpus-v1",
        ),
    )
    path = tmp_path / "analysis.json"
    path.write_text(json.dumps(analysis.to_dict()), encoding="utf-8")

    reloaded = load_corpus_analysis_file(path)

    assert reloaded.corpus_id == "demo-corpus"
    assert reloaded.cross_thread_findings[0].top_evidence[0].top_quote.comment_id == "reddit:c1"
