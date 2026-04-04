from __future__ import annotations

from pathlib import Path

from threadsense.models.corpus import CorpusAnalysis, CorpusProvenance, DomainType
from threadsense.pipeline.corpus_index import index_corpus, load_index, search_index


def build_corpus() -> CorpusAnalysis:
    return CorpusAnalysis(
        corpus_id="demo",
        name="Demo Corpus",
        domain=DomainType.DEVELOPER_TOOLS,
        thread_count=2,
        total_comments=10,
        cross_thread_findings=[],
        theme_frequency={"documentation": 2, "performance": 1},
        temporal_trends=[],
        provenance=CorpusProvenance(
            manifest_path="/tmp/manifest.json",
            input_analysis_paths=["/tmp/a.json"],
            generated_at_utc=1.0,
            schema_version=1,
            corpus_version="corpus-v1",
        ),
    )


def test_corpus_index_round_trips_and_searches(tmp_path: Path) -> None:
    index_path = tmp_path / "corpora.json"
    corpus = build_corpus()

    index_corpus(index_path, corpus)

    entries = load_index(index_path)
    matches = search_index(index_path, "performance")

    assert len(entries) == 1
    assert matches[0]["corpus_id"] == "demo"
