from __future__ import annotations

from pathlib import Path
from time import time

from threadsense.config import AnalysisConfig
from threadsense.errors import AnalysisBoundaryError
from threadsense.models.analysis import (
    ANALYSIS_ENGINE_VERSION,
    ANALYSIS_SCHEMA_VERSION,
    AnalysisProvenance,
    ThreadAnalysis,
)
from threadsense.models.canonical import Thread
from threadsense.pipeline.storage import calculate_sha256, load_normalized_artifact
from threadsense.pipeline.strategies import AnalysisResult, AnalysisStrategy
from threadsense.pipeline.strategies.keyword_heuristic import KeywordHeuristicStrategy

STRATEGY_REGISTRY: dict[str, type[KeywordHeuristicStrategy]] = {
    "keyword_heuristic": KeywordHeuristicStrategy,
}


def resolve_strategy(config: AnalysisConfig) -> AnalysisStrategy:
    strategy_cls = STRATEGY_REGISTRY.get(config.strategy)
    if strategy_cls is None:
        raise AnalysisBoundaryError(
            f"unknown analysis strategy: {config.strategy}",
            details={"strategy": config.strategy},
        )
    return strategy_cls(duplicate_threshold=config.duplicate_threshold)


def analyze_thread_file(
    normalized_artifact_path: Path,
    config: AnalysisConfig | None = None,
) -> ThreadAnalysis:
    thread = load_normalized_artifact(normalized_artifact_path)
    return analyze_thread(thread, normalized_artifact_path, config)


def analyze_thread(
    thread: Thread,
    normalized_artifact_path: Path,
    config: AnalysisConfig | None = None,
) -> ThreadAnalysis:
    if config is not None:
        strategy = resolve_strategy(config)
    else:
        strategy = KeywordHeuristicStrategy()
    analysis_result = strategy.analyze(thread)
    return assemble_thread_analysis(thread, analysis_result, normalized_artifact_path)


def assemble_thread_analysis(
    thread: Thread,
    result: AnalysisResult,
    normalized_artifact_path: Path,
) -> ThreadAnalysis:
    return ThreadAnalysis(
        thread_id=thread.thread_id,
        source_name=thread.source.source_name,
        title=thread.title,
        total_comments=thread.comment_count,
        distinct_comment_count=result.distinct_comment_count,
        duplicate_group_count=result.duplicate_group_count,
        top_phrases=result.top_phrases,
        findings=result.findings,
        duplicate_groups=result.duplicate_groups,
        top_quotes=result.top_quotes,
        provenance=AnalysisProvenance(
            normalized_artifact_path=str(normalized_artifact_path),
            normalized_sha256=calculate_sha256(normalized_artifact_path),
            source_thread_id=thread.source.source_thread_id,
            analyzed_at_utc=time(),
            schema_version=ANALYSIS_SCHEMA_VERSION,
            analysis_version=ANALYSIS_ENGINE_VERSION,
        ),
    )
