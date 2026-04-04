from __future__ import annotations

from pathlib import Path
from time import time

from threadsense.config import AnalysisConfig
from threadsense.contracts import (
    ANALYSIS_CONTRACT_SCHEMA_VERSION,
    AnalysisContract,
    contract_from_config,
    default_contract,
)
from threadsense.domains import load_domain_vocabulary
from threadsense.errors import AnalysisBoundaryError
from threadsense.models.analysis import (
    ANALYSIS_ENGINE_VERSION,
    ANALYSIS_SCHEMA_VERSION,
    AnalysisProvenance,
    ConversationStructure,
    EngagementSubtree,
    ThreadAnalysis,
)
from threadsense.models.canonical import Thread
from threadsense.pipeline.storage import calculate_sha256, load_normalized_artifact
from threadsense.pipeline.strategies import AnalysisResult, AnalysisStrategy
from threadsense.pipeline.strategies.keyword_heuristic import KeywordHeuristicStrategy
from threadsense.pipeline.tree import (
    compute_tree_metrics,
    detect_conversation_patterns,
    extract_reply_chains,
    score_subtrees,
)

STRATEGY_REGISTRY: dict[str, type[KeywordHeuristicStrategy]] = {
    "keyword_heuristic": KeywordHeuristicStrategy,
}


def resolve_strategy(config: AnalysisConfig, contract: AnalysisContract) -> AnalysisStrategy:
    strategy_cls = STRATEGY_REGISTRY.get(config.strategy)
    if strategy_cls is None:
        raise AnalysisBoundaryError(
            f"unknown analysis strategy: {config.strategy}",
            details={"strategy": config.strategy},
        )
    return strategy_cls(
        duplicate_threshold=config.duplicate_threshold,
        vocabulary=load_domain_vocabulary(contract.domain.value),
    )


def analyze_thread_file(
    normalized_artifact_path: Path,
    config: AnalysisConfig | None = None,
    contract: AnalysisContract | None = None,
) -> ThreadAnalysis:
    thread = load_normalized_artifact(normalized_artifact_path)
    return analyze_thread(thread, normalized_artifact_path, config, contract)


def analyze_thread(
    thread: Thread,
    normalized_artifact_path: Path,
    config: AnalysisConfig | None = None,
    contract: AnalysisContract | None = None,
) -> ThreadAnalysis:
    resolved_contract = resolve_analysis_contract(config, contract)
    if config is not None:
        strategy = resolve_strategy(config, resolved_contract)
    else:
        strategy = KeywordHeuristicStrategy(
            vocabulary=load_domain_vocabulary(resolved_contract.domain.value)
        )
    analysis_result = strategy.analyze(thread, resolved_contract)
    return assemble_thread_analysis(
        thread,
        analysis_result,
        normalized_artifact_path,
        resolved_contract,
    )


def assemble_thread_analysis(
    thread: Thread,
    result: AnalysisResult,
    normalized_artifact_path: Path,
    contract: AnalysisContract,
) -> ThreadAnalysis:
    return ThreadAnalysis(
        thread_id=thread.thread_id,
        source_name=thread.source.source_name,
        title=thread.title,
        total_comments=thread.comment_count,
        filtered_comment_count=result.filtered_comment_count,
        distinct_comment_count=result.distinct_comment_count,
        duplicate_group_count=result.duplicate_group_count,
        top_phrases=result.top_phrases,
        conversation_structure=build_conversation_structure(thread),
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
            contract=contract.to_dict(),
            contract_schema_version=ANALYSIS_CONTRACT_SCHEMA_VERSION,
        ),
    )


def resolve_analysis_contract(
    config: AnalysisConfig | None,
    contract: AnalysisContract | None,
) -> AnalysisContract:
    if contract is not None:
        return contract
    if config is not None:
        return contract_from_config(config)
    return default_contract()


def build_conversation_structure(thread: Thread) -> ConversationStructure:
    metrics = compute_tree_metrics(thread.comments)
    reply_chains = extract_reply_chains(thread.comments, min_length=3)
    patterns = detect_conversation_patterns(thread.comments, min_subtree_size=3)
    scored_subtrees = score_subtrees(thread.comments, min_subtree_size=2)

    pattern_counts = {"consensus": 0, "controversy": 0, "monologue": 0}
    for pattern in patterns:
        if pattern.pattern_type in pattern_counts:
            pattern_counts[pattern.pattern_type] += 1

    return ConversationStructure(
        max_depth=metrics.max_depth,
        top_level_count=metrics.top_level_count,
        reply_chain_count=len(reply_chains),
        longest_chain_length=reply_chains[0].length if reply_chains else 0,
        controversy_count=pattern_counts["controversy"],
        consensus_count=pattern_counts["consensus"],
        monologue_count=pattern_counts["monologue"],
        top_engagement_subtrees=[
            EngagementSubtree(
                root_comment_id=subtree.root_comment_id,
                root_author=subtree.root_author,
                subtree_size=subtree.subtree_size,
                max_depth_below=subtree.max_depth_below,
                engagement_score=subtree.engagement_score,
            )
            for subtree in scored_subtrees[:3]
        ],
    )
