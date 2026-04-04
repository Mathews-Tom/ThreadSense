from __future__ import annotations

from pathlib import Path

from threadsense.config import AnalysisConfig
from threadsense.evaluation import compare_strategies, load_golden_dataset
from threadsense.models.canonical import load_canonical_thread


def test_compare_strategies_returns_equal_metrics_for_same_config() -> None:
    dataset = load_golden_dataset(Path("tests/golden/developer_tools/reddit_feedback_thread.json"))
    fixture_path = Path(dataset.thread_fixture)
    thread = load_canonical_thread(fixture_path)
    strategy = AnalysisConfig(strategy="keyword_heuristic", domain=dataset.domain)

    comparison = compare_strategies(thread, fixture_path, strategy, strategy, dataset)

    assert comparison.thread_id == thread.thread_id
    assert comparison.metrics_a == comparison.metrics_b
    assert comparison.winner is None
