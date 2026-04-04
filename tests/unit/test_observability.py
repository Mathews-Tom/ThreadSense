from __future__ import annotations

from threadsense.observability import MetricsRegistry, normalize_metric_labels


def test_normalize_metric_labels_collapses_symbols_and_case() -> None:
    labels = normalize_metric_labels(
        {
            "Stage Name": "Runtime Completion",
            "Source/Name": "Reddit.Com",
        }
    )

    assert labels == {
        "source_name": "reddit_com",
        "stage_name": "runtime_completion",
    }


def test_metrics_registry_renders_prometheus_samples() -> None:
    registry = MetricsRegistry()
    registry.increment(
        "threadsense_stage_total",
        {"stage": "fetch", "source_name": "reddit", "outcome": "ready"},
    )
    registry.observe_seconds(
        "threadsense_stage_seconds_avg",
        {"stage": "fetch", "source_name": "reddit", "outcome": "ready"},
        0.25,
    )
    registry.set_gauge("threadsense_duplicate_ratio", {"source_name": "reddit"}, 0.2)
    registry.observe_histogram(
        "threadsense_inference_latency_seconds",
        {"provider": "local", "task": "analysis_summary"},
        0.75,
    )

    output = registry.render_prometheus()

    assert 'threadsense_stage_total{outcome="ready",source_name="reddit",stage="fetch"}' in output
    assert "threadsense_stage_seconds_avg" in output
    assert "threadsense_gauge" in output
    assert "threadsense_histogram_avg" in output
