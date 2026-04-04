from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from threadsense.config import load_config
from threadsense.connectors.reddit import RedditConnector
from threadsense.logging_config import configure_logging
from threadsense.models.results import PipelineResult
from threadsense.observability import TraceContext
from threadsense.workflows import run_reddit_pipeline


def load_fixture(name: str) -> object:
    return json.loads(Path(f"tests/fixtures/reddit/raw/{name}").read_text(encoding="utf-8"))


def build_fixture_connector(config: object) -> RedditConnector:
    """Build a connector backed by a saved fixture instead of live Reddit."""
    from threadsense.config import RedditConfig

    fixture = load_fixture("normal_thread.json")

    def fixture_transport(
        url: str,
        headers: Mapping[str, str],
        params: Mapping[str, str | int | float | bool],
        timeout: float,
    ) -> object:
        return fixture

    return RedditConnector(
        config=RedditConfig(
            user_agent="threadsense/test",
            timeout_seconds=15,
            max_retries=0,
            backoff_seconds=0.1,
            request_delay_seconds=0,
            listing_limit=500,
        ),
        transport=fixture_transport,
        sleeper=lambda _: None,
    )


def test_full_pipeline_produces_report_from_fixture(tmp_path: Path) -> None:
    """Run the full pipeline against a fixture and verify the report artifact."""
    config = load_config(
        env={
            "THREADSENSE_STORAGE_ROOT": str(tmp_path / ".threadsense"),
            "THREADSENSE_RUNTIME_ENABLED": "false",
        },
    )
    logger = configure_logging()
    trace = TraceContext.create(run_id="test-pipeline", source_name="reddit")

    result = run_reddit_pipeline(
        config=config,
        logger=logger,
        trace=trace,
        url="https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread",
        expand_more=False,
        flat=False,
        report_format="json",
        with_summary=False,
        summary_required=False,
        contract=None,
        connector_factory=build_fixture_connector,
    )

    # Verify typed result structure
    assert isinstance(result, PipelineResult)
    assert result.status == "ready"
    assert result.source == "reddit"

    # Verify fetch stage
    assert result.fetch.status == "ready"
    assert result.fetch.post_id == "abc123"
    assert result.fetch.total_comment_count == 3
    assert result.fetch.output_path.exists()

    # Verify normalize stage
    assert result.normalize.status == "ready"
    assert result.normalize.thread_id == "reddit:abc123"
    assert result.normalize.output_path.exists()

    # Verify analyze stage
    assert result.analyze.status == "ready"
    assert result.analyze.finding_count > 0
    assert result.analyze.output_path.exists()

    # Verify report stage
    assert result.report.status == "ready"
    assert result.report.output_path.exists()
    assert result.report.report_format == "json"

    # Verify report artifact content
    report_content = json.loads(result.report.output_path.read_text(encoding="utf-8"))
    assert report_content["artifact_kind"] == "thread_report"
    assert report_content["report"]["thread_id"] == "reddit:abc123"

    # Verify provenance chain — report references analysis, which references normalized
    report_data = report_content["report"]
    assert "provenance" in report_data


def test_full_pipeline_produces_markdown_report(tmp_path: Path) -> None:
    """Verify markdown format works end-to-end."""
    config = load_config(
        env={
            "THREADSENSE_STORAGE_ROOT": str(tmp_path / ".threadsense"),
            "THREADSENSE_RUNTIME_ENABLED": "false",
        },
    )
    logger = configure_logging()
    trace = TraceContext.create(run_id="test-pipeline-md", source_name="reddit")

    result = run_reddit_pipeline(
        config=config,
        logger=logger,
        trace=trace,
        url="https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread",
        expand_more=False,
        flat=False,
        report_format="markdown",
        with_summary=False,
        summary_required=False,
        contract=None,
        connector_factory=build_fixture_connector,
    )

    assert result.report.report_format == "markdown"
    assert result.report.output_path.exists()
    markdown_content = result.report.output_path.read_text(encoding="utf-8")
    assert "# Normal thread" in markdown_content
    assert "abc123" in markdown_content
