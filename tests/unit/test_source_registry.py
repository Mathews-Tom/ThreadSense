from __future__ import annotations

from threadsense.config import load_config
from threadsense.connectors.registry import SourceRegistry


def test_source_registry_detects_reddit_and_hackernews_urls() -> None:
    registry = SourceRegistry(load_config(env={}))

    assert (
        registry.detect_source("https://www.reddit.com/r/python/comments/abc123/example")
        == "reddit"
    )
    assert registry.detect_source("https://news.ycombinator.com/item?id=123456") == "hackernews"


def test_source_registry_registers_enabled_sources() -> None:
    registry = SourceRegistry(
        load_config(
            env={
                "THREADSENSE_ENABLED_SOURCES": "reddit,hackernews,github_discussions",
            }
        )
    )

    assert registry.get("reddit").source_name == "reddit"
    assert registry.get("hackernews").source_name == "hackernews"
    assert registry.get("github_discussions").source_name == "github_discussions"
