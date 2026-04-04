from __future__ import annotations

from threadsense.config import AppConfig
from threadsense.connectors import SourceConnector
from threadsense.connectors.hackernews import HackerNewsConnector
from threadsense.connectors.reddit import RedditConnector
from threadsense.errors import AnalysisBoundaryError


class SourceRegistry:
    def __init__(self, config: AppConfig) -> None:
        self._connectors: dict[str, SourceConnector] = {}
        self._register_defaults(config)

    def get(self, source_name: str) -> SourceConnector:
        connector = self._connectors.get(source_name)
        if connector is None:
            raise AnalysisBoundaryError(
                "source connector is not registered",
                details={"source_name": source_name, "registered": sorted(self._connectors)},
            )
        return connector

    def detect_source(self, url: str) -> str:
        for source_name, connector in self._connectors.items():
            if connector.supports_url(url):
                return source_name
        raise AnalysisBoundaryError(
            "unable to detect source from URL",
            details={"url": url, "registered": sorted(self._connectors)},
        )

    def _register_defaults(self, config: AppConfig) -> None:
        enabled = set(config.source_policy.enabled_sources)
        if "reddit" in enabled:
            self._connectors["reddit"] = RedditConnector(config.reddit)
        if "hackernews" in enabled or "hn" in enabled:
            self._connectors["hackernews"] = HackerNewsConnector(config.hackernews)
