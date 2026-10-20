"""Recorded Reddit replay transport for watch sweeps.

Serves the synthesized Reddit JSON phases under tests/fixtures/watch/incremental/
in-process; format and replay rules are documented in that directory's README.md.
Imported as `recorded_reddit` by the integration tests (pytest prepends this
directory to sys.path) and by scripts/smoke_monitoring.py child processes.
"""

from __future__ import annotations

import copy
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from threadsense.config import AppConfig
from threadsense.connectors.reddit import RedditConnector
from threadsense.errors import RedditRequestError
from threadsense.workflows import RedditConnectorFactory


@dataclass(frozen=True)
class ReplayRoute:
    url: str
    params_match: dict[str, str]
    response_path: Path | None
    status: int | None


@dataclass(frozen=True)
class ReplayPhase:
    name: str
    routes: tuple[ReplayRoute, ...]
    fresh_comment_ids: frozenset[str]
    fresh_post_ids: frozenset[str]


class UnmatchedReplayRequestError(AssertionError):
    """A sweep made a Reddit request its phase does not list."""


def load_replay_phases(phases_path: Path) -> list[ReplayPhase]:
    document = json.loads(phases_path.read_text(encoding="utf-8"))
    phases: list[ReplayPhase] = []
    for phase in document["phases"]:
        routes = []
        for route in phase["routes"]:
            response = route.get("response")
            routes.append(
                ReplayRoute(
                    url=route["url"],
                    params_match={key: str(value) for key, value in route["params_match"].items()},
                    response_path=None if response is None else phases_path.parent / response,
                    status=route.get("status"),
                )
            )
        phases.append(
            ReplayPhase(
                name=phase["name"],
                routes=tuple(routes),
                fresh_comment_ids=frozenset(phase["fresh_comment_ids"]),
                fresh_post_ids=frozenset(phase["fresh_post_ids"]),
            )
        )
    return phases


def rebase_fresh_timestamps(
    node: Any, *, comment_ids: frozenset[str], post_ids: frozenset[str], served_at: float
) -> None:
    if isinstance(node, list):
        for item in node:
            rebase_fresh_timestamps(
                item, comment_ids=comment_ids, post_ids=post_ids, served_at=served_at
            )
        return
    if not isinstance(node, dict):
        return
    data = node.get("data")
    if isinstance(data, dict):
        kind = node.get("kind")
        thing_id = data.get("id")
        if (kind == "t1" and thing_id in comment_ids) or (kind == "t3" and thing_id in post_ids):
            data["created_utc"] = served_at
    for value in node.values():
        rebase_fresh_timestamps(
            value, comment_ids=comment_ids, post_ids=post_ids, served_at=served_at
        )


@dataclass
class PhaseReplayTransport:
    """`RedditTransport` serving one recorded phase; records every request it receives."""

    phase: ReplayPhase
    clock: Callable[[], float] = time.time
    requests: list[tuple[str, dict[str, str]]] = field(default_factory=list)

    def __call__(
        self,
        url: str,
        headers: Mapping[str, str],
        params: Mapping[str, str | int | float | bool],
        timeout: float,
    ) -> Any:
        resolved = {key: str(value) for key, value in params.items()}
        self.requests.append((url, resolved))
        route = self._match(url, resolved)
        if route.status is not None:
            raise RedditRequestError(
                "reddit request failed",
                details={"url": url, "status_code": route.status, "response_body": ""},
            )
        if route.response_path is None:
            raise UnmatchedReplayRequestError(f"route for {url} has neither response nor status")
        payload = copy.deepcopy(json.loads(route.response_path.read_text(encoding="utf-8")))
        rebase_fresh_timestamps(
            payload,
            comment_ids=self.phase.fresh_comment_ids,
            post_ids=self.phase.fresh_post_ids,
            served_at=self.clock(),
        )
        return payload

    def _match(self, url: str, params: dict[str, str]) -> ReplayRoute:
        for route in self.phase.routes:
            if route.url == url and all(
                params.get(key) == value for key, value in route.params_match.items()
            ):
                return route
        raise UnmatchedReplayRequestError(
            f"phase {self.phase.name!r} has no route for {url} with params {params}"
        )

    @property
    def requested_urls(self) -> list[str]:
        return [url for url, _ in self.requests]


def reroute(
    base: ReplayPhase,
    url_marker: str,
    *,
    status: int | None = None,
    response_path: Path | None = None,
) -> ReplayPhase:
    """Copy a recorded phase with every route whose URL contains `url_marker` replaced."""
    routes = tuple(
        replace(route, status=status, response_path=response_path)
        if url_marker in route.url
        else route
        for route in base.routes
    )
    assert routes != base.routes, f"phase {base.name!r} has no route matching {url_marker}"
    return replace(base, name=f"{base.name}_rerouted", routes=routes)


def replay_connector_factory(transport: PhaseReplayTransport) -> RedditConnectorFactory:
    def build(config: AppConfig) -> RedditConnector:
        return RedditConnector(
            config.reddit, cache=None, transport=transport, sleeper=lambda _: None
        )

    return build
