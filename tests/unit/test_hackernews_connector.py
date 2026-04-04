from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from threadsense.config import HackerNewsConfig
from threadsense.connectors import FetchRequest, SourceConnector
from threadsense.connectors.hackernews import HackerNewsConnector, normalize_url


def load_fixture(name: str) -> dict[str, Any]:
    payload = json.loads(Path(f"tests/fixtures/hackernews/raw/{name}").read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_normalize_url_extracts_item_id() -> None:
    normalized_url, item_id = normalize_url("https://news.ycombinator.com/item?id=12345")

    assert normalized_url == "https://news.ycombinator.com/item?id=12345"
    assert item_id == 12345


def test_hackernews_connector_fetches_and_normalizes_fixture(tmp_path: Path) -> None:
    fixture = load_fixture("story_thread.json")
    item_payloads = fixture["items"]

    def transport(url: str, timeout: float) -> object:
        item_id = url.rsplit("/", 1)[1].removesuffix(".json")
        return item_payloads[item_id]

    connector = HackerNewsConnector(
        config=HackerNewsConfig(
            base_url="https://hacker-news.firebaseio.com/v0",
            timeout_seconds=15,
            request_delay_seconds=0,
        ),
        transport=transport,
        sleeper=lambda _: None,
    )
    assert isinstance(connector, SourceConnector)

    result = connector.fetch(FetchRequest(url=fixture["url"], expand=False, timeout_seconds=15))
    raw_path = tmp_path / "hn.json"
    raw_path.write_text(json.dumps(result.to_dict()), encoding="utf-8")

    thread = connector.normalize(result.to_dict(), raw_path)

    assert result.source_name == "hackernews"
    assert result.source_thread_id == "123"
    assert result.total_comment_count == 3
    assert thread.thread_id == "hn:123"
    assert thread.source.source_name == "hackernews"
    assert thread.comments[1].parent_comment_id == "hn:201"
