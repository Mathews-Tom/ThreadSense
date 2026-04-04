from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from threadsense.models.corpus import CorpusAnalysis


def index_corpus(index_path: Path, corpus: CorpusAnalysis) -> None:
    entries = load_index(index_path)
    updated = [entry for entry in entries if entry.get("corpus_id") != corpus.corpus_id]
    updated.append(
        {
            "corpus_id": corpus.corpus_id,
            "name": corpus.name,
            "domain": corpus.domain.value,
            "thread_count": corpus.thread_count,
            "themes": sorted(corpus.theme_frequency),
        }
    )
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(json.dumps({"entries": updated}, indent=2), encoding="utf-8")


def load_index(index_path: Path) -> list[dict[str, Any]]:
    if not index_path.exists():
        return []
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    entries = payload.get("entries", [])
    return [entry for entry in entries if isinstance(entry, dict)]


def search_index(index_path: Path, query: str) -> list[dict[str, Any]]:
    query_text = query.strip().lower()
    if not query_text:
        return load_index(index_path)
    return [
        entry
        for entry in load_index(index_path)
        if query_text in str(entry.get("name", "")).lower()
        or query_text in str(entry.get("domain", "")).lower()
        or any(query_text in str(theme).lower() for theme in entry.get("themes", []))
    ]
