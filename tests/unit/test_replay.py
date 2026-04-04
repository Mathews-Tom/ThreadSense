from __future__ import annotations

import json
from pathlib import Path

from threadsense.models.canonical import load_canonical_thread
from threadsense.pipeline.analyze import analyze_thread
from threadsense.pipeline.replay import replay_analysis


def test_replay_analysis_reproduces_current_output(tmp_path: Path) -> None:
    canonical_path = Path("tests/fixtures/analysis/canonical_feedback_thread.json")
    thread = load_canonical_thread(canonical_path)
    analysis = analyze_thread(thread, canonical_path)
    analysis_path = tmp_path / "analysis.json"
    analysis_path.write_text(json.dumps(analysis.to_dict()), encoding="utf-8")

    replay = replay_analysis(analysis_path)

    assert replay["status"] == "ready"
    assert replay["identical"] is True
    assert replay["differences"] == []
