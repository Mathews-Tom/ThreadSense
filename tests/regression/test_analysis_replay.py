from __future__ import annotations

import json
from pathlib import Path

import pytest

from threadsense.models.analysis import load_analysis_artifact_file
from threadsense.models.canonical import load_canonical_thread
from threadsense.pipeline.analyze import analyze_thread
from threadsense.pipeline.replay import compare_analysis_outputs


@pytest.mark.parametrize(
    ("dataset_path", "expected_output_path"),
    [
        (
            Path("tests/golden/developer_tools/reddit_feedback_thread.json"),
            Path("tests/regression/expected_outputs/reddit_feedback_thread.json"),
        )
    ],
)
def test_analysis_output_matches_regression_snapshot(
    tmp_path: Path,
    dataset_path: Path,
    expected_output_path: Path,
) -> None:
    dataset = json.loads(dataset_path.read_text(encoding="utf-8"))
    fixture_path = Path(str(dataset["thread_fixture"]))
    analysis = analyze_thread(load_canonical_thread(fixture_path), fixture_path)
    analysis_path = tmp_path / "analysis.json"
    analysis_path.write_text(json.dumps(analysis.to_dict()), encoding="utf-8")

    current = load_analysis_artifact_file(analysis_path)
    expected = load_analysis_artifact_file(expected_output_path)
    comparison = compare_analysis_outputs(expected, current)

    assert comparison["identical"] is True
