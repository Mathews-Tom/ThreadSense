from __future__ import annotations

import json
from pathlib import Path

import pytest

from threadsense.batching import load_batch_manifest, resolve_worker_count, validate_manifest_limits
from threadsense.errors import ResourceLimitError, SchemaBoundaryError


def test_load_batch_manifest_reads_jobs() -> None:
    manifest = load_batch_manifest(Path("tests/fixtures/batch/reddit_manifest.json"))

    assert manifest.run_name == "fixture-replay"
    assert len(manifest.jobs) == 2
    assert manifest.jobs[0].job_id == "normal-thread"


def test_load_batch_manifest_rejects_duplicate_job_ids(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "run_name": "duplicate-jobs",
                "created_at_utc": 1710000000.0,
                "jobs": [
                    {
                        "job_id": "same",
                        "source_name": "reddit",
                        "thread_url": "https://www.reddit.com/r/test/comments/abc123/one",
                        "expand_more": False,
                        "flat": False,
                        "report_format": "json",
                        "with_summary": False,
                        "summary_required": False,
                    },
                    {
                        "job_id": "same",
                        "source_name": "reddit",
                        "thread_url": "https://www.reddit.com/r/test/comments/def456/two",
                        "expand_more": False,
                        "flat": False,
                        "report_format": "json",
                        "with_summary": False,
                        "summary_required": False,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SchemaBoundaryError):
        load_batch_manifest(manifest_path)


def test_resolve_worker_count_respects_limit() -> None:
    assert resolve_worker_count(job_count=5, max_workers=2) == 2
    assert resolve_worker_count(job_count=1, max_workers=4) == 1


def test_validate_manifest_limits_rejects_oversized_runs() -> None:
    manifest = load_batch_manifest(Path("tests/fixtures/batch/reddit_manifest.json"))

    with pytest.raises(ResourceLimitError):
        validate_manifest_limits(manifest, max_jobs=1)
