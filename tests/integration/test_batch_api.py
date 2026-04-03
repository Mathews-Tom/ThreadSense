from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from urllib import request

import pytest

from threadsense.api_server import start_api_server
from threadsense.cli import main
from threadsense.config import RedditConfig, load_config
from threadsense.connectors.reddit import RedditConnector, RedditThreadRequest, RedditThreadResult
from threadsense.logging_config import configure_logging


def load_fixture(name: str) -> list[dict[str, Any]]:
    payload = json.loads(Path(f"tests/fixtures/reddit/raw/{name}").read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    return payload


def build_fixture_connector(
    url_payloads: dict[str, list[dict[str, Any]]],
) -> type[RedditConnector]:
    class FixtureConnector(RedditConnector):
        def __init__(self, config: RedditConfig) -> None:
            self._config = config

        def fetch_thread(self, scrape_request: RedditThreadRequest) -> RedditThreadResult:
            for marker, fixture in url_payloads.items():
                if marker in scrape_request.post_url:
                    connector = RedditConnector(
                        config=RedditConfig(
                            user_agent=self._config.user_agent,
                            timeout_seconds=self._config.timeout_seconds,
                            max_retries=0,
                            backoff_seconds=self._config.backoff_seconds,
                            request_delay_seconds=0,
                            listing_limit=self._config.listing_limit,
                        ),
                        transport=build_fixture_transport(fixture),
                        sleeper=lambda value: None,
                    )
                    return connector.fetch_thread(scrape_request)
            raise AssertionError(f"unexpected fixture request: {scrape_request.post_url}")

    return FixtureConnector


def build_fixture_transport(
    payload: list[dict[str, Any]],
) -> Callable[[str, Mapping[str, str], Mapping[str, str | int | float | bool], float], Any]:
    def transport(
        url: str,
        headers: Mapping[str, str],
        params: Mapping[str, str | int | float | bool],
        timeout: float,
    ) -> list[dict[str, Any]]:
        del url, headers, params, timeout
        return payload

    return transport


def post_json(url: str, payload: dict[str, object]) -> dict[str, object]:
    api_request = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(api_request) as response:
        decoded = json.loads(response.read().decode("utf-8"))
    assert isinstance(decoded, dict)
    return decoded


def test_batch_run_processes_multiple_fixture_threads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture_connector = build_fixture_connector(
        {
            "abc123": load_fixture("normal_thread.json"),
            "jkl012": load_fixture("large_thread.json"),
        }
    )
    monkeypatch.setattr(
        "threadsense.cli.build_reddit_connector",
        lambda config: fixture_connector(config.reddit),
    )
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(tmp_path / "store"))

    exit_code = main(
        [
            "batch",
            "run",
            "--manifest",
            "tests/fixtures/batch/reddit_manifest.json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    batch_artifact = json.loads(Path(str(payload["output_path"])).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["total_jobs"] == 2
    assert payload["failed_jobs"] == 0
    assert batch_artifact["batch_run"]["worker_count"] == 2
    jobs = payload["jobs"]
    assert isinstance(jobs, list)
    first_job = jobs[0]
    assert isinstance(first_job, dict)
    outputs = first_job["outputs"]
    assert isinstance(outputs, dict)
    report_output = outputs["report"]
    assert isinstance(report_output, dict)
    assert Path(str(report_output["output_path"])).exists()


def test_api_workflow_runs_fetch_to_report_and_exposes_metrics(
    tmp_path: Path,
) -> None:
    config = load_config(
        env={
            "THREADSENSE_STORAGE_ROOT": str(tmp_path / "store"),
            "THREADSENSE_API_PORT": "0",
        }
    )
    logger = configure_logging()
    handle = start_api_server(
        config=config,
        logger=logger,
        connector_factory=lambda app_config: build_fixture_connector(
            {"abc123": load_fixture("normal_thread.json")}
        )(app_config.reddit),
        port=0,
    )
    try:
        fetch_payload = post_json(
            f"{handle.base_url}/v1/fetch/reddit",
            {"url": "https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread"},
        )
        normalize_payload = post_json(
            f"{handle.base_url}/v1/normalize/reddit",
            {"input_path": fetch_payload["output_path"]},
        )
        analyze_payload = post_json(
            f"{handle.base_url}/v1/analyze/normalized",
            {"input_path": normalize_payload["output_path"]},
        )
        report_payload = post_json(
            f"{handle.base_url}/v1/report/analysis",
            {"input_path": str(analyze_payload["output_path"]), "format": "json"},
        )
        with request.urlopen(f"{handle.base_url}/v1/metrics") as response:
            metrics_body = response.read().decode("utf-8")

        assert report_payload["format"] == "json"
        assert Path(str(report_payload["output_path"])).exists()
        assert 'stage="fetch"' in metrics_body
        assert 'stage="report"' in metrics_body
    finally:
        handle.server.shutdown()
        handle.server.server_close()
        handle.thread.join(timeout=2)


def test_batch_run_supports_live_summary_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest_path = tmp_path / "live-manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "run_name": "live-summary",
                "created_at_utc": 1710000100.0,
                "jobs": [
                    {
                        "job_id": "summary-job",
                        "source_name": "reddit",
                        "thread_url": "https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread",
                        "expand_more": False,
                        "flat": False,
                        "report_format": "markdown",
                        "with_summary": True,
                        "summary_required": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    fixture_connector = build_fixture_connector({"abc123": load_fixture("normal_thread.json")})
    monkeypatch.setattr(
        "threadsense.cli.build_reddit_connector",
        lambda config: fixture_connector(config.reddit),
    )
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", str(tmp_path / "store"))

    exit_code = main(["batch", "run", "--manifest", str(manifest_path)])
    payload = json.loads(capsys.readouterr().out)
    jobs = payload["jobs"]
    assert isinstance(jobs, list)
    first_job = jobs[0]
    assert isinstance(first_job, dict)
    outputs = first_job["outputs"]
    assert isinstance(outputs, dict)
    report_output = outputs["report"]
    assert isinstance(report_output, dict)

    assert exit_code == 0
    assert report_output["summary_provider"] == "local_openai_compatible"
    assert Path(str(report_output["output_path"])).exists()
