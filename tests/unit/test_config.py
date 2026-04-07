from __future__ import annotations

from pathlib import Path

import pytest

from threadsense import __version__
from threadsense.config import InferenceBackend, PrivacyMode, load_config
from threadsense.contracts import AbstractionLevel, DomainType, ObjectiveType
from threadsense.errors import ConfigurationError
from threadsense.models.corpus import TrendPeriod


def test_load_config_uses_defaults_when_no_file_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("THREADSENSE_CONFIG", raising=False)
    config = load_config(env={})

    assert config.inference_backend is InferenceBackend.LOCAL_OPENAI_COMPATIBLE
    assert config.privacy_mode is PrivacyMode.LOCAL_ONLY
    assert config.runtime.enabled is True
    assert config.runtime.chat_endpoint == "http://127.0.0.1:11434/v1/chat/completions"
    assert config.runtime.timeout_seconds == 120.0
    assert config.runtime.repair_retries == 1
    assert config.source_policy.enabled_sources == (
        "reddit",
        "hackernews",
        "github_discussions",
        "github_gist",
    )
    assert config.reddit.listing_limit == 500
    assert config.reddit.timeout_seconds == 15.0
    assert config.hackernews.base_url == "https://hacker-news.firebaseio.com/v0"
    assert config.hackernews.request_delay_seconds == 1.0
    assert (
        config.reddit.user_agent
        == f"threadsense/{__version__} (https://github.com/Mathews-Tom/ThreadSense)"
    )
    assert config.storage.root_dir == Path(".threadsense")
    assert config.storage.normalized_dirname == "normalized"
    assert config.storage.analysis_dirname == "analysis"
    assert config.storage.report_dirname == "reports"
    assert config.storage.batch_dirname == "batches"
    assert config.storage.corpus_dirname == "corpora"
    assert config.batch.max_workers == 2
    assert config.batch.max_jobs == 25
    assert config.batch.fail_fast is False
    assert config.api.host == "127.0.0.1"
    assert config.api.port == 8090
    assert config.api.max_request_bytes == 1048576
    assert config.limits.runtime_concurrency == 1
    assert config.analysis.strategy == "keyword_heuristic"
    assert config.analysis.domain is DomainType.DEVELOPER_TOOLS
    assert config.analysis.objective is ObjectiveType.GENERAL_SURVEY
    assert config.analysis.abstraction_level is AbstractionLevel.OPERATIONAL
    assert config.analysis.duplicate_threshold == 0.88
    assert config.corpus.trend_period is TrendPeriod.MONTH
    assert config.corpus.evidence_limit == 3


def test_load_config_reads_toml_and_env_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "threadsense.toml"
    config_path.write_text(
        "\n".join(
            [
                "[app]",
                'inference_backend = "local_openai_compatible"',
                'privacy_mode = "local_only"',
                "",
                "[runtime]",
                "enabled = false",
                'base_url = "http://localhost:9000/"',
                'chat_path = "v1/chat/completions"',
                'model = "baseline-model"',
                "timeout_seconds = 12",
                "repair_retries = 2",
                "",
                "[sources]",
                'enabled = "reddit,hackernews"',
                "",
                "[reddit]",
                'user_agent = "fixture-agent"',
                "timeout_seconds = 20",
                "max_retries = 1",
                "backoff_seconds = 0.25",
                "request_delay_seconds = 0.6",
                "listing_limit = 100",
                "",
                "[hackernews]",
                'base_url = "https://example.com/v0"',
                "timeout_seconds = 22",
                "request_delay_seconds = 1.5",
                "",
                "[storage]",
                'root_dir = ".artifacts"',
                'raw_dirname = "raw-store"',
                'normalized_dirname = "canonical-store"',
                'analysis_dirname = "analysis-store"',
                'report_dirname = "report-store"',
                'batch_dirname = "batch-store"',
                'corpus_dirname = "corpus-store"',
                "",
                "[batch]",
                "max_workers = 3",
                "max_jobs = 40",
                "fail_fast = true",
                "",
                "[api]",
                'host = "0.0.0.0"',
                "port = 9001",
                "max_request_bytes = 2048",
                "",
                "[limits]",
                "runtime_concurrency = 2",
                "",
                "[analysis]",
                'domain = "product_feedback"',
                'objective = "feature_demand"',
                'abstraction_level = "architectural"',
                "",
                "[corpus]",
                'trend_period = "week"',
                "evidence_limit = 4",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("THREADSENSE_RUNTIME_MODEL", "override-model")
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("THREADSENSE_REDDIT_TIMEOUT", "25")
    monkeypatch.setenv("THREADSENSE_REDDIT_REQUEST_DELAY", "0.75")
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", ".runtime-store")
    monkeypatch.setenv("THREADSENSE_BATCH_MAX_WORKERS", "4")
    monkeypatch.setenv("THREADSENSE_RUNTIME_CONCURRENCY", "3")
    monkeypatch.setenv("THREADSENSE_ANALYSIS_DOMAIN", "hiring_careers")
    monkeypatch.setenv("THREADSENSE_ANALYSIS_OBJECTIVE", "competitive_intelligence")
    monkeypatch.setenv("THREADSENSE_ANALYSIS_LEVEL", "strategic")

    config = load_config(config_path=config_path)

    assert config.runtime.chat_endpoint == "http://localhost:9000/v1/chat/completions"
    assert config.runtime.enabled is True
    assert config.runtime.model == "override-model"
    assert config.runtime.repair_retries == 2
    assert config.source_policy.enabled_sources == ("reddit", "hackernews")
    assert config.reddit.user_agent == "fixture-agent"
    assert config.reddit.timeout_seconds == 25.0
    assert config.reddit.request_delay_seconds == 0.75
    assert config.reddit.listing_limit == 100
    assert config.hackernews.base_url == "https://example.com/v0"
    assert config.hackernews.timeout_seconds == 22.0
    assert config.hackernews.request_delay_seconds == 1.5
    assert config.storage.root_dir == Path(".runtime-store")
    assert config.storage.raw_dirname == "raw-store"
    assert config.storage.normalized_dirname == "canonical-store"
    assert config.storage.analysis_dirname == "analysis-store"
    assert config.storage.report_dirname == "report-store"
    assert config.storage.batch_dirname == "batch-store"
    assert config.storage.corpus_dirname == "corpus-store"
    assert config.batch.max_workers == 4
    assert config.batch.max_jobs == 40
    assert config.batch.fail_fast is True
    assert config.api.host == "0.0.0.0"
    assert config.api.port == 9001
    assert config.api.max_request_bytes == 2048
    assert config.limits.runtime_concurrency == 3
    assert config.analysis.domain is DomainType.HIRING_CAREERS
    assert config.analysis.objective is ObjectiveType.COMPETITIVE_INTELLIGENCE
    assert config.analysis.abstraction_level is AbstractionLevel.STRATEGIC
    assert config.corpus.trend_period is TrendPeriod.WEEK
    assert config.corpus.evidence_limit == 4


def test_load_config_rejects_invalid_privacy_mode(tmp_path: Path) -> None:
    config_path = tmp_path / "threadsense.toml"
    config_path.write_text('[app]\nprivacy_mode = "cloud"\n', encoding="utf-8")

    with pytest.raises(ConfigurationError):
        load_config(config_path=config_path, env={})
