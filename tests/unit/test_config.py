from __future__ import annotations

from pathlib import Path

import pytest

from threadsense.config import InferenceBackend, PrivacyMode, load_config
from threadsense.errors import ConfigurationError


def test_load_config_uses_defaults_when_no_file_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("THREADSENSE_CONFIG", raising=False)
    config = load_config(env={})

    assert config.inference_backend is InferenceBackend.LOCAL_OPENAI_COMPATIBLE
    assert config.privacy_mode is PrivacyMode.LOCAL_ONLY
    assert config.runtime.enabled is True
    assert config.runtime.chat_endpoint == "http://127.0.0.1:8080/v1/chat/completions"
    assert config.runtime.repair_retries == 1
    assert config.source_policy.enabled_sources == ("reddit",)
    assert config.reddit.listing_limit == 500
    assert config.reddit.timeout_seconds == 15.0
    assert config.storage.root_dir == Path(".threadsense")
    assert config.storage.normalized_dirname == "normalized"
    assert config.storage.analysis_dirname == "analysis"
    assert config.storage.report_dirname == "reports"


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
                "[storage]",
                'root_dir = ".artifacts"',
                'raw_dirname = "raw-store"',
                'normalized_dirname = "canonical-store"',
                'analysis_dirname = "analysis-store"',
                'report_dirname = "report-store"',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("THREADSENSE_RUNTIME_MODEL", "override-model")
    monkeypatch.setenv("THREADSENSE_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("THREADSENSE_REDDIT_TIMEOUT", "25")
    monkeypatch.setenv("THREADSENSE_REDDIT_REQUEST_DELAY", "0.75")
    monkeypatch.setenv("THREADSENSE_STORAGE_ROOT", ".runtime-store")

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
    assert config.storage.root_dir == Path(".runtime-store")
    assert config.storage.raw_dirname == "raw-store"
    assert config.storage.normalized_dirname == "canonical-store"
    assert config.storage.analysis_dirname == "analysis-store"
    assert config.storage.report_dirname == "report-store"


def test_load_config_rejects_invalid_privacy_mode(tmp_path: Path) -> None:
    config_path = tmp_path / "threadsense.toml"
    config_path.write_text('[app]\nprivacy_mode = "cloud"\n', encoding="utf-8")

    with pytest.raises(ConfigurationError):
        load_config(config_path=config_path, env={})
