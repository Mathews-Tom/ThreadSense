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
    assert config.runtime.chat_endpoint == "http://127.0.0.1:8080/v1/chat/completions"
    assert config.source_policy.enabled_sources == ("reddit",)


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
                'base_url = "http://localhost:9000/"',
                'chat_path = "v1/chat/completions"',
                'model = "baseline-model"',
                "timeout_seconds = 12",
                "",
                "[sources]",
                'enabled = "reddit,hackernews"',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("THREADSENSE_RUNTIME_MODEL", "override-model")

    config = load_config(config_path=config_path)

    assert config.runtime.chat_endpoint == "http://localhost:9000/v1/chat/completions"
    assert config.runtime.model == "override-model"
    assert config.source_policy.enabled_sources == ("reddit", "hackernews")


def test_load_config_rejects_invalid_privacy_mode(tmp_path: Path) -> None:
    config_path = tmp_path / "threadsense.toml"
    config_path.write_text('[app]\nprivacy_mode = "cloud"\n', encoding="utf-8")

    with pytest.raises(ConfigurationError):
        load_config(config_path=config_path, env={})
