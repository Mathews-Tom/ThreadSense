from __future__ import annotations

from threadsense.config import load_config


def test_application_bootstraps_with_default_configuration() -> None:
    config = load_config(env={})

    assert config.runtime.chat_endpoint.endswith("/v1/chat/completions")
    assert config.source_policy.enabled_sources == ("reddit",)
