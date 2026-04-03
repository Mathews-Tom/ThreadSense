from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar

from threadsense.errors import ConfigurationError


class InferenceBackend(StrEnum):
    LOCAL_OPENAI_COMPATIBLE = "local_openai_compatible"


class PrivacyMode(StrEnum):
    LOCAL_ONLY = "local_only"


DEFAULT_CONFIG_PATH = Path("threadsense.toml")
EnumT = TypeVar("EnumT", bound=StrEnum)


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    chat_path: str
    model: str
    timeout_seconds: float

    @property
    def chat_endpoint(self) -> str:
        base = self.base_url.rstrip("/")
        path = self.chat_path if self.chat_path.startswith("/") else f"/{self.chat_path}"
        return f"{base}{path}"


@dataclass(frozen=True)
class SourcePolicyConfig:
    enabled_sources: tuple[str, ...]


@dataclass(frozen=True)
class RedditConfig:
    user_agent: str
    timeout_seconds: float
    max_retries: int
    backoff_seconds: float
    request_delay_seconds: float
    listing_limit: int


@dataclass(frozen=True)
class StorageConfig:
    root_dir: Path
    raw_dirname: str
    normalized_dirname: str


@dataclass(frozen=True)
class AppConfig:
    inference_backend: InferenceBackend
    privacy_mode: PrivacyMode
    runtime: RuntimeConfig
    source_policy: SourcePolicyConfig
    reddit: RedditConfig
    storage: StorageConfig


def _read_toml(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    if not path.exists():
        raise ConfigurationError(
            f"config file does not exist: {path}",
            details={"path": str(path)},
        )
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    if not isinstance(raw, dict):
        raise ConfigurationError("config file must decode to a TOML table")
    return raw


def _read_str(
    env: Mapping[str, str],
    env_key: str,
    fallback: str,
) -> str:
    value = env.get(env_key, fallback).strip()
    if not value:
        raise ConfigurationError(
            f"configuration value is empty: {env_key}",
            details={"env_key": env_key},
        )
    return value


def _parse_float(value: str, env_key: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise ConfigurationError(
            f"configuration value must be numeric: {env_key}",
            details={"env_key": env_key, "value": value},
        ) from error
    if parsed <= 0:
        raise ConfigurationError(
            f"configuration value must be greater than zero: {env_key}",
            details={"env_key": env_key, "value": value},
        )
    return parsed


def _parse_enum(enum_type: type[EnumT], value: str, env_key: str) -> EnumT:
    try:
        return enum_type(value)
    except ValueError as error:
        allowed = [member.value for member in enum_type]
        raise ConfigurationError(
            f"configuration value is invalid: {env_key}",
            details={"env_key": env_key, "value": value, "allowed": allowed},
        ) from error


def _parse_sources(raw_value: str) -> tuple[str, ...]:
    sources = tuple(source.strip() for source in raw_value.split(",") if source.strip())
    if not sources:
        raise ConfigurationError(
            "THREADSENSE_ENABLED_SOURCES must contain at least one source",
            details={"env_key": "THREADSENSE_ENABLED_SOURCES"},
        )
    return sources


def _parse_int(value: str, env_key: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise ConfigurationError(
            f"configuration value must be an integer: {env_key}",
            details={"env_key": env_key, "value": value},
        ) from error
    if parsed < 0:
        raise ConfigurationError(
            f"configuration value must be zero or greater: {env_key}",
            details={"env_key": env_key, "value": value},
        )
    return parsed


def load_config(
    config_path: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> AppConfig:
    resolved_env = dict(os.environ if env is None else env)
    raw_config = _read_toml(
        config_path if config_path is not None else _default_config_path(resolved_env)
    )

    app_section = raw_config.get("app", {})
    runtime_section = raw_config.get("runtime", {})
    source_section = raw_config.get("sources", {})
    reddit_section = raw_config.get("reddit", {})
    storage_section = raw_config.get("storage", {})
    if (
        not isinstance(app_section, dict)
        or not isinstance(runtime_section, dict)
        or not isinstance(source_section, dict)
        or not isinstance(reddit_section, dict)
        or not isinstance(storage_section, dict)
    ):
        raise ConfigurationError("config sections must be TOML tables")

    backend = _parse_enum(
        InferenceBackend,
        _read_str(
            resolved_env,
            "THREADSENSE_INFERENCE_BACKEND",
            str(
                app_section.get(
                    "inference_backend",
                    InferenceBackend.LOCAL_OPENAI_COMPATIBLE.value,
                )
            ),
        ),
        "THREADSENSE_INFERENCE_BACKEND",
    )
    privacy_mode = _parse_enum(
        PrivacyMode,
        _read_str(
            resolved_env,
            "THREADSENSE_PRIVACY_MODE",
            str(app_section.get("privacy_mode", PrivacyMode.LOCAL_ONLY.value)),
        ),
        "THREADSENSE_PRIVACY_MODE",
    )
    runtime = RuntimeConfig(
        base_url=_read_str(
            resolved_env,
            "THREADSENSE_RUNTIME_BASE_URL",
            str(runtime_section.get("base_url", "http://127.0.0.1:8080")),
        ),
        chat_path=_read_str(
            resolved_env,
            "THREADSENSE_RUNTIME_CHAT_PATH",
            str(runtime_section.get("chat_path", "/v1/chat/completions")),
        ),
        model=_read_str(
            resolved_env,
            "THREADSENSE_RUNTIME_MODEL",
            str(runtime_section.get("model", "local-model")),
        ),
        timeout_seconds=_parse_float(
            _read_str(
                resolved_env,
                "THREADSENSE_RUNTIME_TIMEOUT_SECONDS",
                str(runtime_section.get("timeout_seconds", "30")),
            ),
            "THREADSENSE_RUNTIME_TIMEOUT_SECONDS",
        ),
    )
    source_policy = SourcePolicyConfig(
        enabled_sources=_parse_sources(
            _read_str(
                resolved_env,
                "THREADSENSE_ENABLED_SOURCES",
                str(source_section.get("enabled", "reddit")),
            ),
        ),
    )
    reddit = RedditConfig(
        user_agent=_read_str(
            resolved_env,
            "THREADSENSE_REDDIT_USER_AGENT",
            str(
                reddit_section.get(
                    "user_agent",
                    "threadsense/0.1.0 (https://github.com/Mathews-Tom/ThreadSense)",
                )
            ),
        ),
        timeout_seconds=_parse_float(
            _read_str(
                resolved_env,
                "THREADSENSE_REDDIT_TIMEOUT",
                str(reddit_section.get("timeout_seconds", "15")),
            ),
            "THREADSENSE_REDDIT_TIMEOUT",
        ),
        max_retries=_parse_int(
            _read_str(
                resolved_env,
                "THREADSENSE_REDDIT_MAX_RETRIES",
                str(reddit_section.get("max_retries", "2")),
            ),
            "THREADSENSE_REDDIT_MAX_RETRIES",
        ),
        backoff_seconds=_parse_float(
            _read_str(
                resolved_env,
                "THREADSENSE_REDDIT_BACKOFF",
                str(reddit_section.get("backoff_seconds", "0.5")),
            ),
            "THREADSENSE_REDDIT_BACKOFF",
        ),
        request_delay_seconds=_parse_float(
            _read_str(
                resolved_env,
                "THREADSENSE_REDDIT_REQUEST_DELAY",
                str(reddit_section.get("request_delay_seconds", "0.6")),
            ),
            "THREADSENSE_REDDIT_REQUEST_DELAY",
        ),
        listing_limit=_parse_int(
            _read_str(
                resolved_env,
                "THREADSENSE_REDDIT_LISTING_LIMIT",
                str(reddit_section.get("listing_limit", "500")),
            ),
            "THREADSENSE_REDDIT_LISTING_LIMIT",
        ),
    )
    storage = StorageConfig(
        root_dir=Path(
            _read_str(
                resolved_env,
                "THREADSENSE_STORAGE_ROOT",
                str(storage_section.get("root_dir", ".threadsense")),
            )
        ),
        raw_dirname=_read_str(
            resolved_env,
            "THREADSENSE_STORAGE_RAW_DIR",
            str(storage_section.get("raw_dirname", "raw")),
        ),
        normalized_dirname=_read_str(
            resolved_env,
            "THREADSENSE_STORAGE_NORMALIZED_DIR",
            str(storage_section.get("normalized_dirname", "normalized")),
        ),
    )
    return AppConfig(
        inference_backend=backend,
        privacy_mode=privacy_mode,
        runtime=runtime,
        source_policy=source_policy,
        reddit=reddit,
        storage=storage,
    )


def _default_config_path(env: Mapping[str, str]) -> Path | None:
    env_path = env.get("THREADSENSE_CONFIG")
    if env_path:
        return Path(env_path)
    return DEFAULT_CONFIG_PATH if DEFAULT_CONFIG_PATH.exists() else None
