from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar

from threadsense import __version__
from threadsense.errors import ConfigurationError


class InferenceBackend(StrEnum):
    LOCAL_OPENAI_COMPATIBLE = "local_openai_compatible"


class PrivacyMode(StrEnum):
    LOCAL_ONLY = "local_only"


DEFAULT_CONFIG_PATH = Path("threadsense.toml")
DEFAULT_REDDIT_USER_AGENT = (
    f"threadsense/{__version__} (https://github.com/Mathews-Tom/ThreadSense)"
)
EnumT = TypeVar("EnumT", bound=StrEnum)


@dataclass(frozen=True)
class RuntimeConfig:
    enabled: bool
    base_url: str
    chat_path: str
    model: str
    timeout_seconds: float
    repair_retries: int
    json_mode: bool

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
    analysis_dirname: str
    report_dirname: str
    batch_dirname: str


@dataclass(frozen=True)
class BatchConfig:
    max_workers: int
    max_jobs: int
    fail_fast: bool


@dataclass(frozen=True)
class ApiConfig:
    host: str
    port: int
    max_request_bytes: int


@dataclass(frozen=True)
class LimitsConfig:
    runtime_concurrency: int


@dataclass(frozen=True)
class AnalysisConfig:
    strategy: str
    duplicate_threshold: float


@dataclass(frozen=True)
class AppConfig:
    inference_backend: InferenceBackend
    privacy_mode: PrivacyMode
    runtime: RuntimeConfig
    source_policy: SourcePolicyConfig
    reddit: RedditConfig
    storage: StorageConfig
    batch: BatchConfig
    api: ApiConfig
    limits: LimitsConfig
    analysis: AnalysisConfig


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


def _parse_positive_int(value: str, env_key: str) -> int:
    parsed = _parse_int(value, env_key)
    if parsed <= 0:
        raise ConfigurationError(
            f"configuration value must be greater than zero: {env_key}",
            details={"env_key": env_key, "value": value},
        )
    return parsed


def _parse_bool(value: str, env_key: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(
        f"configuration value must be boolean: {env_key}",
        details={"env_key": env_key, "value": value},
    )


def _read_section(raw_config: Mapping[str, Any], section_name: str) -> Mapping[str, Any]:
    section = raw_config.get(section_name, {})
    if not isinstance(section, dict):
        raise ConfigurationError("config sections must be TOML tables")
    return section


def _read_section_str(
    env: Mapping[str, str],
    env_key: str,
    section: Mapping[str, Any],
    field: str,
    default: str,
) -> str:
    return _read_str(env, env_key, str(section.get(field, default)))


def _load_app_settings(
    resolved_env: Mapping[str, str],
    app_section: Mapping[str, Any],
) -> tuple[InferenceBackend, PrivacyMode]:
    return (
        _parse_enum(
            InferenceBackend,
            _read_section_str(
                resolved_env,
                "THREADSENSE_INFERENCE_BACKEND",
                app_section,
                "inference_backend",
                InferenceBackend.LOCAL_OPENAI_COMPATIBLE.value,
            ),
            "THREADSENSE_INFERENCE_BACKEND",
        ),
        _parse_enum(
            PrivacyMode,
            _read_section_str(
                resolved_env,
                "THREADSENSE_PRIVACY_MODE",
                app_section,
                "privacy_mode",
                PrivacyMode.LOCAL_ONLY.value,
            ),
            "THREADSENSE_PRIVACY_MODE",
        ),
    )


def _load_runtime_config(
    resolved_env: Mapping[str, str],
    runtime_section: Mapping[str, Any],
) -> RuntimeConfig:
    return RuntimeConfig(
        enabled=_parse_bool(
            _read_section_str(
                resolved_env,
                "THREADSENSE_RUNTIME_ENABLED",
                runtime_section,
                "enabled",
                "true",
            ),
            "THREADSENSE_RUNTIME_ENABLED",
        ),
        base_url=_read_section_str(
            resolved_env,
            "THREADSENSE_RUNTIME_BASE_URL",
            runtime_section,
            "base_url",
            "http://127.0.0.1:8080",
        ),
        chat_path=_read_section_str(
            resolved_env,
            "THREADSENSE_RUNTIME_CHAT_PATH",
            runtime_section,
            "chat_path",
            "/v1/chat/completions",
        ),
        model=_read_section_str(
            resolved_env,
            "THREADSENSE_RUNTIME_MODEL",
            runtime_section,
            "model",
            "local-model",
        ),
        timeout_seconds=_parse_float(
            _read_section_str(
                resolved_env,
                "THREADSENSE_RUNTIME_TIMEOUT_SECONDS",
                runtime_section,
                "timeout_seconds",
                "90",
            ),
            "THREADSENSE_RUNTIME_TIMEOUT_SECONDS",
        ),
        repair_retries=_parse_int(
            _read_section_str(
                resolved_env,
                "THREADSENSE_RUNTIME_REPAIR_RETRIES",
                runtime_section,
                "repair_retries",
                "1",
            ),
            "THREADSENSE_RUNTIME_REPAIR_RETRIES",
        ),
        json_mode=_parse_bool(
            _read_section_str(
                resolved_env,
                "THREADSENSE_RUNTIME_JSON_MODE",
                runtime_section,
                "json_mode",
                "false",
            ),
            "THREADSENSE_RUNTIME_JSON_MODE",
        ),
    )


def _load_source_policy(
    resolved_env: Mapping[str, str],
    source_section: Mapping[str, Any],
) -> SourcePolicyConfig:
    return SourcePolicyConfig(
        enabled_sources=_parse_sources(
            _read_section_str(
                resolved_env,
                "THREADSENSE_ENABLED_SOURCES",
                source_section,
                "enabled",
                "reddit",
            )
        )
    )


def _load_reddit_config(
    resolved_env: Mapping[str, str],
    reddit_section: Mapping[str, Any],
) -> RedditConfig:
    return RedditConfig(
        user_agent=_read_section_str(
            resolved_env,
            "THREADSENSE_REDDIT_USER_AGENT",
            reddit_section,
            "user_agent",
            DEFAULT_REDDIT_USER_AGENT,
        ),
        timeout_seconds=_parse_float(
            _read_section_str(
                resolved_env,
                "THREADSENSE_REDDIT_TIMEOUT",
                reddit_section,
                "timeout_seconds",
                "15",
            ),
            "THREADSENSE_REDDIT_TIMEOUT",
        ),
        max_retries=_parse_int(
            _read_section_str(
                resolved_env,
                "THREADSENSE_REDDIT_MAX_RETRIES",
                reddit_section,
                "max_retries",
                "2",
            ),
            "THREADSENSE_REDDIT_MAX_RETRIES",
        ),
        backoff_seconds=_parse_float(
            _read_section_str(
                resolved_env,
                "THREADSENSE_REDDIT_BACKOFF",
                reddit_section,
                "backoff_seconds",
                "0.5",
            ),
            "THREADSENSE_REDDIT_BACKOFF",
        ),
        request_delay_seconds=_parse_float(
            _read_section_str(
                resolved_env,
                "THREADSENSE_REDDIT_REQUEST_DELAY",
                reddit_section,
                "request_delay_seconds",
                "0.6",
            ),
            "THREADSENSE_REDDIT_REQUEST_DELAY",
        ),
        listing_limit=_parse_int(
            _read_section_str(
                resolved_env,
                "THREADSENSE_REDDIT_LISTING_LIMIT",
                reddit_section,
                "listing_limit",
                "500",
            ),
            "THREADSENSE_REDDIT_LISTING_LIMIT",
        ),
    )


def _load_storage_config(
    resolved_env: Mapping[str, str],
    storage_section: Mapping[str, Any],
) -> StorageConfig:
    return StorageConfig(
        root_dir=Path(
            _read_section_str(
                resolved_env,
                "THREADSENSE_STORAGE_ROOT",
                storage_section,
                "root_dir",
                ".threadsense",
            )
        ),
        raw_dirname=_read_section_str(
            resolved_env,
            "THREADSENSE_STORAGE_RAW_DIR",
            storage_section,
            "raw_dirname",
            "raw",
        ),
        normalized_dirname=_read_section_str(
            resolved_env,
            "THREADSENSE_STORAGE_NORMALIZED_DIR",
            storage_section,
            "normalized_dirname",
            "normalized",
        ),
        analysis_dirname=_read_section_str(
            resolved_env,
            "THREADSENSE_STORAGE_ANALYSIS_DIR",
            storage_section,
            "analysis_dirname",
            "analysis",
        ),
        report_dirname=_read_section_str(
            resolved_env,
            "THREADSENSE_STORAGE_REPORT_DIR",
            storage_section,
            "report_dirname",
            "reports",
        ),
        batch_dirname=_read_section_str(
            resolved_env,
            "THREADSENSE_STORAGE_BATCH_DIR",
            storage_section,
            "batch_dirname",
            "batches",
        ),
    )


def _load_batch_config(
    resolved_env: Mapping[str, str],
    batch_section: Mapping[str, Any],
) -> BatchConfig:
    return BatchConfig(
        max_workers=_parse_positive_int(
            _read_section_str(
                resolved_env,
                "THREADSENSE_BATCH_MAX_WORKERS",
                batch_section,
                "max_workers",
                "2",
            ),
            "THREADSENSE_BATCH_MAX_WORKERS",
        ),
        max_jobs=_parse_positive_int(
            _read_section_str(
                resolved_env,
                "THREADSENSE_BATCH_MAX_JOBS",
                batch_section,
                "max_jobs",
                "25",
            ),
            "THREADSENSE_BATCH_MAX_JOBS",
        ),
        fail_fast=_parse_bool(
            _read_section_str(
                resolved_env,
                "THREADSENSE_BATCH_FAIL_FAST",
                batch_section,
                "fail_fast",
                "false",
            ),
            "THREADSENSE_BATCH_FAIL_FAST",
        ),
    )


def _load_api_config(
    resolved_env: Mapping[str, str],
    api_section: Mapping[str, Any],
) -> ApiConfig:
    return ApiConfig(
        host=_read_section_str(
            resolved_env,
            "THREADSENSE_API_HOST",
            api_section,
            "host",
            "127.0.0.1",
        ),
        port=_parse_int(
            _read_section_str(
                resolved_env,
                "THREADSENSE_API_PORT",
                api_section,
                "port",
                "8090",
            ),
            "THREADSENSE_API_PORT",
        ),
        max_request_bytes=_parse_positive_int(
            _read_section_str(
                resolved_env,
                "THREADSENSE_API_MAX_REQUEST_BYTES",
                api_section,
                "max_request_bytes",
                "1048576",
            ),
            "THREADSENSE_API_MAX_REQUEST_BYTES",
        ),
    )


def _parse_threshold(value: str, env_key: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise ConfigurationError(
            f"configuration value must be numeric: {env_key}",
            details={"env_key": env_key, "value": value},
        ) from error
    if not (0.0 < parsed <= 1.0):
        raise ConfigurationError(
            f"configuration threshold must be between 0 and 1: {env_key}",
            details={"env_key": env_key, "value": value},
        )
    return parsed


def _load_analysis_config(
    resolved_env: Mapping[str, str],
    analysis_section: Mapping[str, Any],
) -> AnalysisConfig:
    return AnalysisConfig(
        strategy=_read_section_str(
            resolved_env,
            "THREADSENSE_ANALYSIS_STRATEGY",
            analysis_section,
            "strategy",
            "keyword_heuristic",
        ),
        duplicate_threshold=_parse_threshold(
            _read_section_str(
                resolved_env,
                "THREADSENSE_ANALYSIS_DUPLICATE_THRESHOLD",
                analysis_section,
                "duplicate_threshold",
                "0.88",
            ),
            "THREADSENSE_ANALYSIS_DUPLICATE_THRESHOLD",
        ),
    )


def _load_limits_config(
    resolved_env: Mapping[str, str],
    limits_section: Mapping[str, Any],
) -> LimitsConfig:
    return LimitsConfig(
        runtime_concurrency=_parse_positive_int(
            _read_section_str(
                resolved_env,
                "THREADSENSE_RUNTIME_CONCURRENCY",
                limits_section,
                "runtime_concurrency",
                "1",
            ),
            "THREADSENSE_RUNTIME_CONCURRENCY",
        )
    )


def load_config(
    config_path: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> AppConfig:
    resolved_env = dict(os.environ if env is None else env)
    raw_config = _read_toml(
        config_path if config_path is not None else _default_config_path(resolved_env)
    )

    app_section = _read_section(raw_config, "app")
    runtime_section = _read_section(raw_config, "runtime")
    source_section = _read_section(raw_config, "sources")
    reddit_section = _read_section(raw_config, "reddit")
    storage_section = _read_section(raw_config, "storage")
    batch_section = _read_section(raw_config, "batch")
    api_section = _read_section(raw_config, "api")
    limits_section = _read_section(raw_config, "limits")
    analysis_section = _read_section(raw_config, "analysis")
    backend, privacy_mode = _load_app_settings(resolved_env, app_section)
    runtime = _load_runtime_config(resolved_env, runtime_section)
    source_policy = _load_source_policy(resolved_env, source_section)
    reddit = _load_reddit_config(resolved_env, reddit_section)
    storage = _load_storage_config(resolved_env, storage_section)
    batch = _load_batch_config(resolved_env, batch_section)
    api = _load_api_config(resolved_env, api_section)
    limits = _load_limits_config(resolved_env, limits_section)
    analysis = _load_analysis_config(resolved_env, analysis_section)
    return AppConfig(
        inference_backend=backend,
        privacy_mode=privacy_mode,
        runtime=runtime,
        source_policy=source_policy,
        reddit=reddit,
        storage=storage,
        batch=batch,
        api=api,
        limits=limits,
        analysis=analysis,
    )


def _default_config_path(env: Mapping[str, str]) -> Path | None:
    env_path = env.get("THREADSENSE_CONFIG")
    if env_path:
        return Path(env_path)
    return DEFAULT_CONFIG_PATH if DEFAULT_CONFIG_PATH.exists() else None
