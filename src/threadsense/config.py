from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from threadsense import __version__
from threadsense.contracts import AbstractionLevel, DomainType, ObjectiveType
from threadsense.errors import ConfigurationError


class InferenceBackend(StrEnum):
    LOCAL_OPENAI_COMPATIBLE = "local_openai_compatible"


class PrivacyMode(StrEnum):
    LOCAL_ONLY = "local_only"


DEFAULT_CONFIG_PATH = Path("threadsense.toml")
DEFAULT_REDDIT_USER_AGENT = (
    f"threadsense/{__version__} (https://github.com/Mathews-Tom/ThreadSense)"
)


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = True
    base_url: str = "http://127.0.0.1:8080"
    chat_path: str = "/v1/chat/completions"
    model: str = "local-model"
    timeout_seconds: float = 90
    repair_retries: int = 1
    json_mode: bool = False

    @property
    def chat_endpoint(self) -> str:
        base = self.base_url.rstrip("/")
        path = self.chat_path if self.chat_path.startswith("/") else f"/{self.chat_path}"
        return f"{base}{path}"


class SourcePolicyConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled_sources: tuple[str, ...] = ("reddit", "hackernews")

    @field_validator("enabled_sources", mode="before")
    @classmethod
    def parse_sources(cls, v: Any) -> tuple[str, ...] | Any:
        if isinstance(v, str):
            sources = tuple(s.strip() for s in v.split(",") if s.strip())
            if not sources:
                raise ValueError("enabled_sources must contain at least one source")
            return sources
        if isinstance(v, (list, tuple)):
            return tuple(v)
        return v


class RedditConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    user_agent: str = DEFAULT_REDDIT_USER_AGENT
    timeout_seconds: float = 15
    max_retries: int = 2
    backoff_seconds: float = 0.5
    request_delay_seconds: float = 0.6
    listing_limit: int = 500


class HackerNewsConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    base_url: str = "https://hacker-news.firebaseio.com/v0"
    timeout_seconds: float = 15
    request_delay_seconds: float = 1.0


class StorageConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    root_dir: Path = Path(".threadsense")
    raw_dirname: str = "raw"
    normalized_dirname: str = "normalized"
    analysis_dirname: str = "analysis"
    report_dirname: str = "reports"
    batch_dirname: str = "batches"


class BatchConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_workers: int = 2
    max_jobs: int = 25
    fail_fast: bool = False


class ApiConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    host: str = "127.0.0.1"
    port: int = 8090
    max_request_bytes: int = 1048576


class LimitsConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    runtime_concurrency: int = 1


class AnalysisConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy: str = "keyword_heuristic"
    domain: DomainType = DomainType.DEVELOPER_TOOLS
    objective: ObjectiveType = ObjectiveType.GENERAL_SURVEY
    abstraction_level: AbstractionLevel = AbstractionLevel.OPERATIONAL
    duplicate_threshold: float = 0.88

    @field_validator("domain")
    @classmethod
    def validate_domain(cls, v: DomainType) -> DomainType:
        return v

    @field_validator("duplicate_threshold")
    @classmethod
    def validate_threshold(cls, v: float) -> float:
        if not (0.0 < v <= 1.0):
            raise ValueError("duplicate_threshold must be between 0 and 1")
        return v


class AppConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    inference_backend: InferenceBackend = InferenceBackend.LOCAL_OPENAI_COMPATIBLE
    privacy_mode: PrivacyMode = PrivacyMode.LOCAL_ONLY
    runtime: RuntimeConfig = RuntimeConfig()
    source_policy: SourcePolicyConfig = SourcePolicyConfig()
    reddit: RedditConfig = RedditConfig()
    hackernews: HackerNewsConfig = HackerNewsConfig()
    storage: StorageConfig = StorageConfig()
    batch: BatchConfig = BatchConfig()
    api: ApiConfig = ApiConfig()
    limits: LimitsConfig = LimitsConfig()
    analysis: AnalysisConfig = AnalysisConfig()


# Maps environment variable names to their (section, field) path in the config dict.
_ENV_MAP: dict[str, tuple[str, ...]] = {
    "THREADSENSE_INFERENCE_BACKEND": ("inference_backend",),
    "THREADSENSE_PRIVACY_MODE": ("privacy_mode",),
    "THREADSENSE_RUNTIME_ENABLED": ("runtime", "enabled"),
    "THREADSENSE_RUNTIME_BASE_URL": ("runtime", "base_url"),
    "THREADSENSE_RUNTIME_CHAT_PATH": ("runtime", "chat_path"),
    "THREADSENSE_RUNTIME_MODEL": ("runtime", "model"),
    "THREADSENSE_RUNTIME_TIMEOUT_SECONDS": ("runtime", "timeout_seconds"),
    "THREADSENSE_RUNTIME_REPAIR_RETRIES": ("runtime", "repair_retries"),
    "THREADSENSE_RUNTIME_JSON_MODE": ("runtime", "json_mode"),
    "THREADSENSE_ENABLED_SOURCES": ("source_policy", "enabled_sources"),
    "THREADSENSE_REDDIT_USER_AGENT": ("reddit", "user_agent"),
    "THREADSENSE_REDDIT_TIMEOUT": ("reddit", "timeout_seconds"),
    "THREADSENSE_REDDIT_MAX_RETRIES": ("reddit", "max_retries"),
    "THREADSENSE_REDDIT_BACKOFF": ("reddit", "backoff_seconds"),
    "THREADSENSE_REDDIT_REQUEST_DELAY": ("reddit", "request_delay_seconds"),
    "THREADSENSE_REDDIT_LISTING_LIMIT": ("reddit", "listing_limit"),
    "THREADSENSE_HN_BASE_URL": ("hackernews", "base_url"),
    "THREADSENSE_HN_TIMEOUT": ("hackernews", "timeout_seconds"),
    "THREADSENSE_HN_REQUEST_DELAY": ("hackernews", "request_delay_seconds"),
    "THREADSENSE_STORAGE_ROOT": ("storage", "root_dir"),
    "THREADSENSE_STORAGE_RAW_DIR": ("storage", "raw_dirname"),
    "THREADSENSE_STORAGE_NORMALIZED_DIR": ("storage", "normalized_dirname"),
    "THREADSENSE_STORAGE_ANALYSIS_DIR": ("storage", "analysis_dirname"),
    "THREADSENSE_STORAGE_REPORT_DIR": ("storage", "report_dirname"),
    "THREADSENSE_STORAGE_BATCH_DIR": ("storage", "batch_dirname"),
    "THREADSENSE_BATCH_MAX_WORKERS": ("batch", "max_workers"),
    "THREADSENSE_BATCH_MAX_JOBS": ("batch", "max_jobs"),
    "THREADSENSE_BATCH_FAIL_FAST": ("batch", "fail_fast"),
    "THREADSENSE_API_HOST": ("api", "host"),
    "THREADSENSE_API_PORT": ("api", "port"),
    "THREADSENSE_API_MAX_REQUEST_BYTES": ("api", "max_request_bytes"),
    "THREADSENSE_RUNTIME_CONCURRENCY": ("limits", "runtime_concurrency"),
    "THREADSENSE_ANALYSIS_STRATEGY": ("analysis", "strategy"),
    "THREADSENSE_ANALYSIS_DOMAIN": ("analysis", "domain"),
    "THREADSENSE_ANALYSIS_OBJECTIVE": ("analysis", "objective"),
    "THREADSENSE_ANALYSIS_LEVEL": ("analysis", "abstraction_level"),
    "THREADSENSE_ANALYSIS_DUPLICATE_THRESHOLD": ("analysis", "duplicate_threshold"),
}

# Maps TOML section names to config model keys, with field renames where needed.
_TOML_SECTION_MAP: dict[str, str] = {
    "runtime": "runtime",
    "sources": "source_policy",
    "reddit": "reddit",
    "hackernews": "hackernews",
    "storage": "storage",
    "batch": "batch",
    "api": "api",
    "limits": "limits",
    "analysis": "analysis",
}

_TOML_APP_FIELDS = ("inference_backend", "privacy_mode")

# sources.enabled -> source_policy.enabled_sources
_TOML_FIELD_RENAMES: dict[tuple[str, str], str] = {
    ("sources", "enabled"): "enabled_sources",
}


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


def _default_config_path(env: Mapping[str, str]) -> Path | None:
    env_path = env.get("THREADSENSE_CONFIG")
    if env_path:
        return Path(env_path)
    return DEFAULT_CONFIG_PATH if DEFAULT_CONFIG_PATH.exists() else None


def _build_config_dict(raw: dict[str, Any], env: Mapping[str, str]) -> dict[str, Any]:
    config: dict[str, Any] = {}

    # Extract [app] top-level fields.
    app_section = raw.get("app", {})
    if isinstance(app_section, dict):
        for field in _TOML_APP_FIELDS:
            if field in app_section:
                config[field] = app_section[field]

    # Extract remaining TOML sections.
    for toml_section, config_key in _TOML_SECTION_MAP.items():
        section_data = raw.get(toml_section, {})
        if not isinstance(section_data, dict) or not section_data:
            continue
        nested: dict[str, Any] = {}
        for raw_field, value in section_data.items():
            field_name = str(raw_field)
            target_key: str = _TOML_FIELD_RENAMES.get((toml_section, field_name), field_name)
            nested[target_key] = value
        config[config_key] = nested

    # Overlay environment variables (env vars take precedence over TOML).
    for env_key, config_path in _ENV_MAP.items():
        value = env.get(env_key)
        if value is None:
            continue
        if len(config_path) == 1:
            config[config_path[0]] = value
        else:
            section, field = config_path
            config.setdefault(section, {})[field] = value

    return config


def load_config(
    config_path: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> AppConfig:
    resolved_env = dict(os.environ if env is None else env)
    raw = _read_toml(config_path if config_path is not None else _default_config_path(resolved_env))
    config_dict = _build_config_dict(raw, resolved_env)
    try:
        return AppConfig.model_validate(config_dict)
    except Exception as exc:
        raise ConfigurationError(str(exc)) from exc
