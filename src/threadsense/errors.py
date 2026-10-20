from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ThreadSenseError(Exception):
    message: str
    code: str
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.__class__.__name__,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


class ConfigurationError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="configuration_error", details=details or {})


class NetworkBoundaryError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="network_error", details=details or {})


class SchemaBoundaryError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="schema_error", details=details or {})


class InferenceBoundaryError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="inference_error", details=details or {})


class AnalysisBoundaryError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="analysis_error", details=details or {})


class BatchBoundaryError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="batch_error", details=details or {})


class ResourceLimitError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="resource_limit_error", details=details or {})


class ApiInputError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="api_input_error", details=details or {})


class MonitoringInputError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="monitoring_input_error", details=details or {})


class MonitoringStateError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="monitoring_state_error", details=details or {})


class RunClaimError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="run_claim_error", details=details or {})


class DecisionInputError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="decision_input_error", details=details or {})


class DecisionRuntimeUnavailableError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(
            message=message, code="decision_runtime_unavailable", details=details or {}
        )


class DecisionProtocolError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="decision_protocol_error", details=details or {})


class DecisionIdentityError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="decision_identity_error", details=details or {})


class DecisionArtifactError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="decision_artifact_error", details=details or {})


class RedditInputError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="reddit_input_error", details=details or {})


class RedditRequestError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="reddit_request_error", details=details or {})


class RedditResponseError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="reddit_response_error", details=details or {})


class GitHubInputError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="github_input_error", details=details or {})


class GitHubRequestError(ThreadSenseError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message=message, code="github_request_error", details=details or {})
