from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from threadsense.config import AppConfig
from threadsense.errors import InferenceBoundaryError, NetworkBoundaryError, SchemaBoundaryError
from threadsense.inference.contracts import InferenceRequest, InferenceResponse, InferenceTask
from threadsense.inference.local_runtime import LocalRuntimeClient
from threadsense.inference.prompts import build_task_request
from threadsense.models.analysis import ThreadAnalysis


class InferenceClient(Protocol):
    def complete(self, request: InferenceRequest) -> InferenceResponse: ...


InferenceClientFactory = Callable[[AppConfig], InferenceClient]


class InferenceRouter:
    def __init__(
        self,
        config: AppConfig,
        client_factory: InferenceClientFactory | None = None,
    ) -> None:
        self._config = config
        self._client_factory = client_factory or (
            lambda app_config: LocalRuntimeClient(app_config.runtime)
        )

    def run_analysis_task(
        self,
        analysis: ThreadAnalysis,
        task: InferenceTask,
        required: bool,
    ) -> InferenceResponse:
        if not self._config.runtime.enabled:
            if required:
                raise InferenceBoundaryError("local inference is disabled by configuration")
            return fallback_response(analysis, task, "runtime_disabled")

        request = build_task_request(
            task=task,
            analysis=analysis,
            required=required,
            repair_retries=self._config.runtime.repair_retries,
        )

        try:
            return self._client_factory(self._config).complete(request)
        except (InferenceBoundaryError, NetworkBoundaryError, SchemaBoundaryError) as error:
            if required:
                raise
            return fallback_response(analysis, task, str(error))


def fallback_response(
    analysis: ThreadAnalysis,
    task: InferenceTask,
    failure_reason: str,
) -> InferenceResponse:
    output: dict[str, Any]
    if task is InferenceTask.ANALYSIS_SUMMARY:
        first_finding = analysis.findings[0] if analysis.findings else None
        headline = (
            f"{first_finding.theme_label.title()} leads the thread"
            if first_finding is not None
            else f"Deterministic analysis for {analysis.title}"
        )
        summary = (
            f"Top themes: {', '.join(finding.theme_key for finding in analysis.findings[:3])}."
            if analysis.findings
            else "No findings were available from deterministic analysis."
        )
        output = {
            "headline": headline,
            "summary": summary,
            "cited_theme_keys": [finding.theme_key for finding in analysis.findings[:3]],
            "cited_comment_ids": first_finding.evidence_comment_ids[:5] if first_finding else [],
            "next_steps": [
                f"Review {finding.theme_key} evidence group" for finding in analysis.findings[:3]
            ],
        }
    elif task is InferenceTask.FINDING_CLASSIFICATION:
        output = {
            "classifications": [
                {
                    "theme_key": finding.theme_key,
                    "category": finding.theme_label,
                    "confidence": 1.0,
                }
                for finding in analysis.findings[:5]
            ]
        }
    else:
        output = {
            "executive_summary": (
                "Deterministic-only output for "
                f"{analysis.title} because local inference was unavailable."
            ),
            "caveats": ["Local inference was unavailable."],
            "cited_theme_keys": [finding.theme_key for finding in analysis.findings[:3]],
        }

    return InferenceResponse(
        task=task,
        provider="deterministic_fallback",
        model=None,
        finish_reason=None,
        output=output,
        used_fallback=True,
        degraded=True,
        failure_reason=failure_reason,
    )
