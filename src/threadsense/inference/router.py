from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from threadsense.config import AppConfig
from threadsense.domains import DomainVocabulary
from threadsense.errors import InferenceBoundaryError, NetworkBoundaryError, SchemaBoundaryError
from threadsense.inference.contracts import InferenceRequest, InferenceResponse, InferenceTask
from threadsense.inference.local_runtime import LocalRuntimeClient
from threadsense.inference.prompts import build_task_request, build_vocabulary_expansion_request
from threadsense.models.analysis import ThreadAnalysis
from threadsense.models.canonical import Thread
from threadsense.models.corpus import CorpusAnalysis


class InferenceClient(Protocol):
    def complete(
        self,
        request: InferenceRequest,
        opener: Any = ...,
        *,
        analysis: ThreadAnalysis | None = ...,
        corpus: CorpusAnalysis | None = ...,
    ) -> InferenceResponse: ...


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
            corpus=None,
            required=required,
            repair_retries=self._config.runtime.repair_retries,
        )

        try:
            return self._client_factory(self._config).complete(
                request,
                analysis=analysis,
                corpus=None,
            )
        except (InferenceBoundaryError, NetworkBoundaryError, SchemaBoundaryError) as error:
            if required:
                raise
            return fallback_response(analysis, task, str(error))

    def run_vocabulary_expansion(
        self,
        thread: Thread,
        vocabulary: DomainVocabulary,
    ) -> InferenceResponse:
        if not self._config.runtime.enabled:
            return _empty_vocabulary_expansion("runtime_disabled")

        request = build_vocabulary_expansion_request(
            thread=thread,
            theme_rules=vocabulary.theme_rules,
            required=False,
            repair_retries=self._config.runtime.repair_retries,
        )
        try:
            return self._client_factory(self._config).complete(request)
        except (InferenceBoundaryError, NetworkBoundaryError, SchemaBoundaryError) as error:
            return _empty_vocabulary_expansion(str(error))

    def run_corpus_task(
        self,
        corpus: CorpusAnalysis,
        task: InferenceTask,
        required: bool,
    ) -> InferenceResponse:
        if task is not InferenceTask.CORPUS_SYNTHESIS:
            raise InferenceBoundaryError(
                "corpus inference requires corpus_synthesis task",
                details={"task": task.value},
            )
        if not self._config.runtime.enabled:
            if required:
                raise InferenceBoundaryError("local inference is disabled by configuration")
            return fallback_corpus_response(corpus, "runtime_disabled")

        request = build_task_request(
            task=task,
            analysis=None,
            corpus=corpus,
            required=required,
            repair_retries=self._config.runtime.repair_retries,
        )
        try:
            return self._client_factory(self._config).complete(
                request,
                analysis=None,
                corpus=corpus,
            )
        except (InferenceBoundaryError, NetworkBoundaryError, SchemaBoundaryError) as error:
            if required:
                raise
            return fallback_corpus_response(corpus, str(error))


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


def _empty_vocabulary_expansion(failure_reason: str) -> InferenceResponse:
    return InferenceResponse(
        task=InferenceTask.VOCABULARY_EXPANSION,
        provider="deterministic_fallback",
        model=None,
        finish_reason=None,
        output={"existing_themes": {}, "new_themes": {}},
        used_fallback=True,
        degraded=True,
        failure_reason=failure_reason,
    )


def fallback_corpus_response(
    corpus: CorpusAnalysis,
    failure_reason: str,
) -> InferenceResponse:
    first_finding = corpus.cross_thread_findings[0] if corpus.cross_thread_findings else None
    cited_thread_ids = (
        [evidence.thread_id for evidence in first_finding.top_evidence[:3]]
        if first_finding is not None
        else []
    )
    return InferenceResponse(
        task=InferenceTask.CORPUS_SYNTHESIS,
        provider="deterministic_fallback",
        model=None,
        finish_reason=None,
        output={
            "headline": (
                f"{first_finding.theme_label.title()} is the dominant cross-thread pattern"
                if first_finding is not None
                else f"Deterministic corpus summary for {corpus.name}"
            ),
            "key_patterns": [
                f"{finding.theme_key} appears in {finding.thread_count} threads"
                for finding in corpus.cross_thread_findings[:5]
            ],
            "cited_thread_ids": cited_thread_ids,
            "recommended_actions": [
                f"Review cross-thread evidence for {finding.theme_key}"
                for finding in corpus.cross_thread_findings[:3]
            ],
            "confidence_note": (
                f"Built from {corpus.thread_count} threads without runtime synthesis."
            ),
        },
        used_fallback=True,
        degraded=True,
        failure_reason=failure_reason,
    )
