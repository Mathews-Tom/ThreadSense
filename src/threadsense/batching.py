from __future__ import annotations

import logging
from collections.abc import Mapping
from concurrent.futures import ALL_COMPLETED, FIRST_EXCEPTION, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from pathlib import Path
from time import time
from typing import Any

from threadsense.config import AppConfig
from threadsense.errors import BatchBoundaryError, ResourceLimitError, SchemaBoundaryError
from threadsense.observability import DEFAULT_METRICS, MetricsRegistry, TraceContext
from threadsense.pipeline.storage import read_json, write_json
from threadsense.workflows import RedditConnectorFactory, run_reddit_pipeline

BATCH_MANIFEST_VERSION = 1


@dataclass(frozen=True)
class BatchJob:
    job_id: str
    source_name: str
    thread_url: str
    expand_more: bool
    flat: bool
    report_format: str
    with_summary: bool
    summary_required: bool


@dataclass(frozen=True)
class BatchManifest:
    run_name: str
    jobs: list[BatchJob]
    created_at_utc: float
    manifest_version: int


@dataclass(frozen=True)
class BatchJobResult:
    job_id: str
    source_name: str
    thread_url: str
    status: str
    error: dict[str, Any] | None
    outputs: dict[str, Any] | None


@dataclass(frozen=True)
class BatchRunResult:
    run_name: str
    manifest_path: str
    started_at_utc: float
    completed_at_utc: float
    total_jobs: int
    succeeded_jobs: int
    failed_jobs: int
    worker_count: int
    jobs: list[BatchJobResult]
    reproducibility: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_kind": "batch_run",
            "schema_version": BATCH_MANIFEST_VERSION,
            "batch_run": asdict(self),
        }


def load_batch_manifest(path: Path) -> BatchManifest:
    payload = read_json(path)
    manifest_version = payload.get("manifest_version")
    if manifest_version != BATCH_MANIFEST_VERSION:
        raise SchemaBoundaryError(
            "batch manifest version is unsupported",
            details={"manifest_version": manifest_version},
        )
    jobs_data = payload.get("jobs")
    if not isinstance(jobs_data, list) or not jobs_data:
        raise SchemaBoundaryError("batch manifest jobs field is invalid")
    jobs = [job_from_dict(item) for item in jobs_data]
    ensure_unique_job_ids(jobs)
    return BatchManifest(
        run_name=required_str(payload, "run_name"),
        jobs=jobs,
        created_at_utc=required_float(payload, "created_at_utc"),
        manifest_version=manifest_version,
    )


def run_batch_manifest(
    *,
    config: AppConfig,
    logger: logging.Logger,
    manifest_path: Path,
    output_path: Path | None,
    connector_factory: RedditConnectorFactory,
    registry: MetricsRegistry = DEFAULT_METRICS,
) -> dict[str, Any]:
    manifest = load_batch_manifest(manifest_path)
    validate_manifest_limits(manifest, config.batch.max_jobs)
    worker_count = resolve_worker_count(len(manifest.jobs), config.batch.max_workers)
    started_at_utc = time()

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                run_batch_job,
                config=config,
                logger=logger,
                job=job,
                run_name=manifest.run_name,
                connector_factory=connector_factory,
                registry=registry,
            ): job
            for job in manifest.jobs
        }
        done, not_done = wait(
            future_map.keys(),
            return_when=FIRST_EXCEPTION if config.batch.fail_fast else ALL_COMPLETED,
        )
        if config.batch.fail_fast:
            for future in done:
                if future.exception() is not None:
                    for pending in not_done:
                        pending.cancel()
                    break
        job_results = [resolve_job_result(future_map[future], future) for future in future_map]

    completed_at_utc = time()
    result = BatchRunResult(
        run_name=manifest.run_name,
        manifest_path=str(manifest_path),
        started_at_utc=started_at_utc,
        completed_at_utc=completed_at_utc,
        total_jobs=len(job_results),
        succeeded_jobs=sum(1 for job in job_results if job.status == "ready"),
        failed_jobs=sum(1 for job in job_results if job.status == "error"),
        worker_count=worker_count,
        jobs=sorted(job_results, key=lambda job: job.job_id),
        reproducibility={
            "privacy_mode": config.privacy_mode.value,
            "inference_backend": config.inference_backend.value,
            "runtime_enabled": config.runtime.enabled,
            "runtime_model": config.runtime.model,
            "storage_root": str(config.storage.root_dir),
            "max_workers": config.batch.max_workers,
            "runtime_concurrency": config.limits.runtime_concurrency,
        },
    )
    resolved_output_path = output_path or (
        config.storage.root_dir / config.storage.batch_dirname / f"{manifest.run_name}.json"
    )
    write_json(resolved_output_path, result.to_dict())
    return {
        "status": "ready" if result.failed_jobs == 0 else "degraded",
        "artifact_type": "batch_run",
        "manifest_path": str(manifest_path),
        "output_path": str(resolved_output_path),
        "run_name": manifest.run_name,
        "worker_count": worker_count,
        "total_jobs": result.total_jobs,
        "succeeded_jobs": result.succeeded_jobs,
        "failed_jobs": result.failed_jobs,
        "jobs": [asdict(job_result) for job_result in result.jobs],
    }


def run_batch_job(
    *,
    config: AppConfig,
    logger: logging.Logger,
    job: BatchJob,
    run_name: str,
    connector_factory: RedditConnectorFactory,
    registry: MetricsRegistry,
) -> dict[str, Any]:
    if job.source_name != "reddit":
        raise BatchBoundaryError(
            "batch job source is unsupported",
            details={"job_id": job.job_id, "source_name": job.source_name},
        )
    trace = TraceContext.create(
        run_id=run_name,
        source_name=job.source_name,
        job_id=job.job_id,
    )
    return run_reddit_pipeline(
        config=config,
        logger=logger,
        trace=trace,
        url=job.thread_url,
        expand_more=job.expand_more,
        flat=job.flat,
        report_format=job.report_format,
        with_summary=job.with_summary,
        summary_required=job.summary_required,
        connector_factory=connector_factory,
        registry=registry,
    )


def resolve_worker_count(job_count: int, max_workers: int) -> int:
    if job_count <= 0:
        raise ResourceLimitError("batch run must contain at least one job")
    if max_workers <= 0:
        raise ResourceLimitError("batch max_workers must be greater than zero")
    return min(job_count, max_workers)


def validate_manifest_limits(manifest: BatchManifest, max_jobs: int) -> None:
    if len(manifest.jobs) > max_jobs:
        raise ResourceLimitError(
            "batch manifest exceeds configured max_jobs limit",
            details={"job_count": len(manifest.jobs), "max_jobs": max_jobs},
        )


def resolve_job_result(job: BatchJob, future: Future[dict[str, Any]]) -> BatchJobResult:
    try:
        outputs = future.result()
    except Exception as error:
        return BatchJobResult(
            job_id=job.job_id,
            source_name=job.source_name,
            thread_url=job.thread_url,
            status="error",
            error=job_error_payload(error),
            outputs=None,
        )
    return BatchJobResult(
        job_id=job.job_id,
        source_name=job.source_name,
        thread_url=job.thread_url,
        status="ready",
        error=None,
        outputs=outputs,
    )


def job_error_payload(error: Exception) -> dict[str, Any]:
    if isinstance(error, (BatchBoundaryError, ResourceLimitError, SchemaBoundaryError)):
        return error.to_dict()
    to_dict = getattr(error, "to_dict", None)
    if callable(to_dict):
        payload = to_dict()
        if isinstance(payload, dict):
            return payload
    return {
        "type": error.__class__.__name__,
        "code": "unhandled_error",
        "message": str(error),
        "details": {},
    }


def job_from_dict(payload: Mapping[str, Any]) -> BatchJob:
    return BatchJob(
        job_id=required_str(payload, "job_id"),
        source_name=required_str(payload, "source_name"),
        thread_url=required_str(payload, "thread_url"),
        expand_more=required_bool(payload, "expand_more"),
        flat=required_bool(payload, "flat"),
        report_format=required_str(payload, "report_format"),
        with_summary=required_bool(payload, "with_summary"),
        summary_required=required_bool(payload, "summary_required"),
    )


def ensure_unique_job_ids(jobs: list[BatchJob]) -> None:
    job_ids = [job.job_id for job in jobs]
    if len(set(job_ids)) != len(job_ids):
        raise SchemaBoundaryError("batch manifest contains duplicate job_id values")


def required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise SchemaBoundaryError(
            "batch manifest string field is invalid",
            details={"key": key},
        )
    return value


def required_float(payload: Mapping[str, Any], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise SchemaBoundaryError(
            "batch manifest float field is invalid",
            details={"key": key},
        )
    return value


def required_bool(payload: Mapping[str, Any], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise SchemaBoundaryError(
            "batch manifest boolean field is invalid",
            details={"key": key},
        )
    return value
