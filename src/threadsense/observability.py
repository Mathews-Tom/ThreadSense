from __future__ import annotations

import json
import logging
import threading
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from time import perf_counter

from threadsense.errors import ThreadSenseError

MetricLabels = Mapping[str, str]


def normalize_metric_label(value: str) -> str:
    normalized = "".join(
        character.lower() if character.isalnum() else "_" for character in value.strip()
    )
    collapsed = "_".join(part for part in normalized.split("_") if part)
    return collapsed or "unknown"


def normalize_metric_labels(labels: Mapping[str, str]) -> dict[str, str]:
    return {
        normalize_metric_label(key): normalize_metric_label(value)
        for key, value in sorted(labels.items())
    }


@dataclass(frozen=True)
class TraceContext:
    trace_id: str
    run_id: str
    source_name: str
    job_id: str | None = None

    @classmethod
    def create(
        cls,
        run_id: str,
        source_name: str,
        job_id: str | None = None,
    ) -> TraceContext:
        return cls(
            trace_id=uuid.uuid4().hex,
            run_id=run_id,
            source_name=source_name,
            job_id=job_id,
        )

    def to_dict(self) -> dict[str, str]:
        payload = {
            "trace_id": self.trace_id,
            "run_id": self.run_id,
            "source_name": self.source_name,
        }
        if self.job_id is not None:
            payload["job_id"] = self.job_id
        return payload


@dataclass(frozen=True)
class MetricSample:
    labels: dict[str, str]
    value: float


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}
        self._latencies: dict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = {}

    def increment(self, name: str, labels: Mapping[str, str]) -> None:
        key = self._metric_key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + 1

    def observe_seconds(self, name: str, labels: Mapping[str, str], value: float) -> None:
        key = self._metric_key(name, labels)
        with self._lock:
            self._latencies.setdefault(key, []).append(value)

    def snapshot(self) -> dict[str, list[MetricSample]]:
        with self._lock:
            counters = [
                MetricSample(labels=dict(key[1]), value=float(value))
                for key, value in sorted(self._counters.items())
            ]
            latencies = [
                MetricSample(labels=dict(key[1]), value=sum(values) / len(values))
                for key, values in sorted(self._latencies.items())
            ]
        return {
            "threadsense_stage_total": counters,
            "threadsense_stage_seconds_avg": latencies,
        }

    def render_prometheus(self) -> str:
        snapshot = self.snapshot()
        lines = [
            "# TYPE threadsense_stage_total counter",
            "# TYPE threadsense_stage_seconds_avg gauge",
        ]
        for metric_name, samples in snapshot.items():
            for sample in samples:
                label_body = ",".join(
                    f'{key}="{value}"' for key, value in sorted(sample.labels.items())
                )
                lines.append(f"{metric_name}{{{label_body}}} {sample.value:.6f}")
        return "\n".join(lines) + "\n"

    def _metric_key(
        self,
        name: str,
        labels: Mapping[str, str],
    ) -> tuple[str, tuple[tuple[str, str], ...]]:
        normalized_labels = normalize_metric_labels(labels)
        return name, tuple(sorted(normalized_labels.items()))


DEFAULT_METRICS = MetricsRegistry()


def emit_log(
    logger: logging.Logger,
    event: str,
    trace: TraceContext,
    **fields: object,
) -> None:
    payload: dict[str, object] = {"event": event, **trace.to_dict(), **fields}
    logger.info(json.dumps(payload, sort_keys=True))


@contextmanager
def observe_stage(
    *,
    registry: MetricsRegistry,
    logger: logging.Logger,
    trace: TraceContext,
    stage: str,
    labels: Mapping[str, str] | None = None,
) -> Iterator[None]:
    start = perf_counter()
    base_labels = {
        "stage": stage,
        "source_name": trace.source_name,
        **(dict(labels) if labels is not None else {}),
    }
    emit_log(logger, "stage_started", trace, stage=stage, labels=base_labels)
    try:
        yield
    except ThreadSenseError as error:
        latency = perf_counter() - start
        failure_labels = {**base_labels, "outcome": "error", "error_code": error.code}
        registry.increment("threadsense_stage_total", failure_labels)
        registry.observe_seconds("threadsense_stage_seconds_avg", failure_labels, latency)
        emit_log(
            logger,
            "stage_failed",
            trace,
            stage=stage,
            latency_seconds=round(latency, 6),
            error=error.to_dict(),
        )
        raise
    else:
        latency = perf_counter() - start
        success_labels = {**base_labels, "outcome": "ready"}
        registry.increment("threadsense_stage_total", success_labels)
        registry.observe_seconds("threadsense_stage_seconds_avg", success_labels, latency)
        emit_log(
            logger,
            "stage_completed",
            trace,
            stage=stage,
            latency_seconds=round(latency, 6),
        )
