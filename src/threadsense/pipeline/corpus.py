from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import time
from typing import NamedTuple

from threadsense.contracts import DomainType
from threadsense.errors import AnalysisBoundaryError
from threadsense.models.analysis import AnalysisFinding, RepresentativeQuote, ThreadAnalysis
from threadsense.models.canonical import load_canonical_thread
from threadsense.models.corpus import (
    CORPUS_ENGINE_VERSION,
    CORPUS_SCHEMA_VERSION,
    CorpusAnalysis,
    CorpusManifest,
    CorpusProvenance,
    CrossThreadEvidence,
    CrossThreadFinding,
    TemporalTrend,
    TrendPeriod,
)
from threadsense.pipeline.storage import load_analysis_artifact

_SEVERITY_ORDER = {"low": 1, "medium": 2, "high": 3}


class _TrendBucket(NamedTuple):
    theme_key: str
    period: str


@dataclass(frozen=True)
class _ThemeObservation:
    analysis: ThreadAnalysis
    finding: AnalysisFinding
    top_quote: RepresentativeQuote | None
    thread_timestamp: float


def build_corpus_manifest(
    *,
    name: str,
    description: str,
    domain: DomainType,
    analysis_paths: list[Path],
    source_filter: str | None,
) -> CorpusManifest:
    if not analysis_paths:
        raise AnalysisBoundaryError("corpus manifest requires at least one analysis artifact")

    analyses = [load_analysis_artifact(path) for path in analysis_paths]
    filtered = [
        (path, analysis)
        for path, analysis in zip(analysis_paths, analyses, strict=True)
        if source_filter is None or analysis.source_name == source_filter
    ]
    if not filtered:
        raise AnalysisBoundaryError(
            "corpus manifest source filter excluded every analysis artifact",
            details={"source_filter": source_filter},
        )

    corpus_id = slugify_name(name)
    return CorpusManifest(
        corpus_id=corpus_id,
        name=name,
        description=description,
        source_filter=source_filter,
        domain=domain,
        created_at_utc=time(),
        thread_ids=[analysis.thread_id for _, analysis in filtered],
        analysis_artifact_paths=[str(path) for path, _ in filtered],
    )


def build_corpus_analysis(
    manifest: CorpusManifest,
    *,
    manifest_path: Path,
    evidence_limit: int,
    period: TrendPeriod,
) -> CorpusAnalysis:
    analysis_paths = [Path(path) for path in manifest.analysis_artifact_paths]
    analyses = [load_analysis_artifact(path) for path in analysis_paths]
    cross_thread_findings = aggregate_findings(analyses, evidence_limit=evidence_limit)
    temporal_trends = build_temporal_trends(analyses, period=period)
    return CorpusAnalysis(
        corpus_id=manifest.corpus_id,
        name=manifest.name,
        domain=manifest.domain,
        thread_count=len(analyses),
        total_comments=sum(analysis.total_comments for analysis in analyses),
        cross_thread_findings=cross_thread_findings,
        theme_frequency={
            finding.theme_key: finding.thread_count for finding in cross_thread_findings
        },
        temporal_trends=temporal_trends,
        provenance=CorpusProvenance(
            manifest_path=str(manifest_path),
            input_analysis_paths=[str(path) for path in analysis_paths],
            generated_at_utc=time(),
            schema_version=CORPUS_SCHEMA_VERSION,
            corpus_version=CORPUS_ENGINE_VERSION,
        ),
    )


def aggregate_findings(
    analyses: list[ThreadAnalysis],
    *,
    evidence_limit: int,
) -> list[CrossThreadFinding]:
    theme_index: dict[str, list[_ThemeObservation]] = defaultdict(list)
    for analysis in analyses:
        timestamp = thread_timestamp_for_analysis(analysis)
        for finding in analysis.findings:
            theme_index[finding.theme_key].append(
                _ThemeObservation(
                    analysis=analysis,
                    finding=finding,
                    top_quote=select_top_quote(finding),
                    thread_timestamp=timestamp,
                )
            )

    aggregated: list[CrossThreadFinding] = []
    for theme_key, observations in theme_index.items():
        theme_label = observations[0].finding.theme_label
        severity = aggregate_severity([item.finding.severity for item in observations])
        evidence = [
            CrossThreadEvidence(
                thread_id=item.analysis.thread_id,
                thread_title=item.analysis.title,
                finding_severity=item.finding.severity,
                comment_count=item.finding.comment_count,
                top_quote=item.top_quote,
            )
            for item in observations
            if item.top_quote is not None
        ]
        evidence.sort(
            key=lambda item: (
                _SEVERITY_ORDER.get(item.finding_severity, 0),
                item.comment_count,
                item.top_quote.score,
            ),
            reverse=True,
        )
        aggregated.append(
            CrossThreadFinding(
                theme_key=theme_key,
                theme_label=theme_label,
                severity=severity,
                thread_count=len({item.analysis.thread_id for item in observations}),
                total_comment_count=sum(item.finding.comment_count for item in observations),
                top_evidence=evidence[:evidence_limit],
            )
        )

    aggregated.sort(
        key=lambda item: (
            item.thread_count,
            item.total_comment_count,
            _SEVERITY_ORDER.get(item.severity, 0),
            item.theme_key,
        ),
        reverse=True,
    )
    return aggregated


def build_temporal_trends(
    analyses: list[ThreadAnalysis],
    *,
    period: TrendPeriod,
) -> list[TemporalTrend]:
    buckets: dict[_TrendBucket, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    thread_counts: dict[_TrendBucket, set[str]] = defaultdict(set)
    for analysis in analyses:
        timestamp = thread_timestamp_for_analysis(analysis)
        period_key = format_period(timestamp, period)
        for finding in analysis.findings:
            bucket = _TrendBucket(finding.theme_key, period_key)
            buckets[bucket][finding.severity] += 1
            thread_counts[bucket].add(analysis.thread_id)

    trends = [
        TemporalTrend(
            theme_key=bucket.theme_key,
            period=bucket.period,
            thread_count=len(thread_counts[bucket]),
            severity_distribution=dict(sorted(distribution.items())),
        )
        for bucket, distribution in buckets.items()
    ]
    trends.sort(key=lambda item: (item.period, item.thread_count, item.theme_key), reverse=True)
    return trends


def aggregate_severity(severities: list[str]) -> str:
    high_count = sum(1 for severity in severities if severity == "high")
    medium_or_high_count = sum(1 for severity in severities if severity in {"medium", "high"})
    if high_count >= 3:
        return "high"
    if medium_or_high_count >= 3:
        return "medium"
    return max(severities, key=lambda severity: _SEVERITY_ORDER.get(severity, 0), default="low")


def select_top_quote(finding: AnalysisFinding) -> RepresentativeQuote | None:
    if not finding.quotes:
        return None
    return max(finding.quotes, key=lambda quote: (quote.score, len(quote.body_excerpt)))


def thread_timestamp_for_analysis(analysis: ThreadAnalysis) -> float:
    thread = load_canonical_thread(Path(analysis.provenance.normalized_artifact_path))
    if thread.comments:
        return min(comment.created_utc for comment in thread.comments)
    return thread.provenance.retrieved_at_utc


def format_period(timestamp: float, period: TrendPeriod) -> str:
    dt = datetime.fromtimestamp(timestamp, tz=UTC)
    if period is TrendPeriod.WEEK:
        iso_year, iso_week, _ = dt.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    return dt.strftime("%Y-%m")


def slugify_name(name: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in name).strip("-")
    collapsed = "-".join(part for part in cleaned.split("-") if part)
    return collapsed or "corpus"
