"""Additive source capture: only new tables are indexed or populated by migration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceSpec:
    name: str
    primary_key: tuple[str, ...]
    columns: tuple[str, ...]
    semantic_columns: tuple[str, ...]
    watch_column: str | None = "watch_id"


def _spec(
    name: str,
    primary_key: str,
    columns: str,
    *,
    semantic: str | None = None,
    shared: bool = False,
) -> SourceSpec:
    fields = tuple(columns.split())
    return SourceSpec(
        name,
        tuple(primary_key.split()),
        fields,
        tuple(semantic.split()) if semantic is not None else fields,
        None if shared else "watch_id",
    )


# The primary keys below already have indexes in the historical schema. Never add
# an index to an old table: bootstrap walks these exact, immutable tuples.
LEGACY_SPECS: tuple[SourceSpec, ...] = (
    _spec(
        "watch_alert_reports",
        "report_id",
        "report_id watch_id sweep_run_id cohort_id window_id baseline_window_id status "
        "payload_json payload_sha256 json_path markdown_path run_id created_at_utc",
    ),
    _spec(
        "watch_alert_completions",
        "report_id",
        "report_id json_sha256 markdown_sha256 completed_at_utc run_id",
        shared=True,
    ),
    _spec("watch_alert_events", "alert_id", "alert_id watch_id report_id payload_json"),
    _spec(
        "watch_alert_seen",
        "watch_id evidence_key",
        "watch_id evidence_key source_report_id first_window_id",
    ),
    _spec("watch_alert_cohorts", "cohort_id", "cohort_id watch_id basis_json created_at_utc"),
)
SOURCE_SPECS: tuple[SourceSpec, ...] = (
    *LEGACY_SPECS,
    _spec("watches", "watch_id", "watch_id current_revision created_at_utc updated_at_utc"),
    _spec(
        "watch_revisions",
        "watch_id revision",
        "watch_id revision profile_version content_sha256 profile_json created_at_utc",
    ),
    _spec(
        "watch_runs",
        "run_id",
        "run_id watch_id profile_revision owner_token_sha256 status claimed_at_utc "
        "lease_expires_at_utc finished_at_utc recovered_by_run_id",
        semantic="run_id watch_id profile_revision status claimed_at_utc "
        "finished_at_utc recovered_by_run_id",
    ),
    _spec(
        "watch_threads",
        "watch_id source_thread_id",
        "watch_id source_thread_id thread_url subreddit title source_created_utc selected "
        "rejection_reason first_discovered_at_utc first_discovered_run_id last_discovered_at_utc "
        "last_attempted_at_utc last_refreshed_at_utc last_acquisition_id current_revision_id",
        semantic="watch_id source_thread_id thread_url subreddit title source_created_utc "
        "first_discovered_at_utc first_discovered_run_id last_acquisition_id current_revision_id",
    ),
    _spec(
        "watch_acquisitions",
        "acquisition_id",
        "acquisition_id watch_id source_thread_id run_id observed_at_utc raw_path raw_sha256 "
        "normalized_path normalized_sha256 completeness cache_status parsed_comment_count "
        "reported_comment_count absent_known_comment_count revision_id",
    ),
    _spec(
        "watch_comments",
        "watch_id source_thread_id comment_id",
        "watch_id source_thread_id comment_id current_text_sha256 source_created_utc novelty "
        "first_observed_at_utc first_observed_run_id first_acquisition_id last_seen_at_utc "
        "last_seen_acquisition_id",
        semantic="watch_id source_thread_id comment_id current_text_sha256 source_created_utc "
        "novelty first_observed_at_utc first_observed_run_id first_acquisition_id",
    ),
    _spec(
        "watch_comment_texts",
        "watch_id source_thread_id comment_id acquisition_id",
        "watch_id source_thread_id comment_id acquisition_id text_sha256 change observed_at_utc",
    ),
    _spec(
        "watch_thread_revisions",
        "revision_id",
        "revision_id watch_id source_thread_id sequence previous_revision_id substantive_sha256 "
        "thread_text_sha256 snapshot_path snapshot_sha256 acquisition_id run_id created_at_utc "
        "changes_json",
    ),
    _spec(
        "watch_revision_analyses",
        "revision_id",
        "revision_id watch_id analysis_path analysis_sha256 run_id analyzed_at_utc",
    ),
    _spec("watch_sweeps", "run_id", "run_id watch_id outcome summary_json finished_at_utc"),
    _spec(
        "watch_digest_entries",
        "entry_id",
        "entry_id watch_id source_thread_id revision_id kind theme_key payload_json created_at_utc "
        "created_run_id digest_id presented_at_utc",
        semantic="entry_id watch_id source_thread_id revision_id kind theme_key payload_json "
        "created_at_utc created_run_id",
    ),
    _spec(
        "watch_digests",
        "digest_id",
        "digest_id watch_id sequence run_id status entry_ids_json backlog_json json_path "
        "markdown_path json_sha256 markdown_sha256 created_at_utc written_at_utc",
        semantic="digest_id watch_id sequence run_id entry_ids_json backlog_json created_at_utc",
    ),
    _spec(
        "watch_issue_policies",
        "policy_sha256",
        "policy_sha256 policy_json created_at_utc",
        shared=True,
    ),
    _spec(
        "watch_issues",
        "issue_id",
        "issue_id watch_id home_thread_id created_sequence anchor_utc created_at_utc "
        "created_run_id policy_sha256",
    ),
    _spec(
        "watch_issue_entries",
        "entry_id",
        "entry_id watch_id source_thread_id theme_key key_phrases_json ingested_at_utc run_id",
    ),
    _spec(
        "watch_issue_reports",
        "report_id",
        "report_id watch_id source_thread_id comment_id record_sequence first_text_sha256 "
        "author_sha256 community permalink source_created_utc lineage copy_of_report_id "
        "first_entry_id recorded_at_utc run_id",
    ),
    _spec(
        "watch_issue_evidence",
        "report_id entry_id",
        "report_id entry_id watch_id text_sha256 revision_id snapshot_sha256 links_json "
        "recorded_at_utc",
    ),
    _spec(
        "watch_issue_updates",
        "update_id",
        "update_id watch_id source_thread_id report_id entry_id text_sha256 kind cue "
        "recorded_at_utc run_id",
    ),
    _spec(
        "watch_issue_memberships",
        "event_id",
        "event_id watch_id source_thread_id issue_id previous_issue_id action basis reason "
        "report_id policy_sha256 run_id created_at_utc",
    ),
    _spec(
        "watch_issue_ingestions",
        "run_id",
        "run_id watch_id policy_sha256 summary_json created_at_utc",
    ),
    _spec("watch_digest_issue_views", "digest_id", "digest_id watch_id view_json created_at_utc"),
    _spec(
        "watch_ranking_policies",
        "policy_id",
        "policy_id name policy_json created_at_utc",
        shared=True,
    ),
    _spec(
        "watch_ranking_selections",
        "watch_id sequence",
        "watch_id sequence policy_id selected_at_utc",
    ),
    _spec(
        "watch_digest_rankings",
        "digest_id",
        "digest_id watch_id profile_revision policy_id preference_sequence created_at_utc",
    ),
    _spec(
        "watch_feedback_keys",
        "watch_id idempotency_key",
        "watch_id idempotency_key feedback_id kind content_sha256 recorded_at_utc",
    ),
    _spec(
        "watch_preference_events",
        "feedback_id",
        "feedback_id watch_id sequence value target_kind target_id source_thread_id theme_key "
        "scope_kind scope_key note recorded_at_utc",
    ),
    _spec(
        "watch_correction_events",
        "feedback_id",
        "feedback_id watch_id sequence value target_kind target_id source_thread_id "
        "theme_key note recorded_at_utc",
    ),
)
SPEC_BY_KIND = {spec.name: spec for spec in SOURCE_SPECS}

ORDER_COLUMN_BY_KIND: dict[str, str] = {
    "watches": "current_revision",
    "watch_revisions": "revision",
    "watch_sweeps": "finished_at_utc",
    "watch_ranking_selections": "sequence",
    "watch_preference_events": "sequence",
    "watch_correction_events": "sequence",
}


def _key(spec: SourceSpec, prefix: str) -> str:
    return "json_array(" + ",".join(f'{prefix}."{c}"' for c in spec.primary_key) + ")"


def _payload(spec: SourceSpec, prefix: str) -> str:
    pairs = [f"'{c}',{prefix}.\"{c}\"" for c in spec.columns]
    if spec.name == "watch_alert_reports":
        pairs.append(f"'_legacy_ordinal',{prefix}.rowid")
    return "json_object(" + ",".join(pairs) + ")"


def _capture(spec: SourceSpec, prefix: str, *, missing_only: bool = False) -> str:
    key = _key(spec, prefix)
    watch = f'{prefix}."watch_id"' if spec.watch_column else "NULL"
    order_column = ORDER_COLUMN_BY_KIND.get(spec.name)
    order = f'{prefix}."{order_column}"' if order_column else "0"
    condition = (
        (
            f" WHERE NOT EXISTS (SELECT 1 FROM alert_source_heads "
            f"WHERE kind='{spec.name}' AND source_key={key})"
        )
        if missing_only
        else ""
    )
    statement = (
        "INSERT INTO alert_source_versions"
        "(kind,source_key,semantic_version,watch_id,source_order,data_json) "
        f"SELECT '{spec.name}',{key},COALESCE((SELECT semantic_version FROM alert_source_heads "
        f"WHERE kind='{spec.name}' AND source_key={key}),0)+1,{watch},{order},"
        f"{_payload(spec, prefix)}"
        f"{condition}; "
        "INSERT INTO alert_source_heads(kind,source_key,semantic_version,sequence) "
        f"SELECT kind,source_key,semantic_version,sequence FROM alert_source_versions "
        f"WHERE kind='{spec.name}' AND source_key={key} ORDER BY sequence DESC LIMIT 1 "
        "ON CONFLICT(kind,source_key) DO UPDATE SET "
        "semantic_version=excluded.semantic_version,sequence=excluded.sequence; "
    )
    if spec.name == "watch_alert_reports":
        statement += (
            "INSERT OR IGNORE INTO alert_legacy_reports"
            "(report_id,watch_id,observed_ordinal,completed) "
            f"VALUES ({prefix}.report_id,{prefix}.watch_id,{prefix}.rowid,"
            f"EXISTS(SELECT 1 FROM watch_alert_completions WHERE report_id={prefix}.report_id)); "
        )
    if spec.name == "watch_alert_completions":
        statement += (
            f"UPDATE alert_legacy_reports SET completed=1 WHERE report_id={prefix}.report_id; "
        )
    return statement


def _triggers(spec: SourceSpec) -> tuple[str, ...]:
    changed = " OR ".join(f'OLD."{c}" IS NOT NEW."{c}"' for c in spec.semantic_columns)
    key_changed = " OR ".join(f'OLD."{c}" IS NOT NEW."{c}"' for c in spec.primary_key)
    return (
        f"CREATE TRIGGER alert_capture_{spec.name}_insert AFTER INSERT ON {spec.name} "
        f"BEGIN {_capture(spec, 'NEW')} END",
        f"CREATE TRIGGER alert_capture_{spec.name}_update AFTER UPDATE ON {spec.name} "
        f"WHEN {changed} BEGIN {_capture(spec, 'OLD', missing_only=True)}"
        f"{_capture(spec, 'NEW')} END",
        f"CREATE TRIGGER alert_capture_{spec.name}_key BEFORE UPDATE ON {spec.name} "
        f"WHEN {key_changed} BEGIN "
        "SELECT RAISE(ABORT,'alert source primary key is immutable'); END",
    )


SOURCE_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """CREATE TABLE alert_source_versions (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL, source_key TEXT NOT NULL, semantic_version INTEGER NOT NULL,
        watch_id TEXT, source_order NUMERIC NOT NULL, data_json TEXT NOT NULL,
        UNIQUE(kind,source_key,semantic_version))""",
    "CREATE INDEX alert_source_watch_sequence ON alert_source_versions(watch_id,sequence)",
    "CREATE INDEX alert_source_watch_kind_sequence "
    "ON alert_source_versions(watch_id,kind,sequence)",
    "CREATE INDEX alert_source_version_at ON alert_source_versions(kind,source_key,sequence)",
    """CREATE TABLE alert_source_latest (
        sequence INTEGER PRIMARY KEY REFERENCES alert_source_versions(sequence),
        watch_id TEXT,kind TEXT NOT NULL,source_order NUMERIC NOT NULL)""",
    "CREATE INDEX alert_source_latest_at ON alert_source_latest(watch_id,kind,sequence)",
    """CREATE TRIGGER alert_source_latest_capture AFTER INSERT ON alert_source_versions
        BEGIN
        INSERT INTO alert_source_latest(sequence,watch_id,kind,source_order)
        SELECT NEW.sequence,NEW.watch_id,NEW.kind,NEW.source_order
        WHERE NEW.source_order >= COALESCE(
            (SELECT source_order FROM alert_source_latest
             WHERE watch_id IS NEW.watch_id AND kind=NEW.kind ORDER BY sequence DESC LIMIT 1),
            NEW.source_order);
        END""",
    """CREATE TABLE alert_source_heads (
        kind TEXT NOT NULL,source_key TEXT NOT NULL,semantic_version INTEGER NOT NULL,
        sequence INTEGER NOT NULL REFERENCES alert_source_versions(sequence),
        PRIMARY KEY(kind,source_key))""",
    """CREATE TABLE alert_source_epoch (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),device INTEGER NOT NULL,
        inode INTEGER NOT NULL,schema_cookie INTEGER NOT NULL,
        legacy_complete INTEGER NOT NULL DEFAULT 0,complete INTEGER NOT NULL DEFAULT 0)""",
    """CREATE TABLE alert_source_bootstrap (
        kind TEXT PRIMARY KEY,upper_json TEXT,cursor_json TEXT,complete INTEGER NOT NULL,
        anchor_report TEXT,anchor_ordinal INTEGER)""",
    """CREATE TABLE alert_legacy_reports (
        report_id TEXT PRIMARY KEY,watch_id TEXT NOT NULL,observed_ordinal INTEGER NOT NULL,
        completed INTEGER NOT NULL,position INTEGER,predecessor_report_id TEXT,
        UNIQUE(observed_ordinal))""",
    "CREATE INDEX alert_legacy_order ON alert_legacy_reports(watch_id,observed_ordinal,report_id)",
    "CREATE INDEX alert_legacy_pending "
    "ON alert_legacy_reports(watch_id,completed,observed_ordinal,report_id)",
    "CREATE INDEX alert_legacy_unpositioned "
    "ON alert_legacy_reports(watch_id,observed_ordinal,report_id) WHERE position IS NULL",
    """CREATE TABLE alert_legacy_heads (
        watch_id TEXT PRIMARY KEY,report_id TEXT NOT NULL,observed_ordinal INTEGER NOT NULL,
        position INTEGER NOT NULL)""",
    """CREATE TABLE alert_legacy_positions (
        position INTEGER PRIMARY KEY AUTOINCREMENT,report_id TEXT NOT NULL UNIQUE,
        watch_id TEXT NOT NULL,predecessor_report_id TEXT)""",
    *tuple(statement for spec in SOURCE_SPECS for statement in _triggers(spec)),
    """CREATE TRIGGER alert_source_versions_no_update BEFORE UPDATE ON alert_source_versions
        BEGIN SELECT RAISE(ABORT,'alert source versions are append-only'); END""",
    """CREATE TRIGGER alert_source_versions_no_delete BEFORE DELETE ON alert_source_versions
        BEGIN SELECT RAISE(ABORT,'alert source versions are append-only'); END""",
    *tuple(
        f"CREATE TRIGGER {table}_no_{operation.lower()} BEFORE {operation} ON {table} "
        "BEGIN SELECT RAISE(ABORT,'alert source positions are append-only'); END"
        for table in ("alert_source_latest", "alert_legacy_positions")
        for operation in ("UPDATE", "DELETE")
    ),
)
