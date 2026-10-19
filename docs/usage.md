# ThreadSense Usage

This document is the command reference for the current CLI.

## Global Option

All commands support:

```bash
--output-format json|human|quiet
```

Modes:

- `json`: machine-readable payloads
- `human`: Rich tables and summary panels
- `quiet`: status-only output

See [output-modes.md](output-modes.md) for details.

## Command Overview

Top-level commands:

- `preflight`
- `fetch`
- `normalize`
- `analyze`
- `infer`
- `report`
- `inspect`
- `replay`
- `diff`
- `corpus`
- `evaluate`
- `batch`
- `serve`
- `run`
- `research`
- `watch`

## Happy Paths

### Single-thread run

```bash
uv run threadsense run [<source>] <url> \
  [--format markdown|html|json] \
  [--with-summary] \
  [--summary-required] \
  [--expand-more] \
  [--flat]
```

Source can be `reddit`, `hn`, `github-discussions`, `gist`, or omitted for auto-detection.

Examples:

```bash
uv run threadsense run reddit \
  "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/anyone_actually_built_a_second_brain_that_isnt/" \
  --format markdown \
  --with-summary \
  --summary-required
```

```bash
uv run threadsense run gist \
  "https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f" \
  --format markdown
```

### Topic research across subreddits

```bash
uv run threadsense research reddit \
  --query "second brain OR agentic PKM" \
  --subreddit ClaudeCode \
  --subreddit LocalLLaMA \
  --subreddit AI_Agents \
  [--time-window 30d] \
  [--limit 20] \
  [--per-subreddit-limit 8] \
  [--sort relevance] \
  [--with-summary]
```

Force human-mode summary output:

```bash
uv run threadsense --output-format human research reddit \
  --query "second brain OR agentic PKM" \
  --subreddit ClaudeCode \
  --subreddit LocalLLaMA \
  --subreddit AI_Agents \
  --limit 5 \
  --per-subreddit-limit 3 \
  --with-summary
```

## `preflight`

Validate config and optionally probe the local runtime.

```bash
uv run threadsense preflight [--config threadsense.toml] [--skip-runtime]
```

## `fetch`

### `fetch reddit`

```bash
uv run threadsense fetch reddit <reddit-url> \
  [--output path/to/raw.json] \
  [--expand-more] \
  [--flat]
```

Notes:

- `--expand-more` expands Reddit `morechildren`
- `--flat` flattens nested comments in the persisted raw artifact

### `fetch hn`

```bash
uv run threadsense fetch hn <hackernews-item-url> [--output path/to/raw.json]
```

### `fetch github-discussions`

```bash
uv run threadsense fetch github-discussions <discussion-url> [--output path/to/raw.json]
```

### `fetch github-gist`

```bash
uv run threadsense fetch github-gist <gist-url> [--output path/to/raw.json]
```

Aliases: `fetch gist`

Notes:

- Authentication is optional. Public gists work without a token (60 req/hr rate limit).
- Set `THREADSENSE_GITHUB_GIST_TOKEN` for authenticated access (5,000 req/hr).
- Comments are paginated at 100/page automatically.

## `normalize`

Normalize one raw artifact into the canonical thread schema.

### `normalize reddit`

```bash
uv run threadsense normalize reddit --input path/to/raw.json [--output path/to/normalized.json]
```

### `normalize hn`

```bash
uv run threadsense normalize hn --input path/to/raw.json [--output path/to/normalized.json]
```

### `normalize github-discussions`

```bash
uv run threadsense normalize github-discussions --input path/to/raw.json [--output path/to/normalized.json]
```

### `normalize github-gist`

```bash
uv run threadsense normalize github-gist --input path/to/raw.json [--output path/to/normalized.json]
```

Aliases: `normalize gist`

Notes:

- Gist comments are flat — all normalized to `depth=0` with no `parent_comment_id`.
- Multi-file gists are concatenated into a single body, each file prefixed with its filename.

## `analyze`

Run deterministic analysis for one canonical thread artifact.

```bash
uv run threadsense analyze normalized \
  --input path/to/normalized.json \
  [--output path/to/analysis.json]
```

Contract-related options are also supported:

- `--domain`
- `--objective`
- `--abstraction-level`
- `--auto-domain`

## `infer`

### `infer analysis`

```bash
uv run threadsense infer analysis \
  --input path/to/analysis.json \
  [--task analysis_summary] \
  [--required]
```

### `infer corpus`

```bash
uv run threadsense infer corpus \
  --input path/to/corpus-analysis.json \
  [--required]
```

Behavior:

- without `--required`, local inference may degrade to deterministic output where supported
- with `--required`, the command fails if the local runtime is unavailable or returns invalid output

## `report`

### `report analysis`

```bash
uv run threadsense report analysis \
  --input path/to/analysis.json \
  [--format markdown|html|json] \
  [--output path/to/report.md] \
  [--with-summary] \
  [--summary-required]
```

Single-thread reports support:

- Markdown
- HTML
- JSON report artifacts

Summary-capable reports include richer executive-summary fields such as priority, owner, action type, and expected outcome.

## `inspect`

Inspect persisted artifacts without rerunning the pipeline.

```bash
uv run threadsense inspect normalized --input path/to/normalized.json
uv run threadsense inspect analysis --input path/to/analysis.json
uv run threadsense inspect report --input path/to/report.json
```

## `replay`

Replay one analysis artifact through the analysis stack.

```bash
uv run threadsense replay --input path/to/analysis.json
```

## `diff`

Compare two analysis artifacts.

```bash
uv run threadsense diff --left path/to/a.json --right path/to/b.json
```

## `corpus`

### `corpus create`

```bash
uv run threadsense corpus create \
  --name corpus-name \
  --description "corpus description" \
  --domain developer_tools \
  --analysis-dir .threadsense/analysis/reddit
```

### `corpus analyze`

```bash
uv run threadsense corpus analyze --input path/to/manifest.json
```

### `corpus report`

```bash
uv run threadsense corpus report \
  --input path/to/manifest.json \
  [--with-summary] \
  [--summary-required]
```

### `corpus search`

```bash
uv run threadsense corpus search --input path/to/manifest.json --query "workflow"
```

## `research`

### `research reddit`

Discover and analyze topic discussions across selected subreddits.

```bash
uv run threadsense research reddit \
  --query "second brain OR agentic PKM" \
  --subreddit ClaudeCode \
  --subreddit LocalLLaMA \
  --subreddit AI_Agents \
  [--time-window 30d] \
  [--sort relevance|new|top|comments] \
  [--limit 20] \
  [--per-subreddit-limit 8] \
  [--format markdown] \
  [--expand-more] \
  [--flat] \
  [--with-summary] \
  [--summary-required]
```

Important behavior:

- default `--time-window` is `30d`
- time window is enforced exactly after retrieval
- Reddit still uses a coarse internal bucket such as `month`
- `--format` currently supports `markdown` only for this workflow
- the result includes selected thread metadata plus corpus artifact paths

Supported query syntax:

- `OR`
- `|`

Unsupported query syntax:

- quotes
- parentheses
- `title:`
- `selftext:`
- negation-style advanced Reddit syntax

See [research-reddit.md](research-reddit.md) for the full workflow and ranking behavior.

## `watch`

Manage saved research profiles ("watches") and their persistent monitoring state. A watch stores what to look for: queries, subreddits, analysis contract, refresh cadence and per-stage limits. The command group currently saves and reads profiles only. It does not fetch threads, run sweeps, call a model, or install a scheduler, and there is no CLI command that starts a run.

```bash
uv run threadsense watch create --profile PATH [--config PATH]
uv run threadsense watch list [--config PATH]
uv run threadsense watch inspect --id ID [--config PATH]
```

Output follows [output-modes.md](output-modes.md): human when stdout is a TTY and JSON otherwise, with `--output-format` overriding. In `json` mode the payload is printed to stdout. In `human` mode a successful payload is printed to stdout as syntax-highlighted JSON, and an error is a red panel on stderr showing the error code, message and details. In `quiet` mode a success prints only its `status` (`created`, `revised`, `unchanged` or `ok`) and an error prints `error: <message>`, both on stdout. Failures exit with code `1`. In `json` mode they print:

```json
{"status": "error", "error": {"type": "...", "code": "...", "message": "...", "details": {}}}
```

Profile problems use code `monitoring_input_error`. State database problems use `monitoring_state_error`, with a `reason` in `details` such as `unsupported_schema_version`, `foreign_database`, `not_a_database`, `state_locked`, `unknown_watch`, `recovery_failed` (SQLite could not recover an interrupted transaction of a supported state) or `recovery_conflict` (the state files changed during recovery). An empty, uninitialized state file reads as no watches. Run claim refusals use `run_claim_error`.

### Profile file

Profiles are TOML documents with `profile_version = 1`.

```toml
profile_version = 1
id = "agent-memory"
name = "Agent memory tooling"
domain = "developer_tools"
objective = "friction_analysis"
abstraction_level = "operational"
queries = ["second brain", "agentic PKM"]
subreddits = ["ClaudeCode", "r/LocalLLaMA"]
refresh_interval_minutes = 1440
include_terms = ["obsidian"]
exclude_terms = ["hiring"]

[limits]
discovered_candidates = 100
refreshed_threads = 25
analyzed_revisions = 25
presented_findings = 10
```

| Field                          | Required | Constraint                                                                                            |
| ------------------------------ | -------- | ----------------------------------------------------------------------------------------------------- |
| `profile_version`              | yes      | integer `1`; any other value, including a newer version, is rejected                                  |
| `id`                           | yes      | 1-64 lowercase letters, digits or inner hyphens, so `../x` is rejected; a database key, not a path    |
| `name`                         | yes      | 1-200 characters after trimming                                                                       |
| `domain`                       | yes      | same values as `--domain` in the analysis contract                                                    |
| `objective`                    | yes      | same values as `--objective`                                                                          |
| `abstraction_level`            | yes      | same values as `--level`                                                                              |
| `queries`                      | yes      | 1-10 entries, each at most 256 characters, same query syntax as `research reddit`                     |
| `subreddits`                   | yes      | 1-25 valid subreddit names; a leading `r/` is removed                                                 |
| `refresh_interval_minutes`     | yes      | integer 15-10080                                                                                      |
| `include_terms`                | no       | 0-50 entries, 1-100 characters each                                                                   |
| `exclude_terms`                | no       | 0-50 entries, 1-100 characters each; must not overlap `include_terms` case-insensitively              |
| `[limits]`                     | yes      | table with exactly the four keys below                                                                |
| `limits.discovered_candidates` | yes      | integer 1-500                                                                                         |
| `limits.refreshed_threads`     | yes      | integer 1-200                                                                                         |
| `limits.analyzed_revisions`    | yes      | integer 1-200                                                                                         |
| `limits.presented_findings`    | yes      | integer 1-100                                                                                         |

Validation rules:

- unknown top-level fields and unknown `[limits]` keys are rejected
- numeric fields must be integers; booleans are rejected
- duplicate entries (case-insensitive) in `queries`, `subreddits`, `include_terms` or `exclude_terms` are rejected
- entries are trimmed before validation
- the id never becomes a path; filesystem containment comes from `monitoring_dirname` staying inside the storage root
- all validation runs before the state database is opened for writing, so an invalid profile never creates or modifies state

### `watch create`

Validate a profile and save it as a new watch or a new revision of an existing watch.

```bash
uv run threadsense watch create --profile profiles/agent-memory.toml
```

`queries`, `subreddits`, `include_terms` and `exclude_terms` are treated as sets and stored sorted. The content hash is the SHA-256 of the canonical JSON form of the validated profile and excludes timestamps. Key order, list order, comments and whitespace in the TOML file do not affect it.

`status` in the result:

- `created`: first save for this `id`; revision `1`
- `unchanged`: content hash matches the current revision; the database is not modified
- `revised`: content differs from the current revision; `revision` is incremented and earlier revisions are kept

```json
{
  "artifact_type": "watch_profile",
  "status": "created",
  "watch_id": "agent-memory",
  "revision": 1,
  "content_sha256": "...",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3"
}
```

### `watch list`

List saved watches. A missing state database lists nothing and is not created.

```json
{
  "artifact_type": "watch_list",
  "status": "ok",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "watches": [
    {
      "watch_id": "agent-memory",
      "name": "Agent memory tooling",
      "current_revision": 2,
      "content_sha256": "...",
      "created_at_utc": 1760000000.0,
      "updated_at_utc": 1760003600.0
    }
  ]
}
```

Timestamps are Unix epoch seconds (UTC) as floating-point numbers.

### `watch inspect`

Show the current profile, revision history and recent run claims for one watch. An unknown `--id` is an error (`monitoring_state_error`, reason `unknown_watch`).

```json
{
  "artifact_type": "watch_inspection",
  "status": "ok",
  "state_path": "<resolved storage root>/monitoring/state.sqlite3",
  "watch_id": "agent-memory",
  "current_revision": 2,
  "created_at_utc": 1760000000.0,
  "updated_at_utc": 1760003600.0,
  "profile": {"profile_version": 1, "id": "agent-memory", "...": "..."},
  "revisions": [
    {"revision": 1, "content_sha256": "...", "created_at_utc": 1760000000.0},
    {"revision": 2, "content_sha256": "...", "created_at_utc": 1760003600.0}
  ],
  "active_run": null,
  "run_count": 0,
  "recent_runs": []
}
```

`recent_runs` holds the newest 20 claims. Each entry has `run_id`, `profile_revision`, `status` (`running`, `succeeded`, `failed` or `abandoned`), `claimed_at_utc`, `lease_expires_at_utc`, `finished_at_utc` and `recovered_by_run_id`. `active_run` is the entry whose status is `running`, or `null`. `run_count` is the total number of claims for the watch.

### State location

State is a SQLite database at `<storage root>/<monitoring_dirname>/state.sqlite3`, by default `.threadsense/monitoring/state.sqlite3`. Set `monitoring_dirname` under `[storage]` in the config file or with `THREADSENSE_STORAGE_MONITORING_DIR`. The value must be a relative path that stays inside the storage root; absolute paths and `..` segments are rejected. See [artifacts.md](artifacts.md) for the schema and version behavior.

### Run claims

The `threadsense.monitoring` Python package provides a run-claim primitive for future watch runs. No CLI command uses it yet; `watch inspect` only reports claims that already exist in the database.

- at most one run per watch is `running`, enforced transactionally and by a partial unique index
- a claim carries an owner token and a lease of 1-86400 seconds; only the SHA-256 of the token is stored
- a competing claim while the lease is live is refused with `run_claim_error`, reason `claim_held`
- after the lease expires, the next claim marks the old run `abandoned` with `recovered_by_run_id` pointing at the new run
- the previous owner can no longer finalize that run (reason `stale_owner`); an owner whose lease expired without a newer claim cannot finalize either (reason `lease_expired`)

## `evaluate`

Run golden-dataset evaluation.

```bash
uv run threadsense evaluate --dataset path/to/dataset.json --strategies keyword_heuristic hybrid
```

## `batch run`

Execute a reproducible batch workflow from a manifest.

```bash
uv run threadsense batch run --manifest tests/fixtures/batch/reddit_manifest.json
```

See [batch-api-runtime.md](batch-api-runtime.md).

## `serve`

Run the local HTTP API.

```bash
uv run threadsense serve [--host 127.0.0.1] [--port 8090]
```

Current routes:

- `GET /v1/healthz`
- `GET /v1/metrics`
- `POST /v1/fetch/reddit`
- `POST /v1/normalize/reddit`
- `POST /v1/analyze/normalized`
- `POST /v1/infer/analysis`
- `POST /v1/report/analysis`

## Artifacts

See [artifacts.md](artifacts.md) for the full artifact inventory.

Common paths:

- `.threadsense/raw/<source>/...`
- `.threadsense/normalized/<source>/...`
- `.threadsense/analysis/<source>/...`
- `.threadsense/reports/<source>/...`
- `.threadsense/corpora/<corpus-id>/...`
- `.threadsense/monitoring/state.sqlite3`

## Related Docs

- [README.md](../README.md)
- [research-reddit.md](research-reddit.md)
- [output-modes.md](output-modes.md)
- [artifacts.md](artifacts.md)
- [overview.md](overview.md)
- [system-design.md](system-design.md)
- [local-runtime-contract.md](local-runtime-contract.md)
- [batch-api-runtime.md](batch-api-runtime.md)
