# ThreadSense Usage

This document is the detailed operator reference for the current CLI and local API.

## Command Overview

ThreadSense exposes these top-level CLI commands:

- `preflight`
- `fetch`
- `normalize`
- `analyze`
- `infer`
- `report`
- `inspect`
- `batch`
- `serve`
- `run`

## Happy Path

Run the full workflow for one Reddit thread:

```bash
uv run threadsense run reddit <reddit-url> \
  [--with-summary] \
  [--summary-required] \
  [--format markdown|json] \
  [--expand-more] \
  [--flat] \
  [--config threadsense.toml]
```

Example:

```bash
uv run threadsense run reddit \
  "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/anyone_actually_built_a_second_brain_that_isnt/" \
  --format markdown \
  --with-summary \
  --summary-required
```

This command:

1. fetches the Reddit thread
2. persists the raw artifact
3. normalizes it
4. analyzes it
5. optionally runs local-model summarization
6. writes the final report

It prints a single JSON payload containing the fetch, normalize, analyze, and report outputs.

## `preflight`

Validate configuration and optionally probe the local runtime.

```bash
uv run threadsense preflight [--config threadsense.toml] [--skip-runtime]
```

Examples:

```bash
uv run threadsense preflight
uv run threadsense preflight --skip-runtime
```

Output:

- config summary
- enabled sources
- runtime endpoint
- optional runtime probe result

## `fetch reddit`

Fetch one Reddit thread and persist the raw artifact.

```bash
uv run threadsense fetch reddit <reddit-url> \
  [--output path/to/raw.json] \
  [--expand-more] \
  [--flat] \
  [--config threadsense.toml]
```

Notes:

- `--expand-more` expands deferred comment branches through Reddit `morechildren`
- `--flat` flattens nested comments in the persisted raw artifact
- without `--output`, the raw artifact is written to the configured storage root

## `normalize reddit`

Normalize one raw Reddit artifact into the canonical thread schema.

```bash
uv run threadsense normalize reddit \
  --input path/to/raw.json \
  [--output path/to/normalized.json] \
  [--config threadsense.toml]
```

Without `--output`, the normalized artifact is written to the configured normalized store path.

## `analyze normalized`

Run deterministic analysis for one canonical thread artifact.

```bash
uv run threadsense analyze normalized \
  --input path/to/normalized.json \
  [--output path/to/analysis.json] \
  [--config threadsense.toml]
```

Without `--output`, the analysis artifact is written to the configured analysis store path.

## `infer analysis`

Run a local inference task against one persisted analysis artifact.

```bash
uv run threadsense infer analysis \
  --input path/to/analysis.json \
  [--task analysis_summary] \
  [--required] \
  [--config threadsense.toml]
```

Current tasks come from the inference contract in [src/threadsense/inference/contracts.py](src/threadsense/inference/contracts.py).

Behavior:

- without `--required`, local inference may fall back to deterministic output
- with `--required`, the command fails if the local runtime is unavailable or invalid

## `report analysis`

Generate a report from one analysis artifact.

```bash
uv run threadsense report analysis \
  --input path/to/analysis.json \
  [--format markdown|json] \
  [--output path/to/report.md] \
  [--with-summary] \
  [--summary-required] \
  [--config threadsense.toml]
```

Behavior:

- `--format markdown` writes a Markdown report
- `--format json` writes a structured JSON report artifact
- `--with-summary` requests local-model summary generation
- `--summary-required` fails instead of degrading to deterministic summary output

## `inspect`

Inspect persisted artifacts without rerunning the pipeline.

Normalized artifact:

```bash
uv run threadsense inspect normalized --input path/to/normalized.json
```

Analysis artifact:

```bash
uv run threadsense inspect analysis --input path/to/analysis.json
```

Report artifact:

```bash
uv run threadsense inspect report --input path/to/report.json
```

These commands print compact JSON summaries of the selected artifact.

## `batch run`

Execute a reproducible multi-thread workflow from a manifest.

```bash
uv run threadsense batch run \
  --manifest tests/fixtures/batch/reddit_manifest.json \
  [--output path/to/batch-run.json] \
  [--config threadsense.toml]
```

The manifest shape is documented in [docs/batch-api-runtime.md](docs/batch-api-runtime.md).

Without `--output`, the batch artifact is written to:

```text
.threadsense/batches/<run-name>.json
```

## `serve`

Run the local HTTP API surface.

```bash
uv run threadsense serve \
  [--config threadsense.toml] \
  [--host 127.0.0.1] \
  [--port 8090]
```

Default routes:

- `GET /v1/healthz`
- `GET /v1/metrics`
- `POST /v1/fetch/reddit`
- `POST /v1/normalize/reddit`
- `POST /v1/analyze/normalized`
- `POST /v1/infer/analysis`
- `POST /v1/report/analysis`

Important:

- this API is intended for trusted local use
- it is not authenticated
- it is path-oriented and not a hardened public service contract

## Configuration

Default config file:

- [threadsense.toml](threadsense.toml)

Important config sections:

- `[runtime]`
- `[reddit]`
- `[storage]`
- `[batch]`
- `[api]`
- `[limits]`

Common environment overrides:

- `THREADSENSE_RUNTIME_ENABLED`
- `THREADSENSE_RUNTIME_BASE_URL`
- `THREADSENSE_RUNTIME_CHAT_PATH`
- `THREADSENSE_RUNTIME_MODEL`
- `THREADSENSE_REDDIT_TIMEOUT`
- `THREADSENSE_REDDIT_REQUEST_DELAY`
- `THREADSENSE_STORAGE_ROOT`
- `THREADSENSE_BATCH_MAX_WORKERS`
- `THREADSENSE_BATCH_MAX_JOBS`
- `THREADSENSE_API_PORT`
- `THREADSENSE_RUNTIME_CONCURRENCY`

## Artifact Layout

Default storage root:

```text
.threadsense
```

Default structure:

- `.threadsense/raw/reddit/<thread-id>.json`
- `.threadsense/normalized/reddit/<thread-id>.json`
- `.threadsense/analysis/reddit/<thread-id>.json`
- `.threadsense/reports/reddit/<thread-id>.json`
- `.threadsense/reports/reddit/<thread-id>.md`
- `.threadsense/batches/<run-name>.json`

## Validation

Run the local validation stack:

```bash
uv run ruff check
uv run ruff format --check .
uv run mypy --strict src tests
uv run pytest
./scripts/validate.sh
```

## Related Docs

- [README.md](README.md)
- [docs/local-runtime-contract.md](docs/local-runtime-contract.md)
- [docs/batch-api-runtime.md](docs/batch-api-runtime.md)
- [.docs/2026-04-04-system-enhancement-analysis.md](.docs/2026-04-04-system-enhancement-analysis.md)
