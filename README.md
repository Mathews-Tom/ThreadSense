# ThreadSense

ThreadSense is an evidence-first discussion intelligence pipeline for turning long community threads into structured, inspectable product intelligence.

The current implementation supports:

- Reddit thread ingestion through the public JSON API
- canonical normalization and persisted artifacts
- deterministic analysis with evidence-linked findings
- optional local-model summary generation through an OpenAI-compatible runtime
- JSON and Markdown report generation
- reproducible batch runs
- a local HTTP API surface

ThreadSense is local-first. It is optimized for operator workflows, replayability, and evidence traceability rather than cloud-first convenience.

## Happy Path

Run the full single-thread workflow with one command:

```bash
uv run threadsense run reddit <reddit-url> \
  [--with-summary] \
  [--summary-required] \
  [--format markdown|json]
```

Example:

```bash
uv run threadsense run reddit \
  "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/anyone_actually_built_a_second_brain_that_isnt/" \
  --format markdown \
  --with-summary \
  --summary-required
```

## What It Does

ThreadSense runs a staged pipeline:

1. fetch a Reddit thread
2. persist the raw artifact
3. normalize it into a canonical thread model
4. run deterministic analysis
5. optionally run local inference for summary synthesis
6. persist a JSON or Markdown report

Artifacts remain separate at each stage so the pipeline is inspectable and rerunnable.

## Architecture

Core modules:

- [src/threadsense/connectors/reddit.py](src/threadsense/connectors/reddit.py): Reddit ingestion, retries, response validation, `morechildren` expansion
- [src/threadsense/models/canonical.py](src/threadsense/models/canonical.py): canonical thread schema
- [src/threadsense/pipeline/normalize.py](src/threadsense/pipeline/normalize.py): Reddit-to-canonical normalization
- [src/threadsense/pipeline/analyze.py](src/threadsense/pipeline/analyze.py): deterministic analysis baseline
- [src/threadsense/inference](src/threadsense/inference): local inference contracts, prompts, runtime adapter, and routing
- [src/threadsense/reporting](src/threadsense/reporting): report assembly, rendering, and quality checks
- [src/threadsense/workflows.py](src/threadsense/workflows.py): shared fetch, normalize, analyze, infer, and report execution
- [src/threadsense/batching.py](src/threadsense/batching.py): batch manifests and bounded parallel runs
- [src/threadsense/api_server.py](src/threadsense/api_server.py): local HTTP surface

Supporting docs:

- [docs/overview.md](docs/overview.md)
- [docs/system-design.md](docs/system-design.md)
- [docs/local-runtime-contract.md](docs/local-runtime-contract.md)
- [docs/batch-api-runtime.md](docs/batch-api-runtime.md)
- [.docs/2026-04-04-system-enhancement-analysis.md](.docs/2026-04-04-system-enhancement-analysis.md)

## Requirements

- Python `>=3.11`
- `uv`
- optional local OpenAI-compatible runtime at `http://127.0.0.1:8080/v1/chat/completions`

## Installation

```bash
uv sync
```

Run the CLI through `uv`:

```bash
uv run threadsense --help
```

## Quickstart

### 1. Validate local setup

```bash
uv run threadsense preflight
```

Skip runtime probing if you only want config validation:

```bash
uv run threadsense preflight --skip-runtime
```

### 2. Run the full workflow

```bash
uv run threadsense run reddit \
  "https://www.reddit.com/r/ClaudeCode/comments/1ro0qbl/anyone_actually_built_a_second_brain_that_isnt/" \
  --format markdown
```

The command prints one JSON payload containing the stage outputs and final artifact paths.

Use `--with-summary --summary-required` if you want the final report to require a live local-model summary.

## Usage Reference

For the detailed command-by-command reference, including:

- `preflight`
- `fetch`
- `normalize`
- `analyze`
- `infer`
- `report`
- `inspect`
- `batch run`
- `serve`
- `run reddit`

see [docs/usage.md](docs/usage.md).

## Configuration

Default config file:

- [threadsense.toml](threadsense.toml)

Important sections:

- `[runtime]`: local inference backend URL, path, model, timeout, repair retries
- `[reddit]`: user agent, timeout, retries, backoff, request delay, listing limit
- `[storage]`: raw, normalized, analysis, report, and batch directories
- `[batch]`: worker count, max jobs, fail-fast behavior
- `[api]`: host, port, request size limit
- `[limits]`: runtime concurrency guard

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

- `.threadsense`

Default structure:

- `.threadsense/raw/reddit/<thread-id>.json`
- `.threadsense/normalized/reddit/<thread-id>.json`
- `.threadsense/analysis/reddit/<thread-id>.json`
- `.threadsense/reports/reddit/<thread-id>.json`
- `.threadsense/reports/reddit/<thread-id>.md`
- `.threadsense/batches/<run-name>.json`

## Local Runtime Contract

ThreadSense expects a local OpenAI-compatible chat completion endpoint.

Default target:

- base URL: `http://127.0.0.1:8080`
- chat path: `/v1/chat/completions`
- resolved endpoint: `http://127.0.0.1:8080/v1/chat/completions`

The local runtime contract is documented in [docs/local-runtime-contract.md](docs/local-runtime-contract.md).

## Validation

Run the full local validation stack:

```bash
uv run ruff check
uv run ruff format --check .
uv run mypy --strict src tests
uv run pytest
./scripts/validate.sh
```

## Tests

The repo includes:

- unit tests for config, connectors, normalization, analysis, inference, reporting, observability, batching, and API validation
- integration tests for fetch, normalization, inference, reporting, batch runs, and API workflows
- fixture-backed Reddit payloads and replay manifests under [tests/fixtures](tests/fixtures)

## Current Limits

The current system is intentionally constrained:

- only Reddit is implemented as a source
- analysis is still a deterministic heuristic baseline
- the main product unit is still a single thread or local batch run
- the API is local-first and not hardened for untrusted callers
- report presentation is Markdown and JSON, not yet an interactive HTML review surface

## Roadmap Direction

The completed phased plan established the system backbone. The next high-value areas are:

- claim-level evidence attribution
- evaluation and replay benchmarking
- corpus-level analysis across threads
- better decision-oriented report presentation
- stronger onboarding and developer ergonomics
