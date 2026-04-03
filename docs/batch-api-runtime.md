# Batch, API, and Runtime Hardening

## Batch Manifest

Batch runs consume a JSON manifest with this shape:

```json
{
  "manifest_version": 1,
  "run_name": "fixture-replay",
  "created_at_utc": 1710000000.0,
  "jobs": [
    {
      "job_id": "normal-thread",
      "source_name": "reddit",
      "thread_url": "https://www.reddit.com/r/ThreadSense/comments/abc123/normal_thread",
      "expand_more": false,
      "flat": false,
      "report_format": "json",
      "with_summary": false,
      "summary_required": false
    }
  ]
}
```

Rules:

- `manifest_version` must be `1`
- `job_id` values must be unique
- `source_name` is currently `reddit`
- `report_format` is `json` or `markdown`
- `with_summary` and `summary_required` control local-runtime summary generation

Run a batch job with:

```bash
uv run threadsense batch run --manifest tests/fixtures/batch/reddit_manifest.json
```

The batch artifact is written to `.threadsense/batches/<run_name>.json` unless `--output` is provided.

## HTTP Surface

Start the local API with:

```bash
uv run threadsense serve
```

Routes:

- `POST /v1/fetch/reddit`
- `POST /v1/normalize/reddit`
- `POST /v1/analyze/normalized`
- `POST /v1/infer/analysis`
- `POST /v1/report/analysis`
- `GET /v1/healthz`
- `GET /v1/metrics`

All write paths still pass through the same fetch, normalize, analyze, infer, and report workflow functions used by the CLI. The API does not bypass artifact validation or provenance capture.

## Hard Limits

Config keys:

- `[batch].max_workers`
- `[batch].max_jobs`
- `[api].max_request_bytes`
- `[limits].runtime_concurrency`

Environment overrides:

- `THREADSENSE_BATCH_MAX_WORKERS`
- `THREADSENSE_BATCH_MAX_JOBS`
- `THREADSENSE_API_MAX_REQUEST_BYTES`
- `THREADSENSE_RUNTIME_CONCURRENCY`

`runtime_concurrency` is enforced through a shared semaphore around local-runtime inference calls so batch and API traffic cannot overrun the local model backend.

## Observability

Structured logs emit JSON records with:

- `event`
- `trace_id`
- `run_id`
- `job_id`
- `stage`
- `latency_seconds`

Metrics are exposed in Prometheus text format at `/v1/metrics`:

- `threadsense_stage_total`
- `threadsense_stage_seconds_avg`

Labels include stage, source, route, task, format, provider, and outcome where relevant.
