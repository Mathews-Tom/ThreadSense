# Batch, API, and Runtime Notes

## Batch Runs

Batch runs execute reproducible multi-thread workflows from a manifest.

Example:

```bash
uv run threadsense batch run --manifest tests/fixtures/batch/reddit_manifest.json
```

Batch jobs are still source-thread oriented. They do not replace the dedicated Reddit topic research workflow.

## Batch Manifest Shape

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
- `source_name` must match a supported source
- `report_format` is `json`, `markdown`, or `html` where supported by the selected workflow

## Local API Surface

Start the API with:

```bash
uv run threadsense serve
```

Current routes:

- `GET /v1/healthz`
- `GET /v1/metrics`
- `POST /v1/fetch/reddit`
- `POST /v1/normalize/reddit`
- `POST /v1/analyze/normalized`
- `POST /v1/infer/analysis`
- `POST /v1/report/analysis`

Important:

- the API is a trusted local surface
- it is not authenticated
- it does not yet expose the full `research reddit` workflow

## Runtime Interaction

Batch and API traffic share the same runtime concurrency guard as CLI runs.

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

## Observability

Structured logs emit stage completion and failure records, including:

- `event`
- `trace_id`
- `run_id`
- `job_id`
- `stage`
- `latency_seconds`

Metrics are exposed in Prometheus text format at `/v1/metrics`.

## Relationship To Research Reddit

`research reddit` is a discovery-plus-corpus workflow.

It is distinct from batch runs:

- batch runs execute a manifest of already selected threads
- research runs discover threads first, then create a corpus automatically

Both reuse the same lower-level fetch, normalize, analyze, infer, and reporting functions.
