# Local Runtime Contract

## Purpose

ThreadSense uses a local OpenAI-compatible chat runtime for optional synthesis tasks.

The deterministic pipeline does not require the runtime. The runtime is used for structured summary tasks such as:

- thread analysis summary
- corpus synthesis
- readiness probing

## Default Endpoint

- base URL: `http://127.0.0.1:8080`
- chat path: `/v1/chat/completions`
- resolved endpoint: `http://127.0.0.1:8080/v1/chat/completions`

## Runtime Tasks

### 1. Readiness probe

Used by `preflight` to confirm the runtime can answer a minimal deterministic request.

### 2. Analysis summary

Used by:

- `infer analysis`
- `report analysis --with-summary`
- `run ... --with-summary`

The request includes deterministic findings plus enriched thread context such as:

- thread title
- thread body when available
- top comments
- conversation structure
- issue/request marker counts

### 3. Corpus synthesis

Used by:

- `infer corpus`
- `corpus report --with-summary`
- `research reddit --with-summary`

The request includes cross-thread findings and trend context and must return structured corpus synthesis fields.

## Request Shape

ThreadSense sends OpenAI-compatible chat completion requests.

Core fields:

- `model`
- `messages`
- `stream: false`
- `temperature`

The actual message content varies by inference task.

## Response Shape

ThreadSense validates the standard non-streaming chat completion envelope.

Expected outer fields:

- `id`
- `object == "chat.completion"`
- `model`
- `choices[0].message.content`
- `choices[0].finish_reason`

The message content must be valid JSON matching the task contract.

## Structured Output Contracts

### Analysis summary contract

Expected keys include:

- `headline`
- `summary`
- `priority`
- `confidence`
- `why_now`
- `cited_theme_keys`
- `cited_comment_ids`
- `next_steps`
- `recommended_owner`
- `action_type`
- `expected_outcome`

### Corpus synthesis contract

Expected keys include:

- `headline`
- `key_patterns`
- `cited_thread_ids`
- `recommended_actions`
- `confidence_note`

## Required vs Degraded Behavior

ThreadSense supports both strict and optional runtime usage.

### Required

When `--required` or `--summary-required` is set:

- the command fails if the runtime is unavailable
- the command fails if the response is invalid

### Degraded

When summary inference is optional:

- the deterministic workflow still completes
- ThreadSense may use deterministic fallback output where supported
- the result payload reports the summary provider and degraded state

## Repair Behavior

ThreadSense can retry invalid JSON outputs with a stricter repair instruction.

This keeps the outer request contract stable while tightening schema enforcement for malformed model output.

## Streaming

Streaming is not consumed today.

ThreadSense currently expects non-streaming task responses.

## Operational Note

The runtime root may serve HTML or another non-API surface. ThreadSense always targets the chat completions path explicitly and does not fall back to the root endpoint.
