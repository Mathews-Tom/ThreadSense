# Local Runtime Contract

## Purpose

ThreadSense targets a local OpenAI-compatible chat runtime as the first inference backend. The runtime root may expose an HTML frontend, so application traffic must target the API path explicitly.

Default endpoint:

- base URL: `http://127.0.0.1:8080`
- chat API path: `/v1/chat/completions`
- resolved chat endpoint: `http://127.0.0.1:8080/v1/chat/completions`

## Request Contract

ThreadSense uses a minimal readiness probe request before any higher-level inference tasks are introduced.

```json
{
  "model": "local-model",
  "messages": [
    {
      "role": "system",
      "content": "Return the single token READY."
    },
    {
      "role": "user",
      "content": "READY"
    }
  ],
  "stream": false,
  "temperature": 0
}
```

Required request fields:

- `model`: runtime-selected model identifier
- `messages`: OpenAI-style chat message array
- `stream`: boolean switch for chunked responses
- `temperature`: deterministic probe setting

## Response Contract

ThreadSense currently validates the non-streaming response shape against the OpenAI-compatible chat completion envelope.

```json
{
  "id": "chatcmpl-local-123",
  "object": "chat.completion",
  "created": 1760000000,
  "model": "local-model",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "READY"
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 15,
    "completion_tokens": 1,
    "total_tokens": 16
  }
}
```

Validated response fields:

- `id`
- `object == "chat.completion"`
- `model`
- `choices[0].message.content`
- `choices[0].finish_reason`

## Streaming Expectations

Streaming support stays within the same API path and switches on `stream: true`. ThreadSense does not consume streamed chunks yet, but the backend contract is fixed now so later adapters can validate chunk handling without redefining the endpoint.

Expected streamed object types:

- `chat.completion.chunk` during chunk delivery
- terminal chunk with `finish_reason`

## Model Selection

Model selection stays in application configuration, not in business logic. The runtime probe sends the configured model name exactly as resolved by `threadsense.config`.

## Current Environment Note

During implementation, the root endpoint was observed serving HTML while the API traffic succeeded only when targeted directly at `http://127.0.0.1:8080/v1/chat/completions`.

Known-good local probe result:

- request target: `http://127.0.0.1:8080/v1/chat/completions`
- HTTP status: `200`
- configured request model: `local-model`
- runtime response model: `ggml-org/gemma-4-E4B-it-GGUF:Q8_0`
- observed latency: about `9.8s`

The CLI readiness check reports this endpoint explicitly and does not fall back to the root HTML frontend.
