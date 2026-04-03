# Fixture Conventions

- `reddit/raw/` stores source payloads captured before normalization.
- `reddit/raw/normal_thread.json`, `deleted_thread.json`, `removed_thread.json`, and `large_thread.json` lock connector behavior to representative Reddit payload shapes.
- `reddit/raw/morechildren_response.json` stores deferred-comment expansion data for connector tests.
- `normalized/` stores canonical thread artifacts after normalization exists.
- `inference/` stores request and response payloads for the local runtime contract.

Keep fixtures deterministic, redact secrets, and prefer the smallest payload that still covers the target behavior.
