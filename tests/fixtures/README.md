# Fixture Conventions

- `reddit/raw/` stores source payloads captured before normalization.
- `normalized/` stores canonical thread artifacts after normalization exists.
- `inference/` stores request and response payloads for the local runtime contract.

Keep fixtures deterministic, redact secrets, and prefer the smallest payload that still covers the target behavior.
