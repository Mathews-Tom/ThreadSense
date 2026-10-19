# Fixture Conventions

- `reddit/raw/` stores source payloads captured before normalization.
- `reddit/raw/normal_thread.json`, `deleted_thread.json`, `removed_thread.json`, and `large_thread.json` lock connector behavior to representative Reddit payload shapes.
- `reddit/raw/morechildren_response.json` stores deferred-comment expansion data for connector tests.
- `normalized/` stores canonical thread artifacts after normalization exists.
- `inference/` stores request and response payloads for the local runtime contract.
- `watch/profile_v1.toml` is a complete version 1 watch profile document; keep it loadable by the current profile parser and add a new versioned file rather than editing it when the profile format changes.
- `watch/incremental/` is a recorded-source sequence for incremental watch sweeps: `profile.toml`, `phases.json` (one entry per sweep listing every Reddit request it may make) and `responses/*.json`. The payloads are synthesized Reddit listing shapes with invented ids, not live captures. `watch/incremental/README.md` documents the replay rules (exact URL plus `params_match` subset, unmatched request fails, `status` routes raise `RedditRequestError`, `created_utc` rebased to serve time only for listed fresh ids) and the expected outcome of each phase.

Keep fixtures deterministic, redact secrets, and prefer the smallest payload that still covers the target behavior.
