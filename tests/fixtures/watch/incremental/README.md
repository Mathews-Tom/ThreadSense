# Incremental watch sweep fixture sequence

## Provenance

Every payload in `responses/` is a synthesized, representative Reddit JSON listing shape (search listing, `[post listing, comment listing]` thread payload) written by hand for this test suite. Ids (`sc1aaa`, `ka1`, ...), authors and text are invented. Nothing here was captured from live Reddit, and tests replaying it must not describe it as a live acquisition.

## Files

- `profile.toml`: watch profile `sync-conflicts` (one query, one subreddit, budgets `discovered_candidates = 4`, `refreshed_threads = 2`, `analyzed_revisions = 2`, `presented_findings = 3`, exclude term `giveaway`).
- `phases.json`: ordered list of phases; each phase is exactly one sweep.
- `responses/*.json`: decoded JSON bodies referenced by routes.

## phases.json format

```text
{
  "description": str,
  "provenance": str,
  "phases": [
    {
      "name": str,
      "notes": str,
      "routes": [
        {"url": str, "params_match": {str: scalar}, "response": "responses/<file>.json"}
        | {"url": str, "params_match": {str: scalar}, "status": int}
      ],
      "fresh_comment_ids": [str],   # Reddit comment ids without the t1_ prefix
      "fresh_post_ids": [str]       # Reddit post ids without the t3_ prefix
    }
  ]
}
```

## Replay rules

A replay transport serves one phase at a time and has the `RedditTransport` signature `(url, headers, params, timeout) -> decoded JSON`.

1. A request matches a route when `url` equals the route `url` exactly (the URL the connector calls after `normalize_url`, without query string) and every key in `params_match` is present in the request params with an equal value, compared as strings. Params not named in `params_match` are ignored. The first matching route in list order wins; routes may be served any number of times.
2. A request that matches no route is an error that fails the test. A phase therefore lists every request its sweep is allowed to make.
3. A route with `status` raises the same error the real `fetch_json` raises for an HTTP status error: `RedditRequestError("reddit request failed", details={"url", "status_code", "response_body"})`.
4. A route with `response` returns a deep copy of the decoded file. In that copy, every `t1` thing whose `data.id` is in `fresh_comment_ids` and every `t3` thing whose `data.id` is in `fresh_post_ids` (at any depth, in search and thread payloads) has `data.created_utc` replaced with `time.time()` at serve time. This models comments posted after the previous observation and keeps search hits inside the sweep's 30-day search window. Every other timestamp is served exactly as recorded.

## Phase sequence

Threads: A `sc1aaa`, B `sc1bbb`, C `sc1ccc` (excluded by `giveaway`), D `sc1ddd`, E `sc1eee`. The refresh queue orders selected threads by `COALESCE(last_attempted_at_utc, first_discovered_at_utc)`, then id, so A and B are fetched in every phase and D and E are never fetched.

| Phase | Search returns | A served | B served | Expected sweep outcome |
| --- | --- | --- | --- | --- |
| initial | A, B, C | `thread_a_initial` | `thread_b_initial` | `changes`: two initial revisions, five `initial_observation` comments |
| unchanged | A, B, C | `thread_a_initial` | `thread_b_initial` | `no_changes` |
| score_only | A, B, C | `thread_a_score_only` (scores changed, order reversed) | `thread_b_score_only` | `no_changes` |
| search_omits_known | B, C | `thread_a_new_comment` (ka4 fresh) | `thread_b_score_only` | `changes`: A revision with one `new_source_event` |
| edit | B, C | `thread_a_new_comment` | `thread_b_edited` (kb2 text edited) | `changes`: B revision with one `edited` |
| partial | B, C | `thread_a_nested_more` (ka2 behind nested more) | HTTP 503 | `partial`: truncated A acquisition, ka2 absent, no revision, one fetch failure |
| reappear | B, C | `thread_a_reappear` (ka2 back, ka3 `[removed]`, ka5 old `created_utc`) | `thread_b_edited` | `changes`: A revision with one `newly_observed_historical`, ka3 kept from the previous snapshot |
| backlog | B, D, E | `thread_a_reappear` | `thread_b_edited` | `no_changes`: four tracked threads, two attempted, refresh pending 2 |
