# Cross-thread issue history fixture sequence

## Provenance

Every payload in `responses/` is a synthesized, representative Reddit JSON listing shape (search listing, `[post listing, comment listing]` thread payload) written by hand for this test suite. Ids (`ix1aaa`, `ia1`, ...), authors, links and text are invented. Nothing here was captured from live Reddit, and tests replaying it must not describe it as a live acquisition.

## Files

- `profile.toml`: watch profile `export-issues` (query `export crash`, subreddits `DevNotes` and `NoteApps`, budgets large enough that every phase refreshes every tracked thread, `presented_findings = 20`).
- `phases.json`: ordered list of phases; each phase is exactly one sweep. Its format is the one documented in `../incremental/README.md`.
- `responses/search_devnotes.json`, `responses/search_devnotes_updates.json`: r/DevNotes search listings returning A, B, D, E and F.
- `responses/search_noteapps_empty.json`, `responses/search_noteapps_repost.json`: r/NoteApps search listings, empty and returning C.
- `responses/thread_a.json`, `responses/thread_a_updates.json`, `responses/thread_b.json`, `responses/thread_c.json`, `responses/thread_d.json`, `responses/thread_e.json`, `responses/thread_f.json`: thread payloads.

Replay rules (route matching, status routes, fresh timestamp rebasing) are documented in `../incremental/README.md` and implemented by `tests/integration/recorded_reddit.py`. Every phase lists all six posts as fresh, so search hits stay inside the 30-day search window; comment timestamps are served as recorded except the fresh comments of phase `updates`.

## Threads

Comment ids in stored artifacts are prefixed `reddit:` (for example `reddit:ia1`). Observed analysis with the default configuration:

| Thread | Community | Finding themes / key phrases | Evidence comments (author) | What it exercises |
| --- | --- | --- | --- | --- |
| A `ix1aaa` | DevNotes | reliability: `export crash`, `markdown export`, `markdown export crash`, `after few`, `after few minutes` | `ia1` (harbor_notes), `ia2` (pine_dev); anchor 1760000300 | Origin issue for the exact copy and the update claims |
| B `ix1bbb` | DevNotes | reliability: `export crash`, `markdown export`, `markdown export crash`, `after updating`, `after updating vault` | `ib1` (lena_codes), `ib2` (quill_ops); anchor 1760003720 | Unconfirmed theme and key-phrase suggestion with A that never merges counts; operator link and unlink target |
| C `ix1ccc` (phase `repost`) | NoteApps | reliability | `ic1` (relay_poster), body identical to `ia1`; anchor 1760007260 | Exact-content lineage: `ic1` is an `exact_copy` of `ia1` and C joins A's issue |
| D `ix1ddd` | DevNotes | usability: `login setup`, `login setup screen`, `setup screen`, `after notice`, `also resets` | `id1` (kai_sync), `id2` (mira_ux); `id1` links `https://status.example.com/incidents/77` | Shared link without shared theme or phrase: refused `shared_link_only` with E |
| E `ix1eee` | DevNotes | workflow: `batch pipeline`, `billing dashboard`, ... | `ie1` (ravi_ops), `ie2` (tess_fin); `ie1` links the same URL | Refusal partner of D |
| F `ix1fff` | DevNotes | reliability: `cache folder`, `error banner`, `plugin crash`, ... | `if1` (old_timer), `if2` (cache_watch); anchor 1752000100, about 92 days before A | Outside the default 30-day candidate window: never compared unless the window is widened |

## Phase sequence

| Phase | Search returns | Threads served | Expected issue behavior with defaults (window 30 days, 25 comparisons, 3 suggestions) |
| --- | --- | --- | --- |
| `initial` | DevNotes: A, B, D, E, F; NoteApps: none | `thread_a`, B, D, E, F | Five active issues, one per home thread; A–B unconfirmed suggestion (theme `reliability`); D–E refused `shared_link_only`; F never compared |
| `repost` | DevNotes: A, B, D, E, F; NoteApps: C | `thread_a`, B, C, D, E, F | `ic1` recorded as `exact_copy` of `ia1`; C assigned to A's issue with basis `exact_content_lineage` and C's home issue inactive; A's issue counts 2 threads, 2 communities, 3 accounts, 2 firsthand reports, 1 exact copy, 3 reports; C–B unconfirmed suggestion |
| `updates` | DevNotes: A, B, D, E, F; NoteApps: C | `thread_a_updates`, B, C, D, E, F | A gains fresh `ia3` (pine_dev, `steps to reproduce`), `ia4` (calm_tester, `cannot reproduce`), `ia5` (harbor_notes, `workaround`), `ia6` (news_reader, `fixed in`) in two finding entries (reliability: `ia4`, `ia6`; workflow: `ia3`, `ia5`); one update of each kind, `resolution.resolved` stays false with one resolution claim; 6 firsthand reports, 1 exact copy, 5 accounts |
| `unchanged` | DevNotes: A, B, D, E, F; NoteApps: C | `thread_a_updates`, B, C, D, E, F | No revision, no digest entry, nothing to ingest |
