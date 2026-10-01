# CLAUDE.md: context for future Claude sessions

## Project
`data-lake` is a personal, learning-first data platform. Goal: land raw science data (ocean, astronomy,
more later) in a lake, clean it, and curate it into a queryable warehouse, so the owner learns how
data pipelines and ML work. Related project: github.com/evant455-cpu/ocean-watch (Global Fishing Watch
analysis), which will become the first connector.

## Owner and working style (IMPORTANT)
- Self-taught developer, often on a phone. Not a professional data engineer.
- GO SLOW. One small lesson at a time, then stop so the owner can absorb it and ask questions.
- Do NOT quiz him or end lessons with review questions. He wants to walk through the material, not be tested.
- Explain every new term, tool and abbreviation the first time it appears; add it to `docs/glossary.md`.
- Keep explanations brief and to the point, but never skip the "why".
- Diagnose before acting: check the current state of files and environment before changing anything.
- Give genuine encouragement now and then.
- Avoid pasting long indented code for him to copy; prefer files in the repo and the CLI.

## Architecture (medallion layout)
1. `lake/raw/<source>/<dataset>/<YYYY-MM-DD>/`: bytes exactly as fetched, never edited, never overwritten,
   each with a `.meta.json` sidecar (source, fetch time, sha256).
2. `lake/clean/<source>/<dataset>/<date>/<raw stem>.parquet`: tidy, typed tables (Parquet), one per raw file. Re-creatable from raw at any time.
3. `warehouse/`: curated, query-ready (DuckDB). One warehouse, one schema per domain (e.g. `ocean`, `astro`).
   Separate warehouses only for different access rules or projects.
New source = small connector + raw folder + cleaning step. Existing code should not change.

## Hard rules
- Never commit data, `lake/`, `warehouse/` contents, tokens, or secrets. `.gitignore` enforces this.
- API tokens come from env vars / a git-ignored `.env` / Colab Secrets, never from code or chat.
- Respect each source's license and terms; credit sources. Non-commercial use.
- Deterministic code does bulk cleaning. Use the Claude API only for hard judgment cases (messy text,
  name matching, labelling), not millions of rows.
- New behaviour gets a test using fake data. Run `python -m pytest -q` before committing.

## Lesson plan (status)
- [x] Lesson 1: layout and `landing.land_raw` (raw data + provenance sidecar + no-overwrite rule)
- [x] Lesson 2: raw -> clean (`datalake.cleaning.clean_csv`: parse, types, nulls, Parquet; uses pyarrow)
- [x] Lesson 3: clean -> warehouse (`datalake.warehousing`: load_table + read-only query; DuckDB schemas per domain)
- [x] Lesson 4: first connector (`datalake.connectors.gfw`: land AIS-gap events as raw JSON; fake-client tests; NOT yet run against the live API, sandbox network blocks GFW). Lesson 4b done: `cleaning.clean_json` + `gfw.GAP_EVENT_SCHEMA` flatten raw events to Parquet -> `ocean.gap_events` (tested end to end with fake data). One-command pipeline done: `gfw.run_gap_pipeline` / `python -m datalake.connectors.gfw START END [REGION]` (land -> clean -> `ocean.gap_events`; README has the live-run steps). `--raw PATH` / `gfw.process_raw` re-clean an existing raw file with no token or network. LIVE FETCH SUCCEEDED on owner's Windows machine (2026-09-30, raw file landed in C:\data-lake; his copy predated the pipeline until he pulls). VERIFIED LIVE (2026-09-30): 77 gap events, 0 bad values; vessel_id/type/duration/disabling 77/77 filled, vessel_flag 54/77 (GFW sends the `flag` key with no value for 23 vessels, so the gap is genuine, not a schema error). Raw records also hold unused fields (vessel.ssvid, nextPort, public_authorizations) that can become columns later from raw. NEXT: owner pulls and re-runs `--raw` (expect 0 new / 77 updated), then fetches a second, overlapping date range to watch dedupe on real data; then Lesson 5. Lesson 4c done: `warehousing.append_table` (upsert: key = vessel_id+start, newer copy wins, `_source_file` lineage column, single transaction, old load_table tables auto-upgrade); `process_raw` now appends, so `ocean.gap_events` accumulates across runs. Not yet verified on live data: the owner's first run after pulling should report 0 new / 77 updated / 77 total; `load_table` (replace) still exists for simple one-off tables. `python -m datalake.warehousing "SQL"` (`warehousing.ask`) asks read-only questions and prints a table; all read helpers force UTC display (DuckDB otherwise shows the machine's local zone); `pytz` is now a declared dependency (DuckDB needs it for tz-aware timestamps; pandas 3 no longer brings it). LIVE FINDING (2026-09-30): the real 77-row gap file has 1 pair sharing (vessel_id, start), so the table holds 76; owner is inspecting the pair to decide whether (vessel_id, start) is the right key or `end`/GFW event id should join it. Idea not built: a `rebuild` command that recreates a table from all clean files. Naming fix done: clean files mirror raw as `lake/clean/<source>/<dataset>/<date>/<raw stem>.parquet`, so same-day raw files never collide.
- [ ] Lesson 5: second connector (Gaia astronomy) and cross-domain structure
- [ ] Later: scheduling (GitHub Actions), data-quality checks, first ML project (e.g. anomaly detection)
