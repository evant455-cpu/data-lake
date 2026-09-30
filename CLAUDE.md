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
2. `lake/clean/`: tidy, typed tables (Parquet). Re-creatable from raw at any time.
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
- [ ] Lesson 4: first real connector (GFW, reusing ocean-watch code)
- [ ] Lesson 5: second connector (Gaia astronomy) and cross-domain structure
- [ ] Later: scheduling (GitHub Actions), data-quality checks, first ML project (e.g. anomaly detection)
