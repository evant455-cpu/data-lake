# data-lake

A personal, learning-first data platform for ocean, astronomy and other science data.
Raw data is landed untouched, then cleaned, then curated into a warehouse you can query with SQL.

```
lake/raw/<source>/<dataset>/<date>/   untouched data + provenance sidecar
lake/clean/                           tidy typed tables (Parquet)
warehouse/                            curated, query-ready (DuckDB), one schema per domain
```

Data never goes in git (see `.gitignore`). Progress and the lesson plan are in `CLAUDE.md`;
terms are in `docs/glossary.md`.

Quick start: `pip install -e ".[dev]" && python -m pytest -q`
