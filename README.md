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

## Live run: Global Fishing Watch AIS-gap events

Needs a free GFW API token. Put it in a git-ignored `.env` file (never in code or chat):

```
GFW_API_ACCESS_TOKEN=your_token_here
```

Then, from the repo folder:

```
pip install -e ".[gfw]"
python -m datalake.connectors.gfw 2022-01-01 2022-05-01
```

Args: start date, end date, optional region id (default 5690, Russia EEZ).
It fetches the events, lands them raw, cleans them to Parquet, and loads `ocean.gap_events`.
The last lines show the count of values that did not fit their type; a high count means a field name or type differs from what we assumed.
Ask a question of the warehouse:

```
python -c "from datalake.warehousing import query; print(query('SELECT vessel_flag, count(*) FROM ocean.gap_events GROUP BY 1'))"
```

Already fetched today (or cleaning failed after the raw file landed)? Rebuild from the raw file, no token or network needed:

```
python -m datalake.connectors.gfw --raw lake/raw/gfw/gap-events/2026-09-30/2022-01-01_2022-05-01_public-eez-areas-5690.json
```

Notes: running the same dates + region twice on one day is refused (raw is never overwritten).
Each run is ADDED to `ocean.gap_events`: an event already in the table (same vessel and start time)
is replaced by the newer copy, anything else is added. The third output line shows how many were new,
updated, and the table total. Each row's `_source_file` column says which fetch it came from.
Re-running the same file is harmless (it reports 0 new, N updated).
Data: Global Fishing Watch. Non-commercial use.
