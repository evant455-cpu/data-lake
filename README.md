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
Ask the warehouse a question (read-only, times shown in UTC):

```
python -m datalake.warehousing "SELECT vessel_flag, count(*) AS events FROM ocean.gap_events GROUP BY 1 ORDER BY 2 DESC"
```

Use double quotes around the SQL and single quotes inside it. The SQL can also read clean files directly:
`... FROM read_parquet('lake/clean/gfw/gap-events/*/*.parquet')`.

Already fetched today (or cleaning failed after the raw file landed)? Rebuild from the raw file, no token or network needed:

```
python -m datalake.connectors.gfw --raw lake/raw/gfw/gap-events/2026-09-30/2022-01-01_2022-05-01_public-eez-areas-5690.json
```

Notes: running the same dates + region twice on one day is refused (raw is never overwritten).
Each run is ADDED to `ocean.gap_events`: an event already in the table (same vessel and start time)
is replaced by the newer copy, anything else is added. The third output line shows how many were new,
updated, and the table total. Each row's `_source_file` column says which fetch it came from.
Re-running the same file is harmless (it reports 0 new, N updated).
Paging: GFW sends at most 100 events per request, so the fetch keeps asking for the next page until a
page comes back short or empty (capped at 50 pages). If the fetch might be incomplete (page cap hit, or GFW
seems to ignore the page offset) the output starts with a `WARNING: INCOMPLETE FETCH` line; what was fetched
is still saved, and a later fetch of a narrower range fills the gap safely (upsert).
Limits: GFW allows 50,000 requests a day and 1.5 million a month; going over is blocked (error 429) for 24 hours
or 30 days.
Data: Global Fishing Watch (CC BY-NC 4.0, credit required, non-commercial use).

## Live run: Gaia nearby stars (astronomy)

No account or token needed. From the repo folder:

```
python -m datalake.connectors.gaia
```

It asks the Gaia archive for the 1000 nearest well-measured stars (closer than 20 parsecs), lands the CSV
raw, cleans it, and loads `astro.nearby_stars` in the same warehouse file as the ocean data.
Ask it something: `python -m datalake.warehousing "SELECT source_id, parallax, phot_g_mean_mag FROM astro.nearby_stars ORDER BY parallax DESC LIMIT 5"`.
If you get exactly the limit, the output starts with `WARNING: INCOMPLETE FETCH` (the archive's quick queries
are reported to cut off silently at 2000 rows, so the limit cannot go above that).
Same rules as GFW: raw is never overwritten; `--raw PATH` re-cleans a file without the network.
Data: ESA Gaia DR3 (ESA/Gaia/DPAC).

