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
Since last time: `python -m datalake.connectors.gfw --incremental` fetches only what is new. It finds the latest end date
of a COMPLETE fetch for the region (read from the raw file names and sidecars), backs up 7 days (`--overlap-days`, because
GFW adds and revises events late), and fetches at most 90 days (`--max-days`). Far behind, each run catches up one window
and the next run continues; up to date, it says so and makes no requests. The very first run needs `--since YYYY-MM-DD`.
A cut-off fetch never moves the start forward, and exits with code 2 so automation can stop and tell you.
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
Past 2000 stars, fetch one distance band at a time with `--min-parallax` and `--max-parallax`, for example
`python -m datalake.connectors.gaia --min-parallax 50 --max-parallax 54.74 --limit 2000`
(stars with a parallax above 50 and at most 54.74 mas). Each band is its own raw file; make bands overlap a hair,
because the upsert (key `source_id`) quietly replaces repeats. A band that comes back under the limit has no warning,
which proves it is complete.
Same rules as GFW: raw is never overwritten; `--raw PATH` re-cleans a file without the network.
Data: ESA Gaia DR3 (ESA/Gaia/DPAC).

## Data-quality checks

```
python -m datalake.quality
```

Runs a set of rules over every loaded table and prints one line per rule: `[ok]`, `[WARN]` (suspicious, such as a
gap longer than a year) or `[ERROR]` (impossible, such as a gap that ends before it starts), with a few example
rows under anything flagged. It only flags: nothing is changed or deleted, and it opens the warehouse read-only.
Exit code is 1 if any `[ERROR]` rule flags rows (warnings do not fail the run), so a scheduled job can stop on it.
Rules sit next to each source's schema: `GAP_EVENT_CHECKS` in `connectors/gfw.py`,
`NEARBY_STARS_CHECKS` in `connectors/gaia.py`. A new source adds its own list and registers it in `quality.all_checks`.

## Rebuild from raw

Clean files and the warehouse can always be re-created from raw. To prove it on your own data without touching
anything, build a second warehouse and compare it with the real one:

```
python -m datalake.rebuild --warehouse warehouse/rebuilt.duckdb --compare warehouse/warehouse.duckdb
```

It replays every raw file in the order it was fetched (from each file's `.meta.json`), re-creates the clean files,
loads the rebuilt warehouse, then reports each table as `identical` or `DIFFERENT` (exit code 1 on a difference).
An existing warehouse is never overwritten unless you add `--replace`; the new one is built beside it and swapped
in only if every file worked. Raw files are only read. Raw files of a source it does not know yet are skipped with a note.

## Backfill queue: walk big jobs one slice at a time

Some data is too big for one request. The backfill queue cuts a job into slices and does one per run,
remembering which are done in `lake/state/backfill.json`. Right now the slices are Gaia distance shells,
20 to 30 parsecs in steps of 2 (the closer 20 pc sphere is already complete).

    python -m datalake.backfill --status      # lists done / pending, changes nothing
    python -m datalake.backfill               # runs the next pending slice (one per run)
    python -m datalake.backfill --max-slices 3

A slice counts as done only if its fetch was complete. A shell that hits the row limit, or any crash, stops
the run, stays pending, and exits with code 2. If a shell is cut off, it holds too many stars and needs to
be made thinner. Running the same slice twice on one day is refused (raw is never overwritten); try again tomorrow.

## Fast-moving stars

`python -m datalake.motion` turns each star's drift across the sky into a real speed (km/s) and lists the fastest,
with the median speed for comparison. Formula: speed = 4.74047 x proper motion (mas/yr) / parallax (mas). It is the
sideways (tangential) part only, relative to the Sun. Read-only; use `--top N` for a longer list.

## Close flybys of the Sun

`python -m datalake.flybys` finds which nearby stars will pass closest to the Sun, assuming each keeps moving in a
straight line. The sideways speed sets the miss distance and the toward/away speed sets the time:
time = -d x v_r / (v_r^2 + v_t^2), miss = d x v_t / sqrt(v_r^2 + v_t^2). Options: `--top N`, `--max-myr M`
(default 5 million years), `--past` (stars that already passed). Only stars with a radial velocity can be placed.
No uncertainties yet, so treat results as leads, not facts.

