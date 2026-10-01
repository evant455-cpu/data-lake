"""Lesson 4: the first connector, for Global Fishing Watch (GFW).

A connector knows how to fetch ONE source and hand the bytes to the raw layer.
It does no cleaning. The API call mirrors the one verified in the ocean-watch
project (src/oceanwatch/fetch.py). It has NOT been tested against the live API
from this sandbox (the network blocks it); tests use a fake client.

Data: Global Fishing Watch. Non-commercial use.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

GAPS_DATASET = "public-global-gaps-events:latest"
TOKEN_ENV = "GFW_API_ACCESS_TOKEN"

# How to flatten a raw gap event into a clean table: column -> (type, path into the record).
# Field names follow the ones ocean-watch's analysis already relies on. GFW sends
# duration_hours as text, so the cleaner converts it to a real number.
# One gap = one vessel going dark at one moment, so these two columns identify an event.
# If the same event shows up in two fetches, the newer copy replaces the older one.
GAP_EVENT_KEY = ("vessel_id", "start")

GAP_EVENT_SCHEMA = {
    "start": ("timestamp", "start"),
    "end": ("timestamp", "end"),
    "vessel_id": ("string", "vessel.id"),
    "vessel_name": ("string", "vessel.name"),
    "vessel_type": ("string", "vessel.type"),
    "vessel_flag": ("string", "vessel.flag"),
    "duration_hours": ("float", "gap.duration_hours"),
    "intentional_disabling": ("bool", "gap.intentional_disabling"),
}


def get_token(env_file: Path = Path(".env")) -> str:
    """Find the token: environment variable first, then a git-ignored .env file.

    Never put the token in code or chat. Raises if it is not found.
    """
    token = os.environ.get(TOKEN_ENV, "").strip()
    if token:
        return token
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"{TOKEN_ENV}="):
                token = line.split("=", 1)[1].strip().strip('"').strip("'")
                if token:
                    return token
    raise RuntimeError(f"No GFW token found. Set {TOKEN_ENV} or add it to a .env file.")


@dataclass
class FetchResult:
    """What a fetch brought back, including whether we know it is everything."""

    path: Path  # the raw JSON file
    rows: int  # events in that file
    pages: int  # requests made to GFW
    complete: bool  # False = there may be more events GFW did not give us
    warning: str | None = None  # plain-words explanation when complete is False


async def land_gap_events(
    client,
    *,
    start_date: str,
    end_date: str,
    region: dict,
    limit: int = 100,
    max_pages: int = 50,
    lake_root: Path = Path("lake"),
) -> FetchResult:
    """Fetch AIS-gap events ("went dark") and land them untouched in the raw layer.

    GFW answers in pages of at most `limit` events, so we keep asking for the next page
    (`offset` = how many events to skip) until a page comes back short or empty.
    Two safety nets stop an endless or silently-cut fetch:
      - `max_pages` caps the number of requests (also protects the daily rate limit);
      - if a page is identical to the one before, the API is ignoring `offset`, so we stop.
    In both cases the events fetched so far are still landed, and the result says
    `complete=False` with a warning, so a cut-off fetch is never mistaken for a full one.

    `client` is passed in (not created here) so tests can give a fake one.
    `region` looks like {"dataset": "public-eez-areas", "id": "5690"}.
    """
    # Imported here so the module loads even where the lake code is used without GFW tools.
    import pandas as pd

    from datalake.landing import land_raw

    pages: list = []
    complete, warning, requests = False, None, 0
    for page in range(max_pages):
        result = await client.events.get_all_events(
            datasets=[GAPS_DATASET],
            start_date=start_date,
            end_date=end_date,
            region=region,
            limit=limit,
            offset=page * limit,
        )
        requests += 1
        df = result.df()
        if df is None or len(df) == 0:
            complete = True  # nothing more to give
            break
        if pages and df.equals(pages[-1]):
            warning = (
                "GFW sent the same page twice, so it seems to ignore `offset`. "
                f"Kept the first {sum(len(x) for x in pages)} events; there may be more."
            )
            break
        pages.append(df)
        if len(df) < limit:
            complete = True  # a short page is the last page
            break
    else:
        got = sum(len(x) for x in pages)
        warning = (
            f"Stopped after {max_pages} pages ({got} events) and GFW still had more. "
            "Fetch a narrower date range (or a higher max_pages) to get the rest."
        )

    combined = pd.concat(pages, ignore_index=True) if pages else None
    # JSON keeps nested fields (vessel, gap, position) intact; CSV would flatten them to text.
    payload = "[]" if combined is None else combined.to_json(orient="records", date_format="iso")
    raw = land_raw(
        payload.encode("utf-8"),
        source="gfw",
        dataset="gap-events",
        filename=f"{start_date}_{end_date}_{region['dataset']}-{region['id']}.json",
        lake_root=lake_root,
    )
    return FetchResult(
        path=raw, rows=0 if combined is None else len(combined), pages=requests,
        complete=complete, warning=warning,
    )


@dataclass
class PipelineSummary:
    """What a full run did, printed at the end so nothing is hidden."""

    raw_path: Path
    clean_path: Path
    table: str
    rows_loaded: int  # rows this file contributed (new + updated)
    bad_values: dict[str, int]
    new: int = 0  # events not seen before
    updated: int = 0  # events already in the table, replaced by this newer copy
    skipped_no_key: int = 0  # events missing vessel id or start time (cannot be matched)
    duplicates_in_file: int = 0
    table_total: int = 0  # rows in the table afterwards
    rows_fetched: int = 0  # events GFW gave us in this fetch (0 for --raw)
    pages: int = 0  # requests made to GFW (0 for --raw)
    complete: bool = True  # False = the fetch may be missing events
    warning: str | None = None


def process_raw(
    raw_file: Path,
    *,
    lake_root: Path | None = None,
    warehouse_path: Path | None = None,
    schema: str = "ocean",
    table: str = "gap_events",
) -> PipelineSummary:
    """Clean an already-landed raw file and add it to the warehouse table. No network needed.

    `lake_root` is found from the path if not given (it sits four folders above the file:
    <lake_root>/raw/<source>/<dataset>/<date>/<file>). Safe to repeat: clean and warehouse
    are re-creatable, so running this again just rebuilds them from the same raw file.
    """
    from datalake.cleaning import clean_json
    from datalake.warehousing import DEFAULT_WAREHOUSE, append_table

    if lake_root is None:
        if len(raw_file.parents) < 5 or raw_file.parents[3].name != "raw":
            raise ValueError(f"Expected <lake>/raw/<source>/<dataset>/<date>/<file>, got: {raw_file}")
        lake_root = raw_file.parents[4]
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_file}")

    report = clean_json(raw_file, schema=GAP_EVENT_SCHEMA, lake_root=lake_root)
    res = append_table(
        report.path, schema=schema, table=table, key=GAP_EVENT_KEY,
        warehouse_path=warehouse_path or DEFAULT_WAREHOUSE,
    )
    return PipelineSummary(
        raw_path=raw_file, clean_path=report.path, table=f"{schema}.{table}",
        rows_loaded=res.new + res.updated, bad_values=report.bad_values, new=res.new,
        updated=res.updated, skipped_no_key=res.skipped_no_key,
        duplicates_in_file=res.duplicates_in_file, table_total=res.total,
    )


async def run_gap_pipeline(
    client,
    *,
    start_date: str,
    end_date: str,
    region: dict,
    limit: int = 100,
    max_pages: int = 50,
    lake_root: Path = Path("lake"),
    warehouse_path: Path | None = None,
    schema: str = "ocean",
    table: str = "gap_events",
) -> PipelineSummary:
    """The whole journey in one call: fetch -> raw -> clean -> warehouse.

    Each run is added to the warehouse table; events seen before are replaced by the newer copy.
    """
    fetch = await land_gap_events(
        client, start_date=start_date, end_date=end_date, region=region,
        limit=limit, max_pages=max_pages, lake_root=lake_root,
    )
    summary = process_raw(fetch.path, lake_root=lake_root, warehouse_path=warehouse_path, schema=schema, table=table)
    summary.rows_fetched, summary.pages = fetch.rows, fetch.pages
    summary.complete, summary.warning = fetch.complete, fetch.warning
    return summary


def summary_lines(r: PipelineSummary) -> list[str]:
    """The text the command line prints after a run (kept separate so it can be tested)."""
    lines = []
    if not r.complete:
        lines += [f"WARNING: INCOMPLETE FETCH. {r.warning}", ""]
    lines += [
        f"1. Raw:       {r.raw_path}",
        f"2. Clean:     {r.clean_path}",
        f"3. Warehouse: {r.table}: {r.new} new, {r.updated} updated, {r.table_total} total",
    ]
    if r.pages:
        lines.append(f"   Fetched {r.rows_fetched} events in {r.pages} request{'s' if r.pages != 1 else ''}.")
    if r.skipped_no_key or r.duplicates_in_file:
        lines.append(f"   Not loaded: {r.skipped_no_key} missing vessel id or start time, "
                     f"{r.duplicates_in_file} repeated inside the file")
    bad = {k: v for k, v in r.bad_values.items() if v}
    lines.append(f"Values that did not fit their type: {bad or 'none'}")
    if r.rows_loaded == 0:
        lines.append("No events in the file. Check the dates, region id and token.")
    lines.append("Try: python -m datalake.warehousing \"SELECT count(*) FROM ocean.gap_events\"")
    lines.append("Data: Global Fishing Watch (CC BY-NC 4.0, non-commercial use)")
    return lines


def main() -> None:
    """Command line.

    Fetch + clean + load:  python -m datalake.connectors.gfw START END [REGION_ID]
    Re-clean a raw file:   python -m datalake.connectors.gfw --raw PATH_TO_RAW_JSON
    """
    import argparse
    import asyncio

    p = argparse.ArgumentParser(description="Global Fishing Watch AIS-gap events: raw -> clean -> warehouse.")
    p.add_argument("start_date", nargs="?")
    p.add_argument("end_date", nargs="?")
    p.add_argument("region_id", nargs="?", default="5690")
    p.add_argument("--raw", help="Skip fetching: clean and load this existing raw JSON file (no token needed).")
    a = p.parse_args()

    if a.raw:
        r = process_raw(Path(a.raw))
    else:
        if not (a.start_date and a.end_date):
            p.error("give START and END dates, or use --raw PATH")
        import gfwapiclient as gfw  # installed separately: pip install gfw-api-python-client

        client = gfw.Client(access_token=get_token())
        region = {"dataset": "public-eez-areas", "id": a.region_id}
        try:
            r = asyncio.run(run_gap_pipeline(client, start_date=a.start_date, end_date=a.end_date, region=region))
        except FileExistsError as e:
            # The raw layer never overwrites. Same dates + region on the same day = same filename.
            raise SystemExit(
                f"Already fetched today, raw data is never overwritten.\n{e}\n"
                "To re-clean that file without fetching again, run:\n"
                "  python -m datalake.connectors.gfw --raw <that path>"
            )

    print("\n".join(summary_lines(r)))


if __name__ == "__main__":
    main()
