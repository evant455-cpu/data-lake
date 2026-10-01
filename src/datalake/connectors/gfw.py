"""Lesson 4: the first connector, for Global Fishing Watch (GFW).

A connector knows how to fetch ONE source and hand the bytes to the raw layer.
It does no cleaning. The API call mirrors the one verified in the ocean-watch
project (src/oceanwatch/fetch.py). It has NOT been tested against the live API
from this sandbox (the network blocks it); tests use a fake client.

Data: Global Fishing Watch. Non-commercial use.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from datalake.quality import Check

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


# Data-quality rules (Lesson 6). A row matching the condition is flagged, never removed.
# "end" is quoted because END is an SQL keyword. 8760 hours = 365 days.
GAP_EVENT_CHECKS = [
    Check("end_before_start", "ocean.gap_events", '"end" < start', "error",
          "A gap cannot end before it starts."),
    Check("non_positive_duration", "ocean.gap_events", "duration_hours <= 0", "error",
          "A gap must last longer than zero hours."),
    Check("missing_duration", "ocean.gap_events", "duration_hours IS NULL", "warning",
          "GFW normally sends a duration; check the raw record."),
    Check("extreme_duration", "ocean.gap_events", "duration_hours > 8760", "warning",
          "Longer than a year: real but rare (an event overlapping our window), and it can distort averages."),
]


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
        # Provenance: lets a later incremental run know this window was (or was not) fully fetched.
        extra_meta={
            "complete": complete,
            "rows": 0 if combined is None else len(combined),
            "pages": requests,
            "start_date": start_date,
            "end_date": end_date,
        },
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


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


# Raw gap files are named <start>_<end>_<region dataset>-<region id>.json (see land_gap_events).
_RAW_NAME = re.compile(r"^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(.+)\.json$")


def covered_through(lake_root: Path, region: dict) -> date | None:
    """The latest end date we hold a COMPLETE fetch for, for this region (None if we hold none).

    Read from the raw files themselves: the name says which window and region, and the sidecar says
    whether the fetch was complete. A cut-off fetch must not count, or we would skip the missing events
    forever. Files from before the flag existed (no `complete` field) are taken as complete.
    """
    folder = lake_root / "raw" / "gfw" / "gap-events"
    key = f"{region['dataset']}-{region['id']}"
    best = None
    for f in folder.glob("*/*.json"):
        m = _RAW_NAME.match(f.name)
        if f.name.endswith(".meta.json") or not m or m.group(3) != key:
            continue
        try:
            end = date.fromisoformat(m.group(2))
        except ValueError:
            continue
        try:
            complete = bool(json.loads(f.with_name(f.name + ".meta.json").read_text(encoding="utf-8")).get("complete", True))
        except OSError:
            complete = True  # no sidecar at all: an old or hand-placed file
        except ValueError:
            complete = False  # a damaged sidecar: do not trust it
        if complete and (best is None or end > best):
            best = end
    return best


@dataclass(frozen=True)
class IncrementalPlan:
    start_date: str
    end_date: str
    catching_up: bool  # True = this window stops before today, so more windows will follow


def plan_incremental(
    lake_root: Path,
    region: dict,
    *,
    today: date,
    overlap_days: int = 7,
    max_days: int = 90,
    since: str | None = None,
) -> IncrementalPlan | None:
    """Decide the next window to fetch: from a week before our coverage ends, at most `max_days` long.

    The overlap re-asks for the last few days because GFW adds and revises events late; the upsert makes
    the repeats harmless. The cap is the throttle: far behind, each run catches up one window and the
    next run continues. Returns None when we are already covered through today.
    `since` (YYYY-MM-DD) is needed for the very first run, when there is no earlier fetch to continue from.
    """
    if max_days < 1 or overlap_days < 0:
        raise ValueError("max_days must be at least 1 and overlap_days cannot be negative")
    if since is not None:
        start = date.fromisoformat(since)
        if start >= today:
            raise ValueError(f"--since {since} must be before today ({today})")
    else:
        covered = covered_through(lake_root, region)
        if covered is None:
            raise ValueError(
                "No earlier complete fetch found for this region, so there is no 'since last time' yet. "
                "Give a first start date with --since YYYY-MM-DD."
            )
        if covered >= today:
            return None
        start = covered - timedelta(days=overlap_days)
    end = min(today, start + timedelta(days=max_days))
    return IncrementalPlan(str(start), str(end), catching_up=end < today)


@dataclass
class IncrementalRun:
    plan: IncrementalPlan
    summary: PipelineSummary


async def run_incremental(
    client,
    *,
    region: dict,
    today: date | None = None,
    overlap_days: int = 7,
    max_days: int = 90,
    since: str | None = None,
    limit: int = 100,
    max_pages: int = 50,
    lake_root: Path = Path("lake"),
    warehouse_path: Path | None = None,
    schema: str = "ocean",
    table: str = "gap_events",
) -> IncrementalRun | None:
    """Fetch "since last time": plan the next window, then fetch -> raw -> clean -> warehouse.

    Returns None (and makes no requests) when we are already covered through today.
    """
    plan = plan_incremental(
        lake_root, region, today=today or _utc_today(), overlap_days=overlap_days, max_days=max_days, since=since
    )
    if plan is None:
        return None
    summary = await run_gap_pipeline(
        client, start_date=plan.start_date, end_date=plan.end_date, region=region, limit=limit,
        max_pages=max_pages, lake_root=lake_root, warehouse_path=warehouse_path, schema=schema, table=table,
    )
    return IncrementalRun(plan, summary)


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


def main() -> int:
    """Command line.

    Fetch + clean + load:  python -m datalake.connectors.gfw START END [REGION_ID]
    Since last time:       python -m datalake.connectors.gfw --incremental [--since YYYY-MM-DD]
    Re-clean a raw file:   python -m datalake.connectors.gfw --raw PATH_TO_RAW_JSON
    Exit code 2 means the fetch may be incomplete (so automation can stop and tell you).
    """
    import argparse
    import asyncio

    p = argparse.ArgumentParser(description="Global Fishing Watch AIS-gap events: raw -> clean -> warehouse.")
    p.add_argument("start_date", nargs="?")
    p.add_argument("end_date", nargs="?")
    p.add_argument("region_id", nargs="?", default="5690")
    p.add_argument("--raw", help="Skip fetching: clean and load this existing raw JSON file (no token needed).")
    p.add_argument("--incremental", action="store_true",
                   help="Fetch only what is new since the last complete fetch (plus a few days of overlap).")
    p.add_argument("--since", help="First start date (YYYY-MM-DD) for the very first --incremental run.")
    p.add_argument("--region", default="5690", help="Region id for --incremental (default 5690, the Russian EEZ).")
    p.add_argument("--overlap-days", type=int, default=7, help="Days to re-ask before the last coverage (default 7).")
    p.add_argument("--max-days", type=int, default=90, help="Longest window per run, the catch-up throttle (default 90).")
    a = p.parse_args()

    if a.raw:
        r = process_raw(Path(a.raw))
        print("\n".join(summary_lines(r)))
        return 0

    if not a.incremental and not (a.start_date and a.end_date):
        p.error("give START and END dates, or use --incremental, or --raw PATH")
    import gfwapiclient as gfw  # installed separately: pip install gfw-api-python-client

    client = gfw.Client(access_token=get_token())
    try:
        if a.incremental:
            region = {"dataset": "public-eez-areas", "id": a.region}
            try:
                run = asyncio.run(run_incremental(
                    client, region=region, since=a.since, overlap_days=a.overlap_days, max_days=a.max_days))
            except ValueError as e:
                raise SystemExit(str(e))
            if run is None:
                print(f"Already up to date through {covered_through(Path('lake'), region)} (UTC). Nothing to fetch.")
                return 0
            note = " (catching up: more windows remain, run again)" if run.plan.catching_up else ""
            print(f"Incremental fetch: {run.plan.start_date} to {run.plan.end_date}{note}")
            r = run.summary
        else:
            region = {"dataset": "public-eez-areas", "id": a.region_id}
            r = asyncio.run(run_gap_pipeline(client, start_date=a.start_date, end_date=a.end_date, region=region))
    except FileExistsError as e:
        # The raw layer never overwrites. Same dates + region on the same day = same filename.
        raise SystemExit(
            f"Already fetched today, raw data is never overwritten.\n{e}\n"
            "To re-clean that file without fetching again, run:\n"
            "  python -m datalake.connectors.gfw --raw <that path>"
        )

    print("\n".join(summary_lines(r)))
    return 0 if (r.complete or not a.incremental) else 2


if __name__ == "__main__":
    import sys

    sys.exit(main())
