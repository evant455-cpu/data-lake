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


async def land_gap_events(
    client,
    *,
    start_date: str,
    end_date: str,
    region: dict,
    limit: int = 100,
    lake_root: Path = Path("lake"),
) -> Path:
    """Fetch AIS-gap events ("went dark") and land them untouched in the raw layer.

    `client` is passed in (not created here) so tests can give a fake one.
    `region` looks like {"dataset": "public-eez-areas", "id": "5690"}.
    Returns the path of the raw JSON file.
    """
    # Imported here so the module loads even where the lake code is used without GFW tools.
    from datalake.landing import land_raw

    result = await client.events.get_all_events(
        datasets=[GAPS_DATASET],
        start_date=start_date,
        end_date=end_date,
        region=region,
        limit=limit,
    )
    df = result.df()
    # JSON keeps nested fields (vessel, gap, position) intact; CSV would flatten them to text.
    payload = "[]" if df is None or len(df) == 0 else df.to_json(orient="records", date_format="iso")

    return land_raw(
        payload.encode("utf-8"),
        source="gfw",
        dataset="gap-events",
        filename=f"{start_date}_{end_date}_{region['dataset']}-{region['id']}.json",
        lake_root=lake_root,
    )


@dataclass
class PipelineSummary:
    """What a full run did, printed at the end so nothing is hidden."""

    raw_path: Path
    clean_path: Path
    table: str
    rows_loaded: int
    bad_values: dict[str, int]


def process_raw(
    raw_file: Path,
    *,
    lake_root: Path | None = None,
    warehouse_path: Path | None = None,
    schema: str = "ocean",
    table: str = "gap_events",
) -> PipelineSummary:
    """Clean an already-landed raw file and load it into the warehouse. No network needed.

    `lake_root` is found from the path if not given (it sits four folders above the file:
    <lake_root>/raw/<source>/<dataset>/<date>/<file>). Safe to repeat: clean and warehouse
    are re-creatable, so running this again just rebuilds them from the same raw file.
    """
    from datalake.cleaning import clean_json
    from datalake.warehousing import DEFAULT_WAREHOUSE, load_table

    if lake_root is None:
        if len(raw_file.parents) < 5 or raw_file.parents[3].name != "raw":
            raise ValueError(f"Expected <lake>/raw/<source>/<dataset>/<date>/<file>, got: {raw_file}")
        lake_root = raw_file.parents[4]
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_file}")

    report = clean_json(raw_file, schema=GAP_EVENT_SCHEMA, lake_root=lake_root)
    rows = load_table(
        report.path, schema=schema, table=table, warehouse_path=warehouse_path or DEFAULT_WAREHOUSE
    )
    return PipelineSummary(raw_file, report.path, f"{schema}.{table}", rows, report.bad_values)


async def run_gap_pipeline(
    client,
    *,
    start_date: str,
    end_date: str,
    region: dict,
    limit: int = 100,
    lake_root: Path = Path("lake"),
    warehouse_path: Path | None = None,
    schema: str = "ocean",
    table: str = "gap_events",
) -> PipelineSummary:
    """The whole journey in one call: fetch -> raw -> clean -> warehouse.

    Note: the warehouse table holds the most recent run (load_table replaces it).
    Every raw and clean file from earlier runs is still kept on disk.
    """
    raw = await land_gap_events(
        client, start_date=start_date, end_date=end_date, region=region, limit=limit, lake_root=lake_root
    )
    return process_raw(raw, lake_root=lake_root, warehouse_path=warehouse_path, schema=schema, table=table)


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

    bad = {k: v for k, v in r.bad_values.items() if v}
    print(f"1. Raw:       {r.raw_path}")
    print(f"2. Clean:     {r.clean_path}")
    print(f"3. Warehouse: {r.table} ({r.rows_loaded} rows)")
    print(f"Values that did not fit their type: {bad or 'none'}")
    if r.rows_loaded == 0:
        print("No events in the file. Check the dates, region id and token.")
    print("Try: python -c \"from datalake.warehousing import query; print(query('SELECT count(*) FROM ocean.gap_events'))\"")
    print("Data: Global Fishing Watch")


if __name__ == "__main__":
    main()
