from datetime import datetime, timezone

import duckdb
import pytest

from datalake.cleaning import clean_csv
from datalake.landing import land_raw
from datalake.warehousing import load_table, query

WHEN = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
SCHEMA = {"name": "string", "depth_m": "float", "count": "int"}
CSV = "name,depth_m,count\ntuna,12.5,3\nshark,,7\nray,30.0,2\n"


def _clean_file(tmp_path, text=CSV):
    raw = land_raw(text.encode(), source="demo", dataset="fish", filename="f.csv", lake_root=tmp_path, now=WHEN)
    return clean_csv(raw, schema=SCHEMA, lake_root=tmp_path).path


def test_loads_into_named_schema_and_table(tmp_path):
    wh = tmp_path / "wh.duckdb"
    rows = load_table(_clean_file(tmp_path), schema="ocean", table="sightings", warehouse_path=wh)
    assert rows == 3
    assert query("SELECT count(*) FROM ocean.sightings", wh) == [(3,)]


def test_sql_questions_work_and_nulls_are_skipped_by_avg(tmp_path):
    wh = tmp_path / "wh.duckdb"
    load_table(_clean_file(tmp_path), schema="ocean", table="sightings", warehouse_path=wh)
    assert query("SELECT name FROM ocean.sightings WHERE depth_m > 20", wh) == [("ray",)]
    assert query("SELECT avg(depth_m) FROM ocean.sightings", wh) == [(21.25,)]  # (12.5 + 30) / 2


def test_reload_replaces_table_instead_of_duplicating(tmp_path):
    wh = tmp_path / "wh.duckdb"
    f = _clean_file(tmp_path)
    load_table(f, schema="ocean", table="sightings", warehouse_path=wh)
    load_table(f, schema="ocean", table="sightings", warehouse_path=wh)
    assert query("SELECT count(*) FROM ocean.sightings", wh) == [(3,)]


def test_two_domains_share_one_warehouse(tmp_path):
    wh = tmp_path / "wh.duckdb"
    f = _clean_file(tmp_path)
    load_table(f, schema="ocean", table="a", warehouse_path=wh)
    load_table(f, schema="astro", table="b", warehouse_path=wh)
    assert query("SELECT count(*) FROM ocean.a", wh) == [(3,)]
    assert query("SELECT count(*) FROM astro.b", wh) == [(3,)]


def test_query_is_read_only(tmp_path):
    wh = tmp_path / "wh.duckdb"
    load_table(_clean_file(tmp_path), schema="ocean", table="sightings", warehouse_path=wh)
    with pytest.raises(duckdb.Error):
        query("DROP TABLE ocean.sightings", wh)


def test_bad_names_and_missing_file_rejected(tmp_path):
    wh = tmp_path / "wh.duckdb"
    f = _clean_file(tmp_path)
    with pytest.raises(ValueError):
        load_table(f, schema="ocean; DROP TABLE x", table="t", warehouse_path=wh)
    with pytest.raises(FileNotFoundError):
        load_table(tmp_path / "nope.parquet", schema="ocean", table="t", warehouse_path=wh)
