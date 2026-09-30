from datetime import datetime, timezone

import pyarrow.parquet as pq
import pytest

from datalake.cleaning import clean_csv
from datalake.landing import land_raw

WHEN = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
SCHEMA = {"name": "string", "depth_m": "float", "count": "int", "seen_at": "timestamp"}

CSV = (
    "name,depth_m,count,seen_at,extra\n"
    " tuna ,12.5,3,2026-01-02T03:04:05Z,x\n"
    "shark,,7,2026-01-03T00:00:00Z,y\n"
    "ray,abc,oops,not-a-date,z\n"
)


def _land(tmp_path, text=CSV):
    return land_raw(text.encode(), source="demo", dataset="fish", filename="f.csv", lake_root=tmp_path, now=WHEN)


def test_writes_typed_parquet_in_expected_place(tmp_path):
    report = clean_csv(_land(tmp_path), schema=SCHEMA, lake_root=tmp_path)
    assert report.path == tmp_path / "clean" / "demo" / "fish" / "2026-09-30.parquet"
    table = pq.read_table(report.path)
    assert table.column_names == ["name", "depth_m", "count", "seen_at"]  # "extra" dropped
    assert str(table.schema.field("depth_m").type) == "double"
    assert str(table.schema.field("count").type) == "int64"


def test_trims_blanks_become_null_bad_values_counted(tmp_path):
    report = clean_csv(_land(tmp_path), schema=SCHEMA, lake_root=tmp_path)
    rows = pq.read_table(report.path).to_pylist()
    assert rows[0]["name"] == "tuna"  # whitespace trimmed
    assert rows[1]["depth_m"] is None  # empty -> null
    assert rows[2]["depth_m"] is None and rows[2]["count"] is None and rows[2]["seen_at"] is None
    assert report.rows_in == report.rows_out == 3
    assert report.bad_values == {"name": 0, "depth_m": 1, "count": 1, "seen_at": 1}


def test_rerunning_overwrites_clean_but_raw_is_untouched(tmp_path):
    raw = _land(tmp_path)
    before = raw.read_bytes()
    clean_csv(raw, schema=SCHEMA, lake_root=tmp_path)
    clean_csv(raw, schema=SCHEMA, lake_root=tmp_path)  # no error: clean is re-creatable
    assert raw.read_bytes() == before


def test_missing_column_and_unknown_type_fail_loudly(tmp_path):
    raw = _land(tmp_path)
    with pytest.raises(ValueError, match="missing"):
        clean_csv(raw, schema={"nope": "string"}, lake_root=tmp_path)
    with pytest.raises(ValueError, match="Unknown column types"):
        clean_csv(raw, schema={"name": "decimal"}, lake_root=tmp_path)
