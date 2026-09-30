import json
from datetime import datetime, timezone

import pyarrow.parquet as pq
import pytest

from datalake.cleaning import clean_json
from datalake.landing import land_raw

WHEN = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
SCHEMA = {
    "vid": ("string", "vessel.id"),
    "hours": ("float", "gap.duration_hours"),
    "dark": ("bool", "gap.intentional_disabling"),
    "start": ("timestamp", "start"),
}
RECORDS = [
    {"start": "2022-01-01T00:00:00.000Z", "vessel": {"id": " A "}, "gap": {"duration_hours": "20", "intentional_disabling": True}},
    {"start": "2022-01-02T00:00:00Z", "vessel": {"id": "B"}, "gap": {"duration_hours": None, "intentional_disabling": False}},
    {"start": "garbage", "vessel": {"id": {"oops": 1}}, "gap": {"duration_hours": "abc"}},  # bad values, key absent
    {"vessel": {}},  # almost everything missing
]


def _land(tmp_path, records=RECORDS, name="e.json"):
    return land_raw(json.dumps(records).encode(), source="demo", dataset="ev", filename=name, lake_root=tmp_path, now=WHEN)


def test_flattens_nested_paths_into_typed_columns(tmp_path):
    report = clean_json(_land(tmp_path), schema=SCHEMA, lake_root=tmp_path)
    assert report.path == tmp_path / "clean" / "demo" / "ev" / "2026-09-30" / "e.parquet"
    table = pq.read_table(report.path)
    assert table.column_names == ["vid", "hours", "dark", "start"]
    assert [str(table.schema.field(n).type) for n in table.column_names][:3] == ["string", "double", "bool"]
    rows = table.to_pylist()
    assert rows[0]["vid"] == "A" and rows[0]["hours"] == 20.0 and rows[0]["dark"] is True
    assert rows[0]["start"] == datetime(2022, 1, 1, tzinfo=timezone.utc)
    assert rows[1]["dark"] is False


def test_null_missing_bad_and_nested_record_handling(tmp_path):
    report = clean_json(_land(tmp_path), schema=SCHEMA, lake_root=tmp_path)
    rows = pq.read_table(report.path).to_pylist()
    assert rows[1]["hours"] is None                      # explicit null -> null, not bad
    assert rows[2] == {"vid": None, "hours": None, "dark": None, "start": None}
    assert rows[3] == {"vid": None, "hours": None, "dark": None, "start": None}  # absent keys -> null
    # bad: row 3 vid (a whole record), hours "abc", start "garbage". Missing/null are NOT bad.
    assert report.bad_values == {"vid": 1, "hours": 1, "dark": 0, "start": 1}
    assert report.rows_in == report.rows_out == 4


def test_empty_list_gives_empty_table(tmp_path):
    report = clean_json(_land(tmp_path, records=[]), schema=SCHEMA, lake_root=tmp_path)
    assert report.rows_out == 0 and pq.read_table(report.path).column_names == list(SCHEMA)


def test_bad_inputs_fail_loudly(tmp_path):
    with pytest.raises(ValueError, match="list of records"):
        clean_json(_land(tmp_path, records={"not": "a list"}, name="a.json"), schema=SCHEMA, lake_root=tmp_path)
    with pytest.raises(ValueError, match="Unknown column types"):
        clean_json(_land(tmp_path, name="b.json"), schema={"x": ("decimal", "a")}, lake_root=tmp_path)
