import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datalake.warehousing import append_table, load_table, query

KEY = ("vid", "start")


def _file(tmp_path, name, rows, cols=("vid", "start", "hours")):
    """Make a small clean-style Parquet file from (vid, start, hours) tuples."""
    table = pa.table({c: [r[i] for r in rows] for i, c in enumerate(cols)})
    path = tmp_path / name
    pq.write_table(table, path)
    return path


def _append(tmp_path, f, wh=None):
    return append_table(f, schema="ocean", table="gaps", key=KEY, warehouse_path=wh or tmp_path / "wh.duckdb")


def test_first_append_creates_table_with_lineage_column(tmp_path):
    f = _file(tmp_path, "run1.parquet", [("A", "d1", 1.0), ("B", "d2", 2.0)])
    r = _append(tmp_path, f)
    assert (r.new, r.updated, r.total) == (2, 0, 2)
    assert query("SELECT DISTINCT _source_file FROM ocean.gaps", tmp_path / "wh.duckdb") == [("run1.parquet",)]


def test_overlapping_runs_dedupe_and_newer_wins(tmp_path):
    wh = tmp_path / "wh.duckdb"
    _append(tmp_path, _file(tmp_path, "run1.parquet", [("A", "d1", 1.0), ("B", "d2", 2.0)]))
    r = _append(tmp_path, _file(tmp_path, "run2.parquet", [("B", "d2", 99.0), ("C", "d3", 3.0)]))
    assert (r.new, r.updated, r.total) == (1, 1, 3)
    rows = dict((v, (h, s)) for v, h, s in query("SELECT vid, hours, _source_file FROM ocean.gaps", wh))
    assert rows["B"] == (99.0, "run2.parquet")  # newer value replaced the old one
    assert rows["A"] == (1.0, "run1.parquet")  # untouched row keeps its own lineage


def test_repeating_the_same_file_changes_nothing(tmp_path):
    f = _file(tmp_path, "run1.parquet", [("A", "d1", 1.0), ("B", "d2", 2.0)])
    _append(tmp_path, f)
    r = _append(tmp_path, f)
    assert (r.new, r.updated, r.total) == (0, 2, 2)


def test_empty_keys_are_skipped_and_reported_and_in_file_duplicates_last_wins(tmp_path):
    wh = tmp_path / "wh.duckdb"
    f = _file(tmp_path, "run1.parquet", [("A", "d1", 1.0), ("A", "d1", 5.0), (None, "d2", 2.0), ("B", None, 3.0)])
    r = _append(tmp_path, f)
    assert (r.new, r.skipped_no_key, r.duplicates_in_file, r.total) == (1, 2, 1, 1)
    assert query("SELECT hours FROM ocean.gaps", wh) == [(5.0,)]  # last copy won


def test_upgrades_a_table_made_by_load_table(tmp_path):
    wh = tmp_path / "wh.duckdb"
    old = _file(tmp_path, "old.parquet", [("A", "d1", 1.0)])
    load_table(old, schema="ocean", table="gaps", warehouse_path=wh)  # no _source_file column yet
    r = _append(tmp_path, _file(tmp_path, "run2.parquet", [("A", "d1", 7.0), ("B", "d2", 2.0)]))
    assert (r.new, r.updated, r.total) == (1, 1, 2)
    assert sorted(query("SELECT vid, hours, _source_file FROM ocean.gaps", wh)) == [
        ("A", 7.0, "run2.parquet"), ("B", 2.0, "run2.parquet")]


def test_old_rows_from_load_table_keep_unknown_lineage(tmp_path):
    wh = tmp_path / "wh.duckdb"
    load_table(_file(tmp_path, "old.parquet", [("A", "d1", 1.0)]), schema="ocean", table="gaps", warehouse_path=wh)
    _append(tmp_path, _file(tmp_path, "run2.parquet", [("B", "d2", 2.0)]))
    assert sorted(query("SELECT vid, _source_file FROM ocean.gaps", wh), key=lambda r: r[0]) == [
        ("A", None), ("B", "run2.parquet")]


def test_changed_columns_fail_loudly_and_leave_table_untouched(tmp_path):
    wh = tmp_path / "wh.duckdb"
    _append(tmp_path, _file(tmp_path, "run1.parquet", [("A", "d1", 1.0)]))
    drifted = _file(tmp_path, "run2.parquet", [("B", "d2", 2.0, "extra")], cols=("vid", "start", "hours", "surprise"))
    with pytest.raises(ValueError, match="Columns differ"):
        _append(tmp_path, drifted)
    assert query("SELECT vid FROM ocean.gaps", wh) == [("A",)]


def test_bad_inputs_rejected(tmp_path):
    f = _file(tmp_path, "run1.parquet", [("A", "d1", 1.0)])
    wh = tmp_path / "wh.duckdb"
    with pytest.raises(ValueError, match="Key columns not in the file"):
        append_table(f, schema="ocean", table="gaps", key=("nope",), warehouse_path=wh)
    with pytest.raises(ValueError):
        append_table(f, schema="ocean", table="gaps", key=(), warehouse_path=wh)
    with pytest.raises(ValueError):
        append_table(f, schema="ocean", table="gaps", key=("vid; DROP TABLE x",), warehouse_path=wh)
    with pytest.raises(FileNotFoundError):
        append_table(tmp_path / "nope.parquet", schema="ocean", table="gaps", key=KEY, warehouse_path=wh)
