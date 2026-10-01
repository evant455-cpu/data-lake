import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datalake.warehousing import ask, load_table, main


def _wh(tmp_path):
    f = tmp_path / "t.parquet"
    pq.write_table(pa.table({"name": ["tuna", "shark", None], "depth": [12.5, 30.0, 7.0]}), f)
    wh = tmp_path / "wh.duckdb"
    load_table(f, schema="ocean", table="fish", warehouse_path=wh)
    return wh, f


def test_ask_returns_aligned_table_with_header_and_row_count(tmp_path):
    wh, _ = _wh(tmp_path)
    out = ask("SELECT name, depth FROM ocean.fish ORDER BY depth DESC", wh)
    lines = out.splitlines()
    assert lines[0].split() == ["name", "depth"]
    assert lines[2].split() == ["shark", "30.0"]
    assert lines[-1] == "(3 rows)"
    assert "None" not in out  # empty values show as blanks


def test_ask_limits_rows_and_says_so(tmp_path):
    wh, _ = _wh(tmp_path)
    out = ask("SELECT * FROM ocean.fish", wh, max_rows=1)
    assert out.splitlines()[-1] == "(3 rows, showing 1)"


def test_ask_can_read_clean_files_directly(tmp_path):
    wh, f = _wh(tmp_path)
    assert "3" in ask(f"SELECT count(*) AS n FROM read_parquet('{f.as_posix()}')", wh)


def test_ask_cannot_change_the_warehouse(tmp_path):
    wh, _ = _wh(tmp_path)
    with pytest.raises(Exception):
        ask("DROP TABLE ocean.fish", wh)
    assert "(3 rows)" in ask("SELECT * FROM ocean.fish", wh)


def test_cli_prints_answer_and_reports_sql_mistakes_plainly(tmp_path, capsys):
    wh, _ = _wh(tmp_path)
    main(["SELECT count(*) AS n FROM ocean.fish", "--warehouse", str(wh)])
    assert "(1 row)" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="SQL problem"):
        main(["SELECT nope FROM ocean.fish", "--warehouse", str(wh)])


def test_ask_can_return_timestamp_columns(tmp_path):
    from datetime import datetime, timezone

    f = tmp_path / "ts.parquet"
    pq.write_table(pa.table({"seen": pa.array([datetime(2022, 1, 2, 3, 4, 5, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC"))}), f)
    wh = tmp_path / "wh.duckdb"
    load_table(f, schema="ocean", table="ts", warehouse_path=wh)
    assert "2022-01-02 03:04:05+00:00" in ask("SELECT seen FROM ocean.ts", wh)  # UTC, not this computer's local time


def test_query_also_returns_utc(tmp_path):
    from datetime import datetime, timezone

    from datalake.warehousing import query

    f = tmp_path / "ts.parquet"
    pq.write_table(pa.table({"seen": pa.array([datetime(2022, 1, 2, 3, 4, 5, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC"))}), f)
    wh = tmp_path / "wh.duckdb"
    load_table(f, schema="ocean", table="ts", warehouse_path=wh)
    assert query("SELECT seen FROM ocean.ts", wh)[0][0].utcoffset().total_seconds() == 0
    assert query("SELECT seen FROM ocean.ts", wh)[0][0].hour == 3
