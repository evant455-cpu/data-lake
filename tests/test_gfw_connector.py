import asyncio
import json

import pandas as pd
import pytest

from datalake.connectors.gfw import GAPS_DATASET, TOKEN_ENV, get_token, land_gap_events

REGION = {"dataset": "public-eez-areas", "id": "5690"}


class FakeResult:
    def __init__(self, df):
        self._df = df

    def df(self):
        return self._df


class FakeEvents:
    """Behaves like a paged API: each call returns one slice of the data.

    ignore_offset=True imitates an API that silently ignores `offset` (always page 1).
    """

    def __init__(self, df, ignore_offset=False):
        self.df, self.calls, self.ignore_offset = df, [], ignore_offset

    async def get_all_events(self, **kwargs):
        self.calls.append(kwargs)
        offset = 0 if self.ignore_offset else kwargs.get("offset", 0)
        limit = kwargs.get("limit")
        end = None if limit is None else offset + limit
        return FakeResult(self.df.iloc[offset:end])


class FakeClient:
    def __init__(self, df, ignore_offset=False):
        self.events = FakeEvents(df, ignore_offset)


GAPS = pd.DataFrame({
    "start": pd.to_datetime(["2022-01-01T00:00:00Z"], utc=True),
    "vessel": [{"id": "A", "flag": "RUS"}],
    "gap": [{"duration_hours": "20", "intentional_disabling": True}],
})


def _land(tmp_path, df=GAPS):
    client = FakeClient(df)
    fetch = asyncio.run(land_gap_events(
        client, start_date="2022-01-01", end_date="2022-05-01", region=REGION, lake_root=tmp_path))
    return client, fetch.path


def test_lands_json_with_nested_fields_and_sidecar(tmp_path):
    _, path = _land(tmp_path)
    assert path.relative_to(tmp_path).parts[:3] == ("raw", "gfw", "gap-events")
    rows = json.loads(path.read_text())
    assert rows[0]["vessel"] == {"id": "A", "flag": "RUS"}  # nesting preserved
    assert rows[0]["start"].startswith("2022-01-01")
    assert (path.parent / f"{path.name}.meta.json").exists()


def test_asks_gfw_for_the_right_dataset_and_region(tmp_path):
    client, _ = _land(tmp_path)
    call = client.events.calls[0]
    assert call["datasets"] == [GAPS_DATASET] and call["region"] == REGION
    assert call["start_date"] == "2022-01-01" and call["end_date"] == "2022-05-01"


def test_empty_result_lands_empty_list(tmp_path):
    _, path = _land(tmp_path, df=pd.DataFrame())
    assert json.loads(path.read_text()) == []


def test_token_env_then_file_then_error(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    with pytest.raises(RuntimeError):
        get_token(env)
    env.write_text(f'{TOKEN_ENV}="from-file"\n')
    assert get_token(env) == "from-file"
    monkeypatch.setenv(TOKEN_ENV, "from-env")
    assert get_token(env) == "from-env"


def test_end_to_end_fake_gfw_to_warehouse_sql(tmp_path):
    from datalake.cleaning import clean_json
    from datalake.connectors.gfw import GAP_EVENT_SCHEMA
    from datalake.warehousing import load_table, query

    df = pd.DataFrame({
        "start": pd.to_datetime(["2022-01-01", "2022-01-05", "2022-02-01"], utc=True),
        "end": pd.to_datetime(["2022-01-02", "2022-01-06", "2022-02-02"], utc=True),
        "vessel": [{"id": "A", "name": "ALPHA", "type": "fishing", "flag": "RUS"},
                   {"id": "A", "name": "ALPHA", "type": "fishing", "flag": "RUS"},
                   {"id": "B", "name": "BRAVO", "type": "cargo", "flag": "PAN"}],
        "gap": [{"duration_hours": "20", "intentional_disabling": True},
                {"duration_hours": "30", "intentional_disabling": True},
                {"duration_hours": "500", "intentional_disabling": False}],
    })
    _, raw = _land(tmp_path, df=df)
    report = clean_json(raw, schema=GAP_EVENT_SCHEMA, lake_root=tmp_path)
    assert sum(report.bad_values.values()) == 0
    wh = tmp_path / "wh.duckdb"
    assert load_table(report.path, schema="ocean", table="gap_events", warehouse_path=wh) == 3
    rows = query(
        "SELECT vessel_id, count(*), sum(duration_hours) FROM ocean.gap_events "
        "WHERE vessel_type = 'fishing' AND intentional_disabling GROUP BY vessel_id", wh)
    assert rows == [("A", 2, 50.0)]


def test_run_gap_pipeline_does_all_three_steps_and_reports(tmp_path):
    from datalake.connectors.gfw import run_gap_pipeline
    from datalake.warehousing import query

    wh = tmp_path / "wh.duckdb"
    client = FakeClient(GAPS_FULL)
    summary = asyncio.run(run_gap_pipeline(
        client, start_date="2022-01-01", end_date="2022-05-01", region=REGION,
        lake_root=tmp_path, warehouse_path=wh))
    assert summary.raw_path.exists() and summary.clean_path.exists()
    assert summary.table == "ocean.gap_events" and summary.rows_loaded == 2
    assert sum(summary.bad_values.values()) == 0
    assert query("SELECT count(*) FROM ocean.gap_events", wh) == [(2,)]


def test_pipeline_with_no_events_loads_zero_rows(tmp_path):
    from datalake.connectors.gfw import run_gap_pipeline

    summary = asyncio.run(run_gap_pipeline(
        FakeClient(pd.DataFrame()), start_date="2022-01-01", end_date="2022-05-01", region=REGION,
        lake_root=tmp_path, warehouse_path=tmp_path / "wh.duckdb"))
    assert summary.rows_loaded == 0


GAPS_FULL = pd.DataFrame({
    "start": pd.to_datetime(["2022-01-01", "2022-01-05"], utc=True),
    "end": pd.to_datetime(["2022-01-02", "2022-01-06"], utc=True),
    "vessel": [{"id": "A", "name": "ALPHA", "type": "fishing", "flag": "RUS"}] * 2,
    "gap": [{"duration_hours": "20", "intentional_disabling": True},
            {"duration_hours": "30", "intentional_disabling": False}],
})


def test_process_raw_rebuilds_from_existing_raw_without_a_client(tmp_path):
    from datalake.connectors.gfw import process_raw, run_gap_pipeline
    from datalake.warehousing import query

    wh = tmp_path / "wh.duckdb"
    first = asyncio.run(run_gap_pipeline(
        FakeClient(GAPS_FULL), start_date="2022-01-01", end_date="2022-05-01", region=REGION,
        lake_root=tmp_path, warehouse_path=wh))
    first.clean_path.unlink()  # pretend the clean file is lost or the cleaning code changed
    wh.unlink()
    again = process_raw(first.raw_path, warehouse_path=wh)  # lake_root found from the path
    assert again.clean_path == first.clean_path and again.clean_path.exists()
    assert again.rows_loaded == 2
    assert query("SELECT count(*) FROM ocean.gap_events", wh) == [(2,)]


def test_process_raw_rejects_files_outside_the_lake_layout_and_missing_files(tmp_path):
    from datalake.connectors.gfw import process_raw

    stray = tmp_path / "stray.json"
    stray.write_text("[]")
    with pytest.raises(ValueError, match="Expected"):
        process_raw(stray)
    with pytest.raises(FileNotFoundError):
        process_raw(tmp_path / "raw" / "gfw" / "gap-events" / "2026-09-30" / "nope.json")


def _events(*rows):
    """Fake GFW gap events from (vessel_id, start_date, hours) tuples."""
    return pd.DataFrame({
        "start": pd.to_datetime([r[1] for r in rows], utc=True),
        "end": pd.to_datetime([r[1] for r in rows], utc=True) + pd.Timedelta(hours=5),
        "vessel": [{"id": r[0], "name": "N", "type": "fishing", "flag": "RUS"} for r in rows],
        "gap": [{"duration_hours": str(r[2]), "intentional_disabling": True} for r in rows],
    })


def test_two_overlapping_fetches_build_up_one_table(tmp_path):
    from datalake.connectors.gfw import run_gap_pipeline
    from datalake.warehousing import query

    wh = tmp_path / "wh.duckdb"

    def run(df, start, end):
        return asyncio.run(run_gap_pipeline(
            FakeClient(df), start_date=start, end_date=end, region=REGION, lake_root=tmp_path, warehouse_path=wh))

    a = run(_events(("A", "2022-01-05", 10), ("B", "2022-02-05", 20)), "2022-01-01", "2022-03-01")
    assert (a.new, a.updated, a.table_total) == (2, 0, 2)
    # Second fetch overlaps: B's Feb event comes again (with a revised duration) plus one new event.
    b = run(_events(("B", "2022-02-05", 25), ("C", "2022-03-10", 30)), "2022-02-01", "2022-04-01")
    assert (b.new, b.updated, b.table_total) == (1, 1, 3)
    rows = query("SELECT vessel_id, duration_hours FROM ocean.gap_events ORDER BY vessel_id", wh)
    assert rows == [("A", 10.0), ("B", 25.0), ("C", 30.0)]  # B holds the newer value
    files = {r[0] for r in query("SELECT DISTINCT _source_file FROM ocean.gap_events", wh)}
    assert len(files) == 2  # rows remember which fetch they came from


# ---- paging: never silently lose events beyond the first page ----

def _many(n):
    return _events(*[(f"V{i}", "2022-01-01", 1) for i in range(n)])


def _fetch(tmp_path, df, *, limit=100, max_pages=50, ignore_offset=False):
    client = FakeClient(df, ignore_offset)
    fetch = asyncio.run(land_gap_events(
        client, start_date="2022-01-01", end_date="2022-05-01", region=REGION,
        limit=limit, max_pages=max_pages, lake_root=tmp_path))
    return client, fetch


def test_pages_through_all_events(tmp_path):
    client, fetch = _fetch(tmp_path, _many(250))
    assert [c["offset"] for c in client.events.calls] == [0, 100, 200]
    assert len(json.loads(fetch.path.read_text())) == 250
    assert (fetch.rows, fetch.pages, fetch.complete, fetch.warning) == (250, 3, True, None)


def test_exactly_one_full_page_is_confirmed_complete_by_an_empty_next_page(tmp_path):
    client, fetch = _fetch(tmp_path, _many(100))
    assert [c["offset"] for c in client.events.calls] == [0, 100]
    assert (fetch.rows, fetch.complete, fetch.warning) == (100, True, None)


def test_page_cap_lands_what_it_has_and_warns(tmp_path):
    _, fetch = _fetch(tmp_path, _many(250), max_pages=2)
    assert len(json.loads(fetch.path.read_text())) == 200
    assert fetch.complete is False and "200" in fetch.warning and "narrower" in fetch.warning


def test_api_that_ignores_offset_is_caught_not_duplicated(tmp_path):
    _, fetch = _fetch(tmp_path, _many(250), ignore_offset=True)
    assert len(json.loads(fetch.path.read_text())) == 100  # page 1 only, no repeated copies
    assert fetch.complete is False and "offset" in fetch.warning


def test_empty_result_is_complete_with_one_page(tmp_path):
    _, fetch = _fetch(tmp_path, pd.DataFrame())
    assert (fetch.rows, fetch.pages, fetch.complete) == (0, 1, True)


def test_pipeline_summary_carries_the_completeness_signal(tmp_path):
    from datalake.connectors.gfw import run_gap_pipeline, summary_lines

    kw = dict(start_date="2022-01-01", end_date="2022-05-01", region=REGION,
              lake_root=tmp_path, warehouse_path=tmp_path / "wh.duckdb")
    ok = asyncio.run(run_gap_pipeline(FakeClient(_many(5)), **kw))
    assert ok.complete and ok.pages == 1 and ok.rows_fetched == 5
    assert not any("WARNING" in line for line in summary_lines(ok))

    capped = asyncio.run(run_gap_pipeline(FakeClient(_many(250)), limit=100, max_pages=1,
                                          **{**kw, "end_date": "2022-06-01"}))
    assert not capped.complete
    assert any(line.startswith("WARNING") for line in summary_lines(capped))
    assert capped.rows_loaded > 0  # incomplete data is still kept: upsert makes a later re-fetch safe
