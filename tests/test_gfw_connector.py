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
    def __init__(self, df):
        self.df, self.calls = df, []

    async def get_all_events(self, **kwargs):
        self.calls.append(kwargs)
        return FakeResult(self.df)


class FakeClient:
    def __init__(self, df):
        self.events = FakeEvents(df)


GAPS = pd.DataFrame({
    "start": pd.to_datetime(["2022-01-01T00:00:00Z"], utc=True),
    "vessel": [{"id": "A", "flag": "RUS"}],
    "gap": [{"duration_hours": "20", "intentional_disabling": True}],
})


def _land(tmp_path, df=GAPS):
    client = FakeClient(df)
    path = asyncio.run(land_gap_events(
        client, start_date="2022-01-01", end_date="2022-05-01", region=REGION, lake_root=tmp_path))
    return client, path


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
