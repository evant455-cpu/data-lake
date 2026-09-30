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
