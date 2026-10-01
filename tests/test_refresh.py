import pytest

from datalake import refresh
from datalake.connectors.exoplanets import EXOPLANET_COLUMNS, HOST_POSITION_COLUMNS
from datalake.warehousing import query

PLANET_HEADER = ",".join(EXOPLANET_COLUMNS)
POS_HEADER = ",".join(HOST_POSITION_COLUMNS)


def _planet(name, orbper="11.2"):
    return f"{name},Star A,Gaia DR3 4472832130942575872,1.30,769.0,9.5,1,2016,Radial Velocity,{orbper},1.3,1.1"


class FakeArchive:
    """Answers count questions, the positions question and the planets question."""

    def __init__(self, planets=("P b", "P c"), orbper="11.2", boom=None):
        self.planets, self.orbper, self.boom, self.calls = planets, orbper, boom, 0

    def __call__(self, adql):
        self.calls += 1
        if self.boom:
            raise self.boom
        n = len(self.planets)
        if "count(*)" in adql.lower():
            return f"count\n{n}\n".encode()
        if adql.startswith("SELECT " + ", ".join(HOST_POSITION_COLUMNS) + " "):
            rows = [f"{p},Star A,10.5,-20.25" for p in self.planets]
            return ("\n".join([POS_HEADER, *rows]) + "\n").encode()
        rows = [_planet(p, self.orbper) for p in self.planets]
        return ("\n".join([PLANET_HEADER, *rows]) + "\n").encode()


def _run(tmp_path, fetch):
    return refresh.run_refresh(fetch, lake_root=tmp_path / "lake", warehouse_path=tmp_path / "wh.duckdb")


def test_clean_run_loads_both_tables_and_exits_zero(tmp_path):
    steps = _run(tmp_path, FakeArchive())
    assert [s.name for s in steps] == ["planets", "host-positions", "quality"]
    assert all(s.ok for s in steps) and refresh.exit_code(steps) == 0
    wh = tmp_path / "wh.duckdb"
    assert query("SELECT count(*) FROM astro.exoplanets", wh)[0][0] == 2
    assert query("SELECT count(*) FROM astro.exoplanet_positions", wh)[0][0] == 2


def test_second_run_same_day_is_skipped_not_failed_and_still_checks_quality(tmp_path):
    _run(tmp_path, FakeArchive())
    arch = FakeArchive()
    steps = _run(tmp_path, arch)
    assert refresh.exit_code(steps) == 0 and all(s.ok for s in steps)
    assert "skipped" in steps[0].lines[0] and "skipped" in steps[1].lines[0]
    assert steps[2].name == "quality" and any("non_positive_orbit" in l for l in steps[2].lines)


def test_network_failure_exits_two_but_the_other_steps_still_run(tmp_path):
    steps = _run(tmp_path, FakeArchive(boom=OSError("network down")))
    assert refresh.exit_code(steps) == 2
    assert not steps[0].ok and "network down" in steps[0].lines[0]
    assert not steps[1].ok
    assert steps[2].name == "quality" and not steps[2].ok  # no warehouse was ever made


def test_bad_values_make_a_quality_error_exit_one(tmp_path):
    steps = _run(tmp_path, FakeArchive(orbper="-5"))
    assert steps[0].ok and steps[1].ok
    assert not steps[2].ok and refresh.exit_code(steps) == 1


def test_exit_code_two_beats_one():
    mk = lambda c: refresh.StepResult("x", c == 0, [], c)
    assert refresh.exit_code([mk(1), mk(2)]) == 2
    assert refresh.exit_code([mk(0), mk(1)]) == 1
    assert refresh.exit_code([mk(0), mk(0)]) == 0


def test_gfw_problems_cannot_fail_an_astronomy_refresh():
    tables = {c.table for c in refresh.astro_checks()}
    assert tables and not any("ocean" in t for t in tables)
