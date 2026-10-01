import hashlib
import json
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from datalake.connectors import gaia, gfw
from datalake.landing import land_raw
from datalake.rebuild import compare_warehouses, find_raw_files, main, rebuild
from datalake.warehousing import query

T0 = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
GAP_REGION = "public-eez-areas-5690"


def _gap_json(*rows):
    """Raw GFW gap events from (vessel_id, start_date, hours) tuples."""
    return json.dumps([
        {"start": f"{d}T00:00:00Z", "end": f"{d}T05:00:00Z",
         "vessel": {"id": v, "name": "N", "type": "fishing", "flag": "RUS"},
         "gap": {"duration_hours": str(h), "intentional_disabling": True}}
        for v, d, h in rows
    ]).encode()


def _stars_csv(*rows):
    header = "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,phot_g_mean_mag,bp_rp,radial_velocity"
    return ("\n".join([header] + [f"{s},1.0,2.0,{p},0.1,3.0,4.0,9.5,1.2," for s, p in rows]) + "\n").encode()


def _land_gaps(lake, name, data, minutes=0):
    return land_raw(data, source="gfw", dataset="gap-events", filename=name, lake_root=lake,
                    now=T0 + timedelta(minutes=minutes))


def _land_stars(lake, name, data, minutes=0):
    return land_raw(data, source="gaia", dataset="nearby-stars", filename=name, lake_root=lake,
                    now=T0 + timedelta(minutes=minutes))


def _live_history(tmp_path):
    """Build a lake and warehouse the normal incremental way. Returns (lake, warehouse)."""
    lake, wh = tmp_path / "lake", tmp_path / "live.duckdb"
    # Alphabetical order is the REVERSE of fetch order on purpose: the later fetch must still win.
    first = _land_gaps(lake, "z-first.json", _gap_json(("A", "2022-01-05", 10), ("B", "2022-02-05", 20)), 0)
    second = _land_gaps(lake, "a-second.json", _gap_json(("B", "2022-02-05", 25), ("C", "2022-03-10", 30)), 60)
    stars = _land_stars(lake, "band.csv", _stars_csv((1, 768.5), (2, 546.9)), 90)
    gfw.process_raw(first, lake_root=lake, warehouse_path=wh)
    gfw.process_raw(second, lake_root=lake, warehouse_path=wh)
    gaia.process_raw(stars, lake_root=lake, warehouse_path=wh)
    return lake, wh


def _hashes(lake):
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in (lake / "raw").rglob("*") if p.is_file()}


def test_rebuild_from_raw_alone_matches_the_live_warehouse(tmp_path):
    lake, live = _live_history(tmp_path)
    import shutil
    shutil.rmtree(lake / "clean")  # clean is gone: only raw remains
    new = tmp_path / "rebuilt.duckdb"
    result = rebuild(lake, new)
    assert result.files == 3 and result.tables == {"ocean.gap_events": 3, "astro.nearby_stars": 2}
    diffs = compare_warehouses(live, new, list(result.tables))
    assert all(d.same for d in diffs), diffs
    # B holds the value from the LATER fetch although its file name sorts first.
    assert query("SELECT duration_hours FROM ocean.gap_events WHERE vessel_id = 'B'", new) == [(25.0,)]
    assert list((lake / "clean").rglob("*.parquet"))  # clean files re-created


def test_files_replay_by_fetch_time_not_by_name(tmp_path):
    lake, _ = _live_history(tmp_path)
    names = [p.name for p in find_raw_files(lake)]
    assert names == ["z-first.json", "a-second.json", "band.csv"]


def test_raw_is_never_touched(tmp_path):
    lake, _ = _live_history(tmp_path)
    before = _hashes(lake)
    rebuild(lake, tmp_path / "rebuilt.duckdb")
    assert _hashes(lake) == before


def test_sidecars_and_gitkeep_are_not_treated_as_data(tmp_path):
    lake, _ = _live_history(tmp_path)
    (lake / "raw" / ".gitkeep").write_text("")
    assert all(not p.name.endswith(".meta.json") and p.name != ".gitkeep" for p in find_raw_files(lake))


def test_unknown_datasets_are_skipped_with_a_note_not_a_crash(tmp_path):
    lake, _ = _live_history(tmp_path)
    land_raw(b"x", source="mystery", dataset="things", filename="m.bin", lake_root=lake, now=T0)
    result = rebuild(lake, tmp_path / "rebuilt.duckdb")
    assert result.files == 3 and len(result.skipped) == 1 and "mystery" in result.skipped[0]


def test_a_file_without_a_sidecar_is_still_rebuilt(tmp_path):
    lake, _ = _live_history(tmp_path)
    for side in (lake / "raw").rglob("*.meta.json"):
        side.unlink()
    result = rebuild(lake, tmp_path / "rebuilt.duckdb")
    assert result.files == 3 and result.tables["astro.nearby_stars"] == 2


def test_an_existing_warehouse_is_never_overwritten_by_surprise(tmp_path):
    lake, live = _live_history(tmp_path)
    before = live.read_bytes()
    with pytest.raises(FileExistsError, match="--replace"):
        rebuild(lake, live)
    assert live.read_bytes() == before


def test_replace_swaps_in_the_new_warehouse(tmp_path):
    lake, live = _live_history(tmp_path)
    con = duckdb.connect(str(live))
    con.execute("DELETE FROM ocean.gap_events")  # damage the live copy
    con.close()
    rebuild(lake, live, replace=True)
    assert query("SELECT count(*) FROM ocean.gap_events", live) == [(3,)]
    assert not list(tmp_path.glob("*.rebuilding*"))  # temp file cleaned up


def test_a_failed_rebuild_leaves_the_existing_warehouse_alone(tmp_path):
    lake, live = _live_history(tmp_path)
    before = live.read_bytes()
    _land_gaps(lake, "broken.json", b"this is not json", minutes=200)
    with pytest.raises(RuntimeError, match="broken.json"):
        rebuild(lake, live, replace=True)
    assert live.read_bytes() == before
    assert not list(tmp_path.glob("*.rebuilding*"))


def test_empty_lake_does_nothing(tmp_path):
    target = tmp_path / "w.duckdb"
    result = rebuild(tmp_path / "lake", target)
    assert result.files == 0 and not target.exists()


def test_compare_spots_a_difference(tmp_path):
    lake, live = _live_history(tmp_path)
    other = tmp_path / "other.duckdb"
    rebuild(lake, other)
    con = duckdb.connect(str(other))
    con.execute("UPDATE ocean.gap_events SET duration_hours = 999 WHERE vessel_id = 'A'")
    con.close()
    diff = {d.table: d for d in compare_warehouses(live, other, ["ocean.gap_events", "astro.nearby_stars"])}
    assert not diff["ocean.gap_events"].same and diff["ocean.gap_events"].only_in_a == 1
    assert diff["astro.nearby_stars"].same


def test_command_line_reports_and_exit_code(tmp_path, capsys):
    lake, live = _live_history(tmp_path)
    args = ["--lake", str(lake), "--compare", str(live), "--warehouse"]
    assert main(args + [str(tmp_path / "r1.duckdb")]) == 0
    out = capsys.readouterr().out
    assert "ocean.gap_events" in out and "identical" in out
    con = duckdb.connect(str(live))
    con.execute("DELETE FROM astro.nearby_stars")  # now the live copy disagrees with what raw says
    con.close()
    assert main(args + [str(tmp_path / "r2.duckdb")]) == 1
    assert "DIFFERENT" in capsys.readouterr().out
