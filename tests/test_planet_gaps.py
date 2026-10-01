import random

import pytest

pytest.importorskip("sklearn")

from datalake import planet_gaps
from datalake.connectors import exoplanets, gaia
from datalake.landing import land_raw
from datalake.warehousing import query

STAR_HEADER = "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,phot_g_mean_mag,bp_rp,radial_velocity"
PLANET_HEADER = ",".join(exoplanets.EXOPLANET_COLUMNS)
POS_HEADER = ",".join(exoplanets.HOST_POSITION_COLUMNS)


def _sky(n, seed=1):
    """n fake stars: (source_id, parallax_mas, g, bp_rp, rv). Near stars are bright; colour is random."""
    rnd = random.Random(seed)
    stars = []
    for i in range(n):
        pc = rnd.uniform(2, 30)
        g = 5 + 5 * (pc / 30) * 2 + rnd.uniform(-1, 1)
        stars.append((1000 + i, 1000 / pc, round(g, 3), round(rnd.uniform(0.8, 3.5), 3), rnd.choice(["", "12.5"])))
    return stars


def _build(tmp_path, stars, host_ids, position_hosts=()):
    """Load stars, planets around host_ids (by Gaia number) and around position_hosts (no Gaia number, by position)."""
    wh = tmp_path / "w.duckdb"
    lines = [STAR_HEADER] + [f"{s},{(s % 300) + 0.5},10.0,{p},0.1,0.0,0.0,{g},{c},{rv}" for s, p, g, c, rv in stars]
    gaia.process_raw(land_raw(("\n".join(lines) + "\n").encode(), source="gaia", dataset="nearby-stars", filename="s.csv",
                              lake_root=tmp_path), lake_root=tmp_path, warehouse_path=wh)
    planets = [f"P{s} b,H{s},Gaia DR3 {s},5,200,9,1,2019,Radial Velocity,10,3," for s in host_ids]
    planets += [f"Q{s} b,K{s},,5,200,9,1,2019,Radial Velocity,10,3," for s in position_hosts]
    exoplanets.process_raw(land_raw(("\n".join([PLANET_HEADER] + planets) + "\n").encode(), source="exoplanets",
                                    dataset="planets", filename="p.csv", lake_root=tmp_path),
                           lake_root=tmp_path, warehouse_path=wh)
    if position_hosts:
        pos = [f"Q{s} b,K{s},{(s % 300) + 0.5},10.0" for s in position_hosts]
        exoplanets.process_positions_raw(land_raw(("\n".join([POS_HEADER] + pos) + "\n").encode(), source="exoplanets",
                                                  dataset="host-positions", filename="q.csv", lake_root=tmp_path),
                                         lake_root=tmp_path, warehouse_path=wh)
    return wh


def _nearest(stars, k):
    return [s[0] for s in sorted(stars, key=lambda s: -s[1])[:k]]


def test_the_model_learns_that_near_bright_stars_are_hosts(tmp_path):
    stars = _sky(200)
    hosts = _nearest(stars, 25)
    r = planet_gaps.run_model(_build(tmp_path, stars, hosts))
    assert r.n_stars == 200 and r.n_hosts == 25
    assert r.auc > 0.9  # far better than a coin flip (0.5) on this easy fake sky
    assert r.weights["log10_distance_pc"] < 0  # farther = less likely to be a known host


def test_a_near_bright_star_without_a_planet_tops_the_should_have_list(tmp_path):
    stars = _sky(200)
    near = _nearest(stars, 26)
    gap = near[3]  # one of the nearest stars, but with no planet
    r = planet_gaps.run_model(_build(tmp_path, stars, [s for s in near if s != gap]))
    assert gap in [c.source_id for c in r.candidates[:3]]
    assert all(c.source_id not in near or c.source_id == gap for c in r.candidates)  # hosts are never candidates


def test_planets_matched_by_position_count_as_hosts(tmp_path):
    stars = _sky(200)
    near = _nearest(stars, 25)
    r = planet_gaps.run_model(_build(tmp_path, stars, near[1:], position_hosts=[near[0]]))
    assert r.n_hosts == 25 and near[0] not in [c.source_id for c in r.candidates]


def test_scores_are_out_of_fold_and_repeatable(tmp_path):
    """Same data, same answer (fixed random seed). Every star gets a score from a model that did not see it."""
    stars = _sky(200)
    wh = _build(tmp_path, stars, _nearest(stars, 25))
    a, b = planet_gaps.run_model(wh), planet_gaps.run_model(wh)
    assert [c.score for c in a.candidates] == [c.score for c in b.candidates]
    assert a.folds == 5


def test_stars_without_brightness_are_left_out_and_counted(tmp_path):
    stars = _sky(200)
    stars[0] = (stars[0][0], stars[0][1], "", stars[0][3], "")
    r = planet_gaps.run_model(_build(tmp_path, stars, _nearest(stars[1:], 25)))
    assert r.n_stars == 199 and r.skipped == 1


def test_missing_colour_is_filled_and_flagged_not_dropped(tmp_path):
    stars = _sky(200)
    stars[5] = (stars[5][0], stars[5][1], stars[5][2], "", "")
    r = planet_gaps.run_model(_build(tmp_path, stars, _nearest(stars, 25)))
    assert r.n_stars == 200 and "no_colour" in r.weights


def test_too_few_hosts_is_a_clear_error(tmp_path):
    stars = _sky(50)
    with pytest.raises(ValueError, match="hosts"):
        planet_gaps.run_model(_build(tmp_path, stars, _nearest(stars, 3)))


def test_report_explains_score_and_lists_candidates(tmp_path):
    stars = _sky(200)
    text = planet_gaps.report(planet_gaps.run_model(_build(tmp_path, stars, _nearest(stars, 25))), top=5)
    assert "AUC" in text and "coin flip" in text
    assert "(5 rows)" in text or text.count("\n") > 5
    assert "leads, not discoveries" in text


def test_the_model_changes_nothing(tmp_path):
    stars = _sky(200)
    wh = _build(tmp_path, stars, _nearest(stars, 25))
    before = query("SELECT * FROM astro.nearby_stars ORDER BY source_id", wh)
    planet_gaps.run_model(wh)
    assert query("SELECT * FROM astro.nearby_stars ORDER BY source_id", wh) == before


def test_every_star_is_scored_by_a_model_that_never_saw_it(tmp_path, monkeypatch):
    """Spy on the cross-validation step: it must be what produces the scores, with 5 folds."""
    import sklearn.model_selection as ms
    calls = []
    real = ms.cross_val_predict

    def spy(model, X, y, cv=None, method=None):
        out = real(model, X, y, cv=cv, method=method)
        calls.append((cv.get_n_splits(), out[:, 1].copy(), y.copy()))
        return out

    monkeypatch.setattr(ms, "cross_val_predict", spy)
    stars = _sky(200)
    r = planet_gaps.run_model(_build(tmp_path, stars, _nearest(stars, 25)))
    assert len(calls) == 1 and calls[0][0] == 5
    _, oof, y = calls[0]
    assert sorted(c.score for c in r.candidates) == sorted(oof[y == 0].tolist())


def test_features_use_log_distance_and_flag_missing_colour():
    rows = [(1, 100.0, 9.0, 1.2, 4.0, True), (2, 10.0, 5.0, None, 5.0, False), (3, 1.0, 1.0, 2.0, 6.0, True)]
    X = planet_gaps.features(rows)
    assert X[:, 0].tolist() == [2.0, 1.0, 0.0]  # log10 of 100, 10 and 1 parsecs
    assert X[1, 2] == 1.6 and X[1, 4] == 1.0 and X[0, 4] == 0.0  # median colour filled in, and flagged
    assert X[:, 5].tolist() == [1.0, 0.0, 1.0]
