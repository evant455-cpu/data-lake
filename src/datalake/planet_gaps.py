"""Which nearby stars "should" have a known planet but do not? (our first machine-learning model)

The model is a CLASSIFIER: it learns, from stars that do and do not have known planets, what a typical
planet host looks like in our data, then gives every star a probability (a score from 0 to 1).

  Features (what it looks at): distance (as log10 parsecs), brightness (G), colour (bp_rp),
    true brightness (absolute G), whether colour is missing, whether Gaia has a radial velocity.
  Labels (what it learns to predict): does the star host a known planet? Taken from
    datalake.planet_hosts, so hosts found by sky position count too.
  Model: logistic regression (a weighted sum of the features, squeezed into a probability). Simple and
    easy to inspect; a stronger model (gradient boosting) scored no better on our data.

Honest scoring: CROSS-VALIDATION. The stars are split into 5 groups; a model trained on 4 groups scores the 5th,
in turn. So every star's score comes from a model that never saw it. Without this the model would simply
remember which stars have planets and nothing would ever look "missing".

What a high score on a star with no known planet means: it looks like the stars where planets HAVE been found
(mostly near, bright, well-observed). It is a lead for where a search may have missed something, or where a
search was never done. It is not a prediction that a planet exists. Known planets are themselves biased
(easy-to-find planets around easy-to-study stars), and the model learns that bias too.

Needs scikit-learn (pip install scikit-learn). Read-only: nothing in the warehouse changes.
Run:  python -m datalake.planet_gaps [--top N]
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from datalake.planet_hosts import find_host_matches
from datalake.warehousing import DEFAULT_WAREHOUSE, query

FEATURES = ("log10_distance_pc", "g_mag", "bp_rp", "abs_g", "no_colour", "has_rv")
FOLDS = 5
SEED = 0  # fixed, so the same data always gives the same scores


@dataclass
class Candidate:
    source_id: int
    pc: float
    g_mag: float
    bp_rp: float | None
    abs_g: float
    has_rv: bool
    score: float  # out-of-fold probability of being a known host


@dataclass
class ModelResult:
    n_stars: int
    n_hosts: int
    skipped: int  # stars left out (no brightness or no positive parallax)
    folds: int
    auc: float
    avg_precision: float
    base_rate: float  # share of hosts: what "average precision" would be for random guessing
    weights: dict = field(default_factory=dict)  # feature -> weight on the standardised scale (sign = direction)
    candidates: list[Candidate] = field(default_factory=list)  # stars WITHOUT a known planet, highest score first


def _load(warehouse_path: Path):
    rows = query(
        """SELECT source_id, 1000.0 / parallax AS pc, phot_g_mean_mag, bp_rp,
                  phot_g_mean_mag + 5 * log10(parallax) - 10 AS abs_g, radial_velocity IS NOT NULL AS has_rv
           FROM astro.nearby_stars ORDER BY source_id""",
        warehouse_path,
    )
    usable = [r for r in rows if r[2] is not None and r[1] is not None and r[1] > 0]
    return usable, len(rows) - len(usable)


def features(rows) -> "np.ndarray":
    """One row of numbers per star, in the order of FEATURES. Missing colour -> the typical colour, plus a flag."""
    import numpy as np

    colours = [r[3] for r in rows if r[3] is not None]
    fill = float(np.median(colours)) if colours else 0.0
    return np.array([[np.log10(r[1]), r[2], r[3] if r[3] is not None else fill, r[4],
                      float(r[3] is None), float(r[5])] for r in rows])


def run_model(warehouse_path: Path = DEFAULT_WAREHOUSE) -> ModelResult:
    """Train, score every star out-of-fold, and rank the stars without a known planet. Read-only."""
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import average_precision_score, roc_auc_score
    from sklearn.model_selection import StratifiedKFold, cross_val_predict
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    rows, skipped = _load(warehouse_path)
    hosts = {m.source_id for m in find_host_matches(warehouse_path)}
    y = np.array([r[0] in hosts for r in rows], dtype=int)
    n_hosts = int(y.sum())
    if n_hosts < FOLDS or len(y) - n_hosts < FOLDS:
        raise ValueError(f"Need at least {FOLDS} hosts and {FOLDS} non-hosts to cross-validate; have {n_hosts} hosts "
                         f"among {len(y)} stars.")

    X = features(rows)

    def model():
        return make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))

    cv = StratifiedKFold(n_splits=FOLDS, shuffle=True, random_state=SEED)
    scores = cross_val_predict(model(), X, y, cv=cv, method="predict_proba")[:, 1]
    full = model().fit(X, y)  # fitted on everything, only to show what the model leans on
    weights = dict(zip(FEATURES, (float(w) for w in full[-1].coef_[0])))

    candidates = [
        Candidate(r[0], r[1], r[2], r[3], r[4], bool(r[5]), float(s))
        for r, s, label in zip(rows, scores, y) if not label
    ]
    candidates.sort(key=lambda c: (-c.score, c.source_id))
    return ModelResult(
        n_stars=len(rows), n_hosts=n_hosts, skipped=skipped, folds=FOLDS,
        auc=float(roc_auc_score(y, scores)), avg_precision=float(average_precision_score(y, scores)),
        base_rate=n_hosts / len(rows), weights=weights, candidates=candidates,
    )


def report(r: ModelResult, top: int = 15) -> str:
    if top < 1:
        raise ValueError("top must be at least 1")
    lines = [
        f"{r.n_stars} stars ({r.skipped} left out: no brightness), {r.n_hosts} with a known planet "
        f"({100 * r.base_rate:.1f}%). Scored with {r.folds}-fold cross-validation (each star by a model that never saw it).",
        f"AUC {r.auc:.3f}  (0.5 = coin flip, 1.0 = perfect: how often a real host is ranked above a non-host)",
        f"Average precision {r.avg_precision:.3f}  (random guessing would give {r.base_rate:.3f})",
        "",
        "What the model leans on (weights on a common scale; + = more likely a known host, - = less):",
    ]
    for name, w in sorted(r.weights.items(), key=lambda kv: -abs(kv[1])):
        lines.append(f"  {name:18s} {w:+.2f}")
    shown = r.candidates[:top]
    lines += ["", f"Stars with no known planet that look most like the hosts (top {len(shown)}):",
              f"  {'source_id':>20s}  {'pc':>5s}  {'G':>5s}  {'bp_rp':>5s}  {'abs_G':>5s}  {'RV':>3s}  score"]
    for c in shown:
        colour = f"{c.bp_rp:5.2f}" if c.bp_rp is not None else "    -"
        lines.append(f"  {c.source_id:>20d}  {c.pc:5.1f}  {c.g_mag:5.1f}  {colour}  {c.abs_g:5.1f}  "
                     f"{'yes' if c.has_rv else 'no':>3s}  {c.score:.2f}")
    lines += [f"({len(shown)} rows)", "",
              "These are leads, not discoveries: the stars look like where planets have been found so far. Check a "
              "name in SIMBAD before reading more into it (a planet may be listed on a companion star)."]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    import argparse

    import duckdb

    p = argparse.ArgumentParser(description="Which nearby stars look like planet hosts but have no known planet?")
    p.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    p.add_argument("--top", type=int, default=15, help="how many candidates to list")
    a = p.parse_args(argv)
    wh = Path(a.warehouse)
    if not wh.exists():
        raise SystemExit(f"No warehouse at {wh}. Run a connector first.")
    try:
        import sklearn  # noqa: F401
    except ImportError:
        raise SystemExit("This needs scikit-learn. Install it once with: pip install scikit-learn")
    try:
        print(report(run_model(wh), a.top))
    except duckdb.CatalogException:
        raise SystemExit("Needs astro.nearby_stars and astro.exoplanets. Run the gaia and exoplanets connectors first.")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
