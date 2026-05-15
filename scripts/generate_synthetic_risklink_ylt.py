"""Generate a synthetic RiskLink YLT for demo / smoke-testing.

Writes a CSV (for inspection) and the corresponding parquet (the pipeline input).
Schema matches `RAW_RISKLINK_YLT` — every required pipeline column is present.

Coverage: EU + UK Flood and Winter Storm via the analysis_ids that already
exist in `data/seeds/business/analyses.csv`. ~1000 events per analysis.
Each row is one event in one simulation year. A handful of events recur
across years to give the EP curve some shape.

This is fake data — losses are drawn from a heavy-tailed distribution
just so AEP / OEP curves are non-trivial. Do not interpret the numbers.
"""
from __future__ import annotations
from pathlib import Path

import numpy as np
import polars as pl


REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "data" / "ylt" / "risklink"
CSV_PATH = OUT_DIR / "risklink_ylt_synthetic.csv"
PARQUET_PATH = OUT_DIR / "risklink_ylt_synthetic.parquet"

N_EVENTS_PER_ANALYSIS = 1_000
N_SIMULATIONS = 100_000          # matches Vendor.n_simulations for risklink
RNG_SEED = 42

# (anlsid, modelled_label, peril_family, peril_region, sub_peril_for_description)
ANALYSES: list[tuple[int, str, str, str, str | None]] = [
    (1,  "EU FL HD",   "FL", "EU", None),       # Europe Flood — broad
    (3,  "GB FL HD",   "FL", "UK", None),       # UK Flood
    (4,  "GB WSSS",    "WS", "UK", None),       # UK Winter Storm
    (11, "DE FL",      "FL", "EU", "DE"),       # Europe Flood (DE sub-peril)
    (13, "EUxGB WS",   "WS", "EU", None),       # Europe Winter Storm (excl. GB)
    (29, "BE FL",      "FL", "EU", "BE"),       # Europe Flood (BE sub-peril)
]

# Loss distribution by peril family (mean, scale). Pareto for heavy tail.
LOSS_PARAMS = {
    "FL": dict(shape=1.6, scale=2_000_000),    # flood: heavier tail
    "WS": dict(shape=1.8, scale=1_500_000),    # wind: somewhat lighter
}

def generate() -> pl.DataFrame:
    rng = np.random.default_rng(RNG_SEED)
    frames: list[pl.DataFrame] = []
    next_event_id = 1_000_000        # avoid clashing with anything real

    for anlsid, _label, family, _region, _sub_peril in ANALYSES:
        n = N_EVENTS_PER_ANALYSIS
        params = LOSS_PARAMS[family]
        # Pareto: x_min * (1 - U)^(-1/shape)
        u = rng.random(n)
        loss = params["scale"] * np.power(1.0 - u, -1.0 / params["shape"])
        loss = np.clip(loss, 1.0, 5_000_000_000.0)

        year_ids = rng.integers(1, N_SIMULATIONS + 1, size=n)
        event_ids = np.arange(next_event_id, next_event_id + n, dtype=np.int64)
        next_event_id += n

        p_values = rng.uniform(0.0, 1.0, size=n)
        mean_loss = loss * rng.uniform(0.85, 1.15, size=n)   # ~ around the realised loss
        std_dev = np.sqrt(np.maximum(mean_loss, 0.0))
        exp_value = mean_loss * rng.uniform(50, 200, size=n)

        frames.append(pl.DataFrame({
            "yearid":          year_ids.astype(np.int64),
            "eventid":         event_ids,
            "p_value":         p_values,
            "anlsid":          np.full(n, anlsid, dtype=np.int64),
            "meanloss":        mean_loss,
            "stddev":          std_dev,
            "expvalue":        exp_value,
            "loss":            loss,
        }))

    return pl.concat(frames)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = generate()
    df.write_csv(CSV_PATH)
    df.write_parquet(PARQUET_PATH)

    print(f"wrote {CSV_PATH}  ({CSV_PATH.stat().st_size / 1e3:.1f} KB)")
    print(f"wrote {PARQUET_PATH}  ({PARQUET_PATH.stat().st_size / 1e3:.1f} KB)")
    print(f"rows: {df.height:,}")
    print()
    print("sample:")
    print(df.head(5))
    print()
    print("by analysis:")
    print(df.group_by("anlsid").agg(
        rows=pl.len(),
        unique_years=pl.col("yearid").n_unique(),
        max_loss=pl.col("loss").max(),
    ).sort("anlsid"))


if __name__ == "__main__":
    main()
