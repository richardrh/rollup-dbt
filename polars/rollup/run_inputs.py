"""Run-time input preparation helpers.

These helpers sit between CLI/wizard orchestration and the pure pipeline. They
prepare optional inputs that should not be hidden inside argparse handlers, such
as deriving in-memory blending weights from EP-summary long CSVs.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from rollup import config
from rollup.config import VendorName
from rollup.seeds import load_all
from rollup.stages.blending import derive_blending_weights
from rollup.stages.staging import filter_valid_analyses


@dataclass(frozen=True)
class BlendingInput:
    """Blending weights chosen for a run."""

    weights: pl.LazyFrame | None
    message: str


def derive_blending_for_run(cfg: config.Config) -> BlendingInput:
    """Derive in-memory blending weights when every vendor has long EP CSVs.

    The normal run never overwrites ``blending_weights.csv``. When every
    configured vendor has at least one ``*.long.csv`` EP-summary file, the
    weights are derived for this run and copied to ``output/debug`` for audit.
    Partial EP-summary delivery falls back to the reviewed seed.
    """
    csvs_by_vendor = {
        vendor.name: sorted(vendor.ep_summary_dir.glob("*.long.csv"))
        for vendor in cfg.vendors
    }
    missing = [vendor.value for vendor, paths in csvs_by_vendor.items() if not paths]
    if missing:
        return BlendingInput(
            weights=None,
            message=(
                "blending: using blending_weights.csv "
                f"(missing EP-summary long CSVs for: {', '.join(missing)})"
            ),
        )

    seeds_obj = load_all(cfg.seeds_dir)
    analyses = filter_valid_analyses(seeds_obj.analyses, seeds_obj.valid_analyses).collect()
    df = derive_blending_weights(
        csvs_by_vendor[VendorName.RISKLINK],
        csvs_by_vendor[VendorName.VERISK],
        analyses,
        seeds_obj.perils.collect(),
    )
    debug_dir = cfg.output_dir / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    audit_path = debug_dir / "derived_blending_weights.csv"
    df.write_csv(audit_path)
    return BlendingInput(
        weights=df.lazy(),
        message=f"blending: derived {df.height:,} rows from EP summaries for this run; wrote {audit_path}",
    )
