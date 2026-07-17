"""Discover canonical vendor EP summary CSVs as one lazy Polars frame."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.columns import Col


def load(data_root: Path | str = "data") -> pl.LazyFrame:
    """Load canonical vendor long CSVs lazily after per-file schema checks."""
    ep_summary_root = Path(data_root) / "ep_summaries"
    canonical_columns = [
        Col.vendor,
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.ep_type,
        Col.return_period,
        Col.loss,
    ]
    vendors = ("verisk", "risklink")
    ep_summary_paths = sorted(ep_summary_root.rglob("*.long.csv"))
    frames: list[pl.LazyFrame] = []
    paths_by_vendor: dict[str, list[Path]] = {vendor: [] for vendor in vendors}
    for path in ep_summary_paths:
        relative_path = path.relative_to(ep_summary_root)
        if len(relative_path.parts) < 2:
            raise ValueError(
                f"EP summary file is not under a recognised vendor folder: {path}"
            )
        vendor = relative_path.parts[0]
        if vendor not in vendors:
            raise ValueError(
                f"EP summary file is not under a recognised vendor folder: {path}"
            )
        paths_by_vendor[vendor].append(path)
    for vendor, vendor_paths in paths_by_vendor.items():
        if not vendor_paths:
            raise FileNotFoundError(
                f"No EP summary long CSV files found for {vendor} in {ep_summary_root / vendor}."
            )
        for path in vendor_paths:
            frame = pl.scan_csv(path)
            schema = frame.collect_schema()
            missing = [column for column in canonical_columns if column not in schema]
            if missing:
                raise ValueError(
                    f"{path} is missing canonical EP summary columns: {missing}"
                )
            frames.append(
                frame.with_columns(
                    pl.lit(vendor).alias(Col.vendor),
                    pl.col(Col.analysis_id).cast(pl.String),
                ).select(canonical_columns)
            )
    return pl.concat(frames, how="vertical")
