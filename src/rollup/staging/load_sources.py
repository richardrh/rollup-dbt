from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from rollup.columns import Col, RawCol


VERISK_YLT_SCHEMA = pl.Schema(
    {
        RawCol.Analysis: pl.String,
        RawCol.ExposureAttribute: pl.String,
        RawCol.CatalogTypeCode: pl.String,
        RawCol.EventID: pl.Int64,
        RawCol.ModelCode: pl.Int64,
        RawCol.YearID: pl.Int64,
        RawCol.GroundUpLoss: pl.Float64,
    }
)

RISKLINK_YLT_SCHEMA = pl.Schema(
    {
        RawCol.anlsid: pl.Int64,
        RawCol.yearid: pl.Int64,
        RawCol.eventid: pl.Int64,
        RawCol.loss: pl.Float64,
    }
)

EP_SUMMARY_SCHEMA = pl.Schema(
    {
        Col.vendor: pl.String,
        Col.analysis_id: pl.String,
        Col.modelled_lob: pl.String,
        Col.modelled_peril: pl.String,
        Col.ep_type: pl.String,
        Col.return_period: pl.Int64,
        Col.loss: pl.Float64,
    }
)

LOBS_SCHEMA = pl.Schema(
    {
        Col.modelled_lob: pl.String,
        Col.rollup_lob: pl.String,
        Col.class_: pl.String,
        Col.office: pl.String,
        Col.currency: pl.String,
    }
)

PERILS_SCHEMA = pl.Schema(
    {
        Col.modelled_peril: pl.String,
        Col.rollup_peril: pl.String,
        Col.region_peril_id: pl.Int64,
        Col.selection_priority: pl.Int64,
        Col.is_dialsup: pl.Int64,
    }
)


@dataclass(frozen=True)
class StagingFrames:
    verisk_ylt: pl.LazyFrame
    risklink_ylt: pl.LazyFrame
    ep_summaries: pl.DataFrame
    lobs: pl.DataFrame
    perils: pl.DataFrame
    blending: pl.DataFrame
    fx_rates: pl.DataFrame
    forecast_factors: pl.DataFrame
    euws_factors: pl.DataFrame


def load_sources(data_root: str | Path) -> StagingFrames:
    data_root = Path(data_root)
    verisk_ylt = scan_parquet_folder(data_root / "ylt" / "verisk")
    risklink_ylt = scan_parquet_folder(data_root / "ylt" / "risklink")
    actual = verisk_ylt.collect_schema()
    missing = [str(name) for name in VERISK_YLT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"load_sources verisk_ylt missing columns: {missing}")
    actual = risklink_ylt.collect_schema()
    missing = [str(name) for name in RISKLINK_YLT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"load_sources risklink_ylt missing columns: {missing}")

    ep_summaries = read_ep_summaries(data_root)
    actual = ep_summaries.schema
    missing = [str(name) for name in EP_SUMMARY_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"load_sources ep_summaries missing columns: {missing}")

    lobs = read_seed_csv(data_root, "lobs.csv")
    actual = lobs.schema
    missing = [str(name) for name in LOBS_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"load_sources lobs missing columns: {missing}")
    perils = read_seed_csv(data_root, "perils.csv")
    actual = perils.schema
    missing = [str(name) for name in PERILS_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"load_sources perils missing columns: {missing}")

    return StagingFrames(
        verisk_ylt=verisk_ylt,
        risklink_ylt=risklink_ylt,
        ep_summaries=ep_summaries,
        lobs=lobs,
        perils=perils,
        blending=read_optional_seed(data_root, ("blending_factors.csv", "blending_weights.csv")),
        fx_rates=read_optional_seed(data_root, ("fx_rates.csv",)),
        forecast_factors=read_optional_seed(data_root, ("forecast_factors.csv",)),
        euws_factors=read_optional_seed(data_root, ("euws_rate_factors.csv",)),
    )


def scan_parquet_folder(folder: Path) -> pl.LazyFrame:
    paths = sorted(folder.glob("*.parquet"))
    if not paths:
        raise FileNotFoundError(f"no parquet files found in {folder}")
    return pl.scan_parquet(str(folder / "*.parquet"))


def read_ep_summaries(data_root: Path) -> pl.DataFrame:
    paths = sorted((data_root / "ep_summaries").rglob("*.long.csv"))
    if not paths:
        raise FileNotFoundError(f"no EP summary .long.csv files found in {data_root / 'ep_summaries'}")
    return pl.concat([pl.read_csv(path) for path in paths], how="diagonal_relaxed")


def read_seed_csv(data_root: Path, filename: str) -> pl.DataFrame:
    paths = sorted((data_root / "seeds").rglob(filename), key=lambda path: (len(path.parts), str(path)))
    if not paths:
        raise FileNotFoundError(f"seed file not found: {filename}")
    return pl.read_csv(paths[0])


def read_optional_seed(data_root: Path, filenames: tuple[str, ...]) -> pl.DataFrame:
    for filename in filenames:
        try:
            return read_seed_csv(data_root, filename)
        except FileNotFoundError:
            continue
    return pl.DataFrame()
