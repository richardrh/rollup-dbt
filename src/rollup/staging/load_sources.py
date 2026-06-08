from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from rollup.columns import Col, RawCol
from rollup.schemas import require_columns


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
    verisk_ylt = _scan_parquet_folder(data_root / "ylt" / "verisk")
    risklink_ylt = _scan_parquet_folder(data_root / "ylt" / "risklink")
    require_columns(verisk_ylt, VERISK_YLT_SCHEMA, check_dtypes=False)
    require_columns(risklink_ylt, RISKLINK_YLT_SCHEMA, check_dtypes=False)

    ep_summaries = _read_ep_summaries(data_root)
    require_columns(ep_summaries, EP_SUMMARY_SCHEMA, check_dtypes=False)

    lobs = _read_seed_csv(data_root, "lobs.csv")
    require_columns(lobs, LOBS_SCHEMA, check_dtypes=False)
    perils = _read_seed_csv(data_root, "perils.csv")
    require_columns(perils, PERILS_SCHEMA, check_dtypes=False)

    return StagingFrames(
        verisk_ylt=verisk_ylt,
        risklink_ylt=risklink_ylt,
        ep_summaries=ep_summaries,
        lobs=lobs,
        perils=perils,
        blending=_read_optional_seed(data_root, ("blending_factors.csv", "blending_weights.csv")),
        fx_rates=_read_optional_seed(data_root, ("fx_rates.csv",)),
        forecast_factors=_read_optional_seed(data_root, ("forecast_factors.csv",)),
        euws_factors=_read_optional_seed(data_root, ("euws_rate_factors.csv",)),
    )


def _scan_parquet_folder(folder: Path) -> pl.LazyFrame:
    paths = sorted(folder.glob("*.parquet"))
    if not paths:
        raise FileNotFoundError(f"no parquet files found in {folder}")
    return pl.scan_parquet(str(folder / "*.parquet"))


def _read_ep_summaries(data_root: Path) -> pl.DataFrame:
    paths = sorted((data_root / "ep_summaries").rglob("*.long.csv"))
    if not paths:
        raise FileNotFoundError(f"no EP summary .long.csv files found in {data_root / 'ep_summaries'}")
    return pl.concat([pl.read_csv(path) for path in paths], how="diagonal_relaxed")


def _read_seed_csv(data_root: Path, filename: str) -> pl.DataFrame:
    paths = sorted((data_root / "seeds").rglob(filename), key=lambda path: (len(path.parts), str(path)))
    if not paths:
        raise FileNotFoundError(f"seed file not found: {filename}")
    return pl.read_csv(paths[0])


def _read_optional_seed(data_root: Path, filenames: tuple[str, ...]) -> pl.DataFrame:
    for filename in filenames:
        try:
            return _read_seed_csv(data_root, filename)
        except FileNotFoundError:
            continue
    return pl.DataFrame()
