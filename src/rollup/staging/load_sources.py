from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pandera.polars as pa

from rollup.columns import Col, RawCol


VERISK_YLT_SCHEMA = pa.DataFrameSchema(
    {
        RawCol.Analysis: pa.Column(pl.String, nullable=True),
        RawCol.ExposureAttribute: pa.Column(pl.String, nullable=True),
        RawCol.CatalogTypeCode: pa.Column(pl.String, nullable=True),
        RawCol.EventID: pa.Column(pl.Int64, nullable=True),
        RawCol.ModelCode: pa.Column(pl.Int64, nullable=True),
        RawCol.YearID: pa.Column(pl.Int64, nullable=True),
        RawCol.GroundUpLoss: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)

RISKLINK_YLT_SCHEMA = pa.DataFrameSchema(
    {
        RawCol.anlsid: pa.Column(pl.Int64, nullable=True),
        RawCol.yearid: pa.Column(pl.Int64, nullable=True),
        RawCol.eventid: pa.Column(pl.Int64, nullable=True),
        RawCol.loss: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)

EP_SUMMARY_SCHEMA = pa.DataFrameSchema(
    {
        Col.vendor: pa.Column(pl.String, nullable=True),
        Col.analysis_id: pa.Column(pl.String, nullable=True),
        Col.modelled_lob: pa.Column(pl.String, nullable=True),
        Col.modelled_peril: pa.Column(pl.String, nullable=True),
        Col.ep_type: pa.Column(pl.String, nullable=True),
        Col.return_period: pa.Column(pl.Int64, nullable=True),
        Col.loss: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)

LOBS_SCHEMA = pa.DataFrameSchema(
    {
        Col.modelled_lob: pa.Column(pl.String, nullable=True),
        Col.rollup_lob: pa.Column(pl.String, nullable=True),
        Col.class_: pa.Column(pl.String, nullable=True),
        Col.office: pa.Column(pl.String, nullable=True),
        Col.currency: pa.Column(pl.String, nullable=True),
    },
    strict=False,
)

PERILS_SCHEMA = pa.DataFrameSchema(
    {
        Col.modelled_peril: pa.Column(pl.String, nullable=True),
        Col.rollup_peril: pa.Column(pl.String, nullable=True),
        Col.region_peril_id: pa.Column(pl.Int64, nullable=True),
        Col.selection_priority: pa.Column(pl.Int64, nullable=True),
        Col.is_dialsup: pa.Column(pl.Int64, nullable=True),
    },
    strict=False,
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
    VERISK_YLT_SCHEMA.validate(verisk_ylt)
    RISKLINK_YLT_SCHEMA.validate(risklink_ylt)

    ep_summaries = read_ep_summaries(data_root)
    EP_SUMMARY_SCHEMA.validate(ep_summaries)

    lobs = read_seed_csv(data_root, "lobs.csv")
    LOBS_SCHEMA.validate(lobs)
    perils = read_seed_csv(data_root, "perils.csv")
    PERILS_SCHEMA.validate(perils)

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
