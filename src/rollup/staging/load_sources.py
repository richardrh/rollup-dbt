from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pandera.polars as pa

from rollup.columns import Col, RawCol


def required(dtype: pl.DataType) -> pa.Column:
    return pa.Column(dtype, nullable=False, coerce=True)


def strict_schema(columns: dict[str, pa.Column]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(columns, strict=True)


VERISK_YLT_SCHEMA = pa.DataFrameSchema(
    {
        RawCol.Analysis: pa.Column(pl.String, nullable=False),
        RawCol.ExposureAttribute: pa.Column(pl.String, nullable=False),
        RawCol.CatalogTypeCode: pa.Column(pl.String, nullable=False),
        RawCol.EventID: pa.Column(pl.Int64, nullable=False),
        RawCol.ModelCode: pa.Column(pl.Int64, nullable=False),
        RawCol.YearID: pa.Column(pl.Int64, nullable=False),
        RawCol.GroundUpLoss: pa.Column(pl.Float64, nullable=False),
    },
    strict=False,
)
VERISK_YLT_REQUIRED_COLUMNS = (
    RawCol.Analysis,
    RawCol.ExposureAttribute,
    RawCol.CatalogTypeCode,
    RawCol.EventID,
    RawCol.ModelCode,
    RawCol.YearID,
    RawCol.GroundUpLoss,
)

RISKLINK_YLT_SCHEMA = pa.DataFrameSchema(
    {
        RawCol.anlsid: pa.Column(pl.Int64, nullable=False),
        RawCol.yearid: pa.Column(pl.Int64, nullable=False),
        RawCol.eventid: pa.Column(pl.Int64, nullable=False),
        RawCol.loss: pa.Column(pl.Float64, nullable=False),
    },
    strict=False,
)
RISKLINK_YLT_REQUIRED_COLUMNS = (
    RawCol.anlsid,
    RawCol.yearid,
    RawCol.eventid,
    RawCol.loss,
)

EP_SUMMARY_SCHEMA = strict_schema(
    {
        Col.vendor: required(pl.String),
        Col.analysis_id: required(pl.String),
        Col.modelled_lob: required(pl.String),
        Col.modelled_peril: required(pl.String),
        Col.ep_type: required(pl.String),
        Col.return_period: required(pl.Int64),
        Col.loss: required(pl.Float64),
    }
)

LOBS_SCHEMA = strict_schema(
    {
        "lob_id": required(pl.Int64),
        Col.modelled_lob: required(pl.String),
        Col.rollup_lob: required(pl.String),
        "lob_type": required(pl.String),
        Col.cds_cat_class_name: required(pl.String),
        Col.office: required(pl.String),
        Col.class_: required(pl.String),
        Col.currency: required(pl.String),
    }
)

PERILS_SCHEMA = strict_schema(
    {
        Col.modelled_peril: required(pl.String),
        Col.rollup_peril: required(pl.String),
        "region": required(pl.String),
        "peril": required(pl.String),
        Col.region_peril_id: required(pl.Int64),
        Col.base_model: required(pl.String),
        Col.selection_priority: required(pl.Int64),
        Col.is_dialsup: required(pl.Int64),
    }
)

BLENDING_FACTORS_SEED_SCHEMA = strict_schema(
    {
        "id": required(pl.Int64),
        "BlendSetID": required(pl.Int64),
        RawCol.RegionPerilID: required(pl.Int64),
        "RegionPeril": required(pl.String),
        RawCol.SubRegionPerilID: required(pl.String),
        RawCol.SubRegionPeril: pa.Column(pl.String, nullable=True, coerce=True),
        RawCol.AIRBlend: required(pl.Float64),
        RawCol.RMSBlend: required(pl.Float64),
        "KatRiskBlend": required(pl.Float64),
        "DateCreated": required(pl.String),
    }
)
FX_RATES_SEED_SCHEMA = strict_schema(
    {
        RawCol.currency_code: required(pl.String),
        Col.target_currency: required(pl.String),
        RawCol.rate_date: required(pl.String),
        RawCol.rate: required(pl.Float64),
    }
)
FORECAST_FACTORS_SEED_SCHEMA = strict_schema(
    {
        Col.class_: required(pl.String),
        Col.office: required(pl.String),
        "office_iso2": required(pl.String),
        Col.forecast_date: required(pl.String),
        RawCol.factor: required(pl.Float64),
    }
)
EUWS_FACTORS_SEED_SCHEMA = strict_schema(
    {
        Col.model_event_id: required(pl.Int64),
        RawCol.occ_year: required(pl.Int64),
        RawCol.factor: required(pl.Float64),
    }
)

VERISK_EVENTS_RAW_SCHEMA = pa.DataFrameSchema(
    {
        RawCol.EventID: pa.Column(pl.Int64, nullable=True),
        RawCol.ModelID: pa.Column(pl.Int64, nullable=True),
        RawCol.Event: pa.Column(pl.Int64, nullable=True),
        RawCol.Year: pa.Column(pl.Int64, nullable=True),
        RawCol.Day: pa.Column(pl.Int64, nullable=True),
    },
    strict=False,
)
EUWS_OVERRIDES_SCHEMA = strict_schema(
    {
        Col.rollup_lob: required(pl.String),
        RawCol.max_rank: required(pl.Int64),
        RawCol.factor: required(pl.Float64),
    }
)


@dataclass(frozen=True)
class StagingFrames:
    verisk_ylt: pl.LazyFrame
    risklink_ylt: pl.LazyFrame
    verisk_events: pl.LazyFrame
    ep_summaries: pl.DataFrame
    lobs: pl.DataFrame
    perils: pl.DataFrame
    blending: pl.DataFrame
    fx_rates: pl.DataFrame
    forecast_factors: pl.DataFrame
    euws_factors: pl.DataFrame
    euws_overrides: pl.DataFrame


class RollupInputValidationFailure(ValueError):
    pass


def load_sources(data_root: str | Path) -> StagingFrames:
    data_root = Path(data_root)
    verisk_ylt = scan_parquet_folder(data_root / "ylt" / "verisk")
    risklink_ylt = scan_parquet_folder(data_root / "ylt" / "risklink")
    VERISK_YLT_SCHEMA.validate(verisk_ylt)
    RISKLINK_YLT_SCHEMA.validate(risklink_ylt)
    validate_lazy_required_nulls(verisk_ylt, "verisk_ylt", VERISK_YLT_REQUIRED_COLUMNS)
    validate_lazy_required_nulls(
        risklink_ylt, "risklink_ylt", RISKLINK_YLT_REQUIRED_COLUMNS
    )

    ep_summaries = read_ep_summaries(data_root)
    EP_SUMMARY_SCHEMA.validate(ep_summaries)

    lobs = read_seed_csv(data_root, "lobs.csv")
    LOBS_SCHEMA.validate(lobs)
    perils = read_seed_csv(data_root, "perils.csv")
    PERILS_SCHEMA.validate(perils)
    blending = read_optional_seed(
        data_root,
        ("blending_factors.csv", "blending_weights.csv"),
        BLENDING_FACTORS_SEED_SCHEMA,
    )
    fx_rates = read_optional_seed(data_root, ("fx_rates.csv",), FX_RATES_SEED_SCHEMA)
    forecast_factors = read_optional_seed(
        data_root, ("forecast_factors.csv",), FORECAST_FACTORS_SEED_SCHEMA
    )
    euws_factors = read_optional_seed(
        data_root, ("euws_rate_factors.csv",), EUWS_FACTORS_SEED_SCHEMA
    )
    euws_overrides = read_optional_adjustment(data_root, "euws_rank_overrides.csv")

    return StagingFrames(
        verisk_ylt=verisk_ylt,
        risklink_ylt=risklink_ylt,
        verisk_events=load_verisk_events(data_root),
        ep_summaries=ep_summaries,
        lobs=lobs,
        perils=perils,
        blending=blending,
        fx_rates=fx_rates,
        forecast_factors=forecast_factors,
        euws_factors=euws_factors,
        euws_overrides=euws_overrides,
    )


def scan_parquet_folder(folder: Path) -> pl.LazyFrame:
    paths = sorted(folder.glob("*.parquet"))
    if not paths:
        raise FileNotFoundError(f"no parquet files found in {folder}")
    return pl.scan_parquet(str(folder / "*.parquet"))


def validate_lazy_required_nulls(
    frame: pl.LazyFrame,
    source_group: str,
    required_columns: tuple[str, ...],
) -> None:
    null_counts = frame.select(
        pl.col(column).null_count().alias(column) for column in required_columns
    ).collect()
    null_count_by_column = null_counts.row(0, named=True)
    columns_with_nulls = [
        column for column in required_columns if null_count_by_column.get(column, 0) > 0
    ]
    if columns_with_nulls:
        raise RollupInputValidationFailure(
            f"{source_group} required columns contain nulls: {', '.join(columns_with_nulls)}"
        )


def read_ep_summaries(data_root: Path) -> pl.DataFrame:
    paths = sorted((data_root / "ep_summaries").rglob("*.long.csv"))
    if not paths:
        raise FileNotFoundError(
            f"no EP summary .long.csv files found in {data_root / 'ep_summaries'}"
        )
    return pl.concat([pl.read_csv(path) for path in paths], how="diagonal_relaxed")


def read_seed_csv(data_root: Path, filename: str) -> pl.DataFrame:
    paths = sorted(
        (data_root / "seeds").rglob(filename),
        key=lambda path: (len(path.parts), str(path)),
    )
    if not paths:
        raise FileNotFoundError(f"seed file not found: {filename}")
    return pl.read_csv(paths[0])


def read_optional_seed(
    data_root: Path,
    filenames: tuple[str, ...],
    schema: pa.DataFrameSchema | None = None,
) -> pl.DataFrame:
    for filename in filenames:
        try:
            frame = read_seed_csv(data_root, filename)
            if schema is not None:
                schema.validate(frame)
            return frame
        except FileNotFoundError:
            continue
    return pl.DataFrame()


def load_verisk_events(data_root: Path) -> pl.LazyFrame:
    path = data_root / "seeds" / "validation" / "verisk_events.parquet"
    if not path.exists():
        if path.parent.exists():
            raise FileNotFoundError(
                f"seed file not found: {path.relative_to(data_root)}"
            )
        return empty_verisk_events()
    events = pl.scan_parquet(path)
    VERISK_EVENTS_RAW_SCHEMA.validate(events)
    return events.select(
        pl.col(RawCol.EventID).alias(Col.model_event_id),
        pl.col(RawCol.ModelID).alias(Col.model_code),
        pl.col(RawCol.Event).alias(Col.event_id),
        pl.col(RawCol.Year).alias(Col.year_id),
        pl.col(RawCol.Day).alias(Col.event_day),
    )


def read_optional_adjustment(data_root: Path, filename: str) -> pl.DataFrame:
    path = data_root / "seeds" / "adjustments" / filename
    if not path.exists():
        if path.parent.exists():
            raise FileNotFoundError(
                f"seed file not found: {path.relative_to(data_root)}"
            )
        return pl.DataFrame()
    frame = pl.read_csv(path)
    if filename == "euws_rank_overrides.csv":
        EUWS_OVERRIDES_SCHEMA.validate(frame)
    return frame


def empty_verisk_events() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            Col.model_event_id: pl.Int64,
            Col.model_code: pl.Int64,
            Col.event_id: pl.Int64,
            Col.year_id: pl.Int64,
            Col.event_day: pl.Int64,
        }
    ).lazy()
