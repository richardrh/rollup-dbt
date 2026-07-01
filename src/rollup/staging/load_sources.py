from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from rollup.columns import Col, FanoutCol, RawCol


@dataclass(frozen=True)
class StagingFrames:
    verisk_ylt: pl.LazyFrame
    risklink_ylt: pl.LazyFrame
    verisk_events: pl.LazyFrame
    risklink_flood_events: pl.LazyFrame
    ep_summaries: pl.DataFrame
    lobs: pl.DataFrame
    perils: pl.DataFrame
    blending: pl.DataFrame
    fx_rates: pl.DataFrame
    forecast_factors: pl.DataFrame
    euws_factors: pl.DataFrame
    euws_overrides: pl.DataFrame


def load_sources(data_root: str | Path) -> StagingFrames:
    data_root = Path(data_root)
    verisk_ylt = scan_parquet_folder(data_root / "ylt" / "verisk")
    risklink_ylt = scan_parquet_folder(data_root / "ylt" / "risklink")

    ep_summaries = read_ep_summaries(data_root)

    lobs = read_seed_csv(data_root, "lobs.csv")
    perils = read_seed_csv(data_root, "perils.csv")
    blending = read_optional_seed(
        data_root,
        ("blending_factors.csv", "blending_weights.csv"),
    )
    fx_rates = read_optional_seed(data_root, ("fx_rates.csv",))
    forecast_factors = read_optional_seed(
        data_root, ("forecast_factors.csv",)
    )
    euws_factors = read_optional_seed(
        data_root, ("euws_rate_factors.csv",)
    )
    euws_overrides = read_optional_adjustment(data_root, "euws_rank_overrides.csv")

    return StagingFrames(
        verisk_ylt=verisk_ylt,
        risklink_ylt=risklink_ylt,
        verisk_events=load_verisk_events(data_root),
        risklink_flood_events=load_risklink_flood_events(data_root),
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
) -> pl.DataFrame:
    for filename in filenames:
        try:
            frame = read_seed_csv(data_root, filename)
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
    return events.select(
        pl.col(RawCol.EventID).alias(Col.model_event_id),
        pl.col(RawCol.ModelID).alias(Col.model_code),
        pl.col(RawCol.Event).alias(Col.event_id),
        pl.col(RawCol.Year).alias(Col.year_id),
        pl.col(RawCol.Day).alias(Col.event_day),
    )


def load_risklink_flood_events(data_root: Path) -> pl.LazyFrame:
    path = data_root / "seeds" / "validation" / "risklink_flood22_model_events.parquet"
    if not path.exists():
        if path.parent.exists():
            raise FileNotFoundError(
                f"seed file not found: {path.relative_to(data_root)}"
            )
        return empty_risklink_flood_events()
    return (
        pl.scan_parquet(path)
        .group_by(FanoutCol.ModelEventID, RawCol.RegionPerilID)
        .agg(pl.col(RawCol.ModelOccurrenceDate).min().alias(Col.model_occurrence_date))
        .select(
            pl.col(FanoutCol.ModelEventID).cast(pl.Int64).alias(Col.event_id),
            pl.col(RawCol.RegionPerilID).cast(pl.Int64).alias(Col.region_peril_id),
            pl.col(Col.model_occurrence_date)
            .dt.ordinal_day()
            .cast(pl.Int64)
            .alias(Col.risklink_event_day),
        )
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


def empty_risklink_flood_events() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            Col.event_id: pl.Int64,
            Col.region_peril_id: pl.Int64,
            Col.risklink_event_day: pl.Int64,
        }
    ).lazy()
