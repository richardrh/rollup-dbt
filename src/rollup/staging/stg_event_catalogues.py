from __future__ import annotations
# mypy: ignore-errors

import polars as pl

from rollup.columns import Col, FanoutCol, RawCol


def stg_event_catalogue__verisk(raw_events: pl.LazyFrame) -> pl.LazyFrame:
    return raw_events.select(
        pl.col(RawCol.EventID).alias(Col.model_event_id),
        pl.col(RawCol.ModelID).alias(Col.model_code),
        pl.col(RawCol.Event).alias(Col.event_id),
        pl.col(RawCol.Year).alias(Col.year_id),
        pl.col(RawCol.Day).alias(Col.event_day),
    )


def stg_event_catalogue__risklink_flood(raw_events: pl.LazyFrame) -> pl.LazyFrame:
    return (
        raw_events.group_by(FanoutCol.ModelEventID, RawCol.ModelOccurrenceYear, RawCol.RegionPerilID)
        .agg(pl.col(RawCol.ModelOccurrenceDate).min().alias(Col.model_occurrence_date))
        .select(
            pl.col(FanoutCol.ModelEventID).cast(pl.Int64).alias(Col.event_id),
            pl.col(RawCol.ModelOccurrenceYear).cast(pl.Int64).alias(Col.model_occurrence_year),
            pl.col(RawCol.RegionPerilID).cast(pl.Int64).alias(Col.region_peril_id),
            pl.col(Col.model_occurrence_date).dt.ordinal_day().cast(pl.Int64).alias(Col.risklink_event_day),
        )
    )
