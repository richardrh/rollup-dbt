from __future__ import annotations

import polars as pl

from rollup.columns import Col, FanoutCol


def enrich_risklink_event_days(
    ylt: pl.LazyFrame, risklink_events: pl.LazyFrame
) -> pl.LazyFrame:
    join_year = "__risklink_model_occurrence_year"
    non_risklink = ylt.filter(pl.col(Col.base_model) != "risklink").with_columns(
        pl.lit(None).cast(pl.Int64).alias(Col.model_occurrence_year),
        pl.lit(None).cast(pl.Int64).alias(Col.risklink_event_day),
    )
    risklink_before = ylt.filter(pl.col(Col.base_model) == "risklink")
    risklink = risklink_before.join(
        risklink_events.with_columns(
            pl.col(Col.model_occurrence_year).alias(join_year)
        ),
        left_on=[Col.event_id, Col.year_id, Col.region_peril_id],
        right_on=[Col.event_id, join_year, Col.region_peril_id],
        how="inner",
    )
    return pl.concat([non_risklink, risklink], how="diagonal_relaxed")


def build_fanout(ylt: pl.LazyFrame, risklink_events: pl.LazyFrame) -> pl.LazyFrame:
    ylt = enrich_risklink_event_days(ylt, risklink_events)
    return ylt.select(
        Col.forecast_date,
        Col.base_model,
        pl.col(Col.metric),
        pl.when(pl.col(Col.base_model) == "risklink")
        .then(pl.col(Col.event_id))
        .otherwise(pl.col(Col.model_event_id))
        .cast(pl.Int64)
        .alias(FanoutCol.ModelEventID),
        pl.col(Col.year_id).cast(pl.Int64).alias(FanoutCol.ModelYear),
        pl.col(Col.target_currency).alias(FanoutCol.CurrencyCode),
        pl.lit(0).cast(pl.Int64).alias(FanoutCol.ModelYOA),
        pl.col(Col.loss).cast(pl.Float64).alias(FanoutCol.ModelGrossLoss),
        pl.lit(0).cast(pl.Int64).alias(FanoutCol.ModelInwardsReinstatement),
        pl.when(pl.col(Col.base_model) == "risklink")
        .then(pl.col(Col.risklink_event_day))
        .otherwise(pl.col(Col.event_day))
        .cast(pl.Int64)
        .alias(FanoutCol.ModelEventDay),
        pl.col(Col.cds_cat_class_name).alias(FanoutCol.LossClassName),
    )
