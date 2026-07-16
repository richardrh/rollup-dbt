from __future__ import annotations
import polars as pl
from rollup.columns import Col, RawCol
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
    require_join_key_compatible,
    validate_mapping_key,
)

MODEL = "int_ylt_main_euws"


def validate(
    ylt_forecasted: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    seeds: dict[str, pl.LazyFrame],
) -> None:
    validate_mapping_key(MODEL, "seeds", seeds, "euws_rate_factors")
    rate_factors = seeds["euws_rate_factors"]
    ylt_schema = collect_lazy_schema(MODEL, "ylt_forecasted", ylt_forecasted)
    events_schema = collect_lazy_schema(MODEL, "verisk_events", verisk_events)
    factor_schema = collect_lazy_schema(MODEL, "seeds.euws_rate_factors", rate_factors)
    require_columns(
        MODEL,
        "ylt_forecasted",
        ylt_schema,
        [Col.event_id, Col.year_id, Col.model_code, Col.loss],
    )
    require_columns(
        MODEL,
        "verisk_events",
        events_schema,
        [Col.event_id, Col.year_id, Col.model_code, Col.model_event_id],
    )
    require_columns(
        MODEL, "seeds.euws_rate_factors", factor_schema, [Col.model_event_id]
    )
    require_dtype_family(
        MODEL, "seeds.euws_rate_factors", factor_schema, RawCol.factor, "numeric"
    )
    require_join_key_compatible(
        MODEL,
        "ylt_forecasted",
        ylt_schema,
        "verisk_events",
        events_schema,
        [Col.event_id, Col.year_id, Col.model_code],
    )
    require_join_key_compatible(
        MODEL,
        "verisk_events",
        events_schema,
        "seeds.euws_rate_factors",
        factor_schema,
        [Col.model_event_id],
    )


def transform(
    ylt_forecasted: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    seeds: dict[str, pl.LazyFrame],
) -> pl.LazyFrame:
    validate(ylt_forecasted, verisk_events, seeds)
    euws_factors = seeds["euws_rate_factors"].select(
        Col.model_event_id,
        pl.col(RawCol.factor).alias("_euws_factor_raw_source"),
    )
    frame = (
        ylt_forecasted.join(
            verisk_events, on=[Col.event_id, Col.year_id, Col.model_code], how="left"
        )
        .join(euws_factors, on=Col.model_event_id, how="left")
        .with_columns(
            pl.col("_euws_factor_raw_source").fill_null(1.0).alias("_euws_factor_raw")
        )
        .with_columns(
            pl.col(Col.loss).alias("_localccy_forecast_loss"),
            (pl.col(Col.loss) * pl.col("_euws_factor_raw")).alias(Col.loss),
            pl.lit("euws").alias(Col.metric),
        )
        .drop("_euws_factor_raw_source")
    )
    validate_output(MODEL, frame)
    return frame
