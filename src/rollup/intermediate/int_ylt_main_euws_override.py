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

MODEL = "int_ylt_main_euws_override"


def validate(ylt_euws_raw: pl.LazyFrame, seeds: dict[str, pl.LazyFrame]) -> None:
    validate_mapping_key(MODEL, "seeds", seeds, "euws_rank_overrides")
    overrides = seeds["euws_rank_overrides"]
    ylt_schema = collect_lazy_schema(MODEL, "ylt_euws_raw", ylt_euws_raw)
    override_schema = collect_lazy_schema(MODEL, "seeds.euws_rank_overrides", overrides)
    require_columns(
        MODEL,
        "ylt_euws_raw",
        ylt_schema,
        [
            Col.rollup_lob,
            Col.rnk,
            Col.loss,
            "_euws_factor_raw",
            "_localccy_forecast_loss",
        ],
    )
    require_columns(
        MODEL,
        "seeds.euws_rank_overrides",
        override_schema,
        [Col.rollup_lob, RawCol.max_rank],
    )
    require_dtype_family(
        MODEL, "seeds.euws_rank_overrides", override_schema, RawCol.factor, "numeric"
    )
    require_join_key_compatible(
        MODEL,
        "ylt_euws_raw",
        ylt_schema,
        "seeds.euws_rank_overrides",
        override_schema,
        [Col.rollup_lob],
    )


def transform(
    ylt_euws_raw: pl.LazyFrame, seeds: dict[str, pl.LazyFrame]
) -> pl.LazyFrame:
    validate(ylt_euws_raw, seeds)
    euws_overrides = seeds["euws_rank_overrides"].select(
        Col.rollup_lob,
        pl.col(RawCol.max_rank).alias("_euws_override_max_rank"),
        pl.col(RawCol.factor).alias("_euws_override_factor"),
    )
    override_condition = (
        pl.col("_euws_override_factor").is_not_null()
        & (pl.col(Col.rnk) <= pl.col("_euws_override_max_rank"))
        & (pl.col("_euws_factor_raw") == 0)
    )
    frame = (
        ylt_euws_raw.join(euws_overrides, on=Col.rollup_lob, how="left")
        .with_columns(
            pl.when(override_condition)
            .then(pl.col("_euws_override_factor"))
            .otherwise(pl.col("_euws_factor_raw"))
            .alias("_euws_factor")
        )
        .with_columns(
            pl.when(override_condition)
            .then(pl.col("_localccy_forecast_loss") * pl.col("_euws_override_factor"))
            .otherwise(pl.col(Col.loss))
            .alias(Col.loss),
            pl.lit("euws_override").alias(Col.metric),
        )
        .drop("_euws_override_max_rank", "_euws_override_factor", "_euws_factor")
    )
    validate_output(MODEL, frame)
    return frame
