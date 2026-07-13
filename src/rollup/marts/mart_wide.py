from __future__ import annotations
# mypy: ignore-errors

from collections.abc import Callable

import polars as pl

from rollup.columns import Col


_MTS_WIDE_IDENTITY_COLUMN_ORDER = [
    Col.vendor,
    Col.analysis_id,
    Col.base_model,
    Col.output_use,
    Col.model_code,
    Col.year_id,
    Col.event_id,
    Col.model_event_id,
    Col.event_day,
    Col.risklink_event_day,
    Col.model_occurrence_year,
    Col.model_occurrence_date,
    Col.rollup_lob,
    Col.rollup_peril,
    Col.modelled_lob,
    Col.modelled_peril,
    Col.region_peril_id,
    Col.blend_subregion_peril_id,
    Col.sub_region_peril_id,
    Col.sub_region_peril,
    Col.cds_cat_class_name,
    Col.class_,
    Col.office,
    Col.currency,
    Col.target_currency,
    Col.fx_rate_date,
    Col.fx_rate,
    Col.rnk,
    Col.rp,
    Col.rp_bucket,
    Col.forecast_factor_raw,
    Col.forecast_factor,
    Col.euws_factor_raw_source,
    Col.euws_factor_raw,
    Col.euws_factor,
    Col.euws_override_max_rank,
    Col.euws_override_factor,
    Col.euws_override_applied,
]

_MTS_WIDE_BASE_LOSS_COLUMN_ORDER = [
    Col.original_ylt_loss,
    Col.risklink_loss,
    Col.verisk_loss,
    Col.base_model_loss,
    Col.target_loss,
]

_MTS_WIDE_BLEND_DIAGNOSTIC_COLUMN_ORDER = [
    Col.risklink_weight,
    Col.verisk_weight,
    Col.risklink_blended_contribution,
    Col.verisk_blended_contribution,
    Col.uplift_factor_on_base_model,
]

_MTS_WIDE_MAIN_LOSS_PREFIX_ORDER = [
    "original",
    Col.original_ylt_loss_blended,
    "blended",
    Col.original_ylt_loss_blended_localccy,
    "localccy",
    Col.original_ylt_loss_blended_localccy_forecast,
    "localccy_forecast",
    Col.original_ylt_loss_blended_localccy_forecast_euws_raw,
    Col.original_ylt_loss_blended_localccy_forecast_euws,
    "euws",
    "euws_override",
]


def _mts_output_dimensions(frame: pl.DataFrame | pl.LazyFrame) -> list[str]:
    diagnostic_cols = {
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    }
    return [
        col for col in frame.columns
        if col not in (Col.metric, Col.forecast_date, Col.loss)
        and col not in diagnostic_cols
        and not col.startswith("_")
    ]


def _ordered_mts_wide_columns(columns: list[str]) -> list[str]:
    """Return the presentation order for mts_tbl_ylt_combined_all_factors_wide.

    The helper is intentionally presentation-only: it preserves the supplied
    column set exactly once while grouping columns into identity context, base
    loss context, blend diagnostics, main loss transforms, DIALSUP, and finally
    any unknown columns in their existing relative order.
    """
    remaining = list(dict.fromkeys(columns))
    ordered: list[str] = []

    def take_exact(priority: list[str]) -> None:
        for column in priority:
            if column in remaining:
                ordered.append(column)
                remaining.remove(column)

    def take_matching(predicate: Callable[[str], bool]) -> None:
        matched = [column for column in remaining if predicate(column)]
        ordered.extend(matched)
        for column in matched:
            remaining.remove(column)

    def is_dialsup_column(column: str) -> bool:
        return column.startswith("dialsup_")

    def main_loss_priority(column: str) -> tuple[int, str]:
        for index, prefix in enumerate(_MTS_WIDE_MAIN_LOSS_PREFIX_ORDER):
            if column.startswith(f"{prefix}_"):
                return index, column
        return len(_MTS_WIDE_MAIN_LOSS_PREFIX_ORDER), column

    base_loss_columns = set(_MTS_WIDE_BASE_LOSS_COLUMN_ORDER)
    blend_diagnostic_columns = set(_MTS_WIDE_BLEND_DIAGNOSTIC_COLUMN_ORDER)

    take_exact(_MTS_WIDE_IDENTITY_COLUMN_ORDER)
    take_matching(
        lambda column: not column.endswith("_loss")
        and column not in base_loss_columns
        and column not in blend_diagnostic_columns
        and not is_dialsup_column(column)
    )
    take_exact(_MTS_WIDE_BASE_LOSS_COLUMN_ORDER)
    take_exact(_MTS_WIDE_BLEND_DIAGNOSTIC_COLUMN_ORDER)

    main_loss_columns = [
        column
        for column in remaining
        if column.endswith("_loss")
        and column not in base_loss_columns
        and not is_dialsup_column(column)
    ]
    for column in sorted(main_loss_columns, key=main_loss_priority):
        ordered.append(column)
        remaining.remove(column)

    take_matching(is_dialsup_column)
    ordered.extend(remaining)
    return ordered


def _with_metric_output_use(
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    final_metric: str,
    final_output_use: str,
) -> pl.DataFrame | pl.LazyFrame:
    return frame.with_columns(
        pl.when(pl.col(Col.metric) == final_metric)
        .then(pl.lit(final_output_use))
        .otherwise(pl.lit("intermediate_audit"))
        .alias(Col.output_use)
    )
