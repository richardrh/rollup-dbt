from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl
import pytest

from rollup.columns import Col, RawCol
from rollup.config import RollupConfig
from rollup.intermediate import (
    int_forecast_dates,
    int_ylt_base_selected,
    int_ylt_dialsup_factor_base,
    int_ylt_dialsup_forecast_metric,
    int_ylt_dialsup_local_currency_metric,
    int_ylt_dialsup_metric_stream,
    int_ylt_dialsup_original_metric,
    int_ylt_enriched,
    int_ylt_main_blended,
    int_ylt_main_euws,
    int_ylt_main_euws_override,
    int_ylt_main_forecast,
    int_ylt_main_local_currency,
    int_ylt_main_metric_stream,
    int_ylt_normalized,
    int_ylt_ranked,
)
from rollup.staging import (
    stg_forecast_factors,
    stg_gbp_fx_rates,
    stg_risklink_ylt,
    stg_verisk_ylt,
)


def _empty_risklink_ylt() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            RawCol.anlsid: pl.Int64,
            RawCol.yearid: pl.Int64,
            RawCol.eventid: pl.Int64,
            RawCol.loss: pl.Float64,
        }
    ).lazy()


def test_forecast_factor_staging_canonicalizes_iso_dates() -> None:
    staged = stg_forecast_factors.Model.transform(
        pl.DataFrame(
            {
                Col.class_: ["COMM"],
                "office_iso2": ["DE"],
                Col.forecast_date: ["2026-12-31"],
                RawCol.factor: [2.5],
            }
        ).lazy()
    )

    assert staged.collect_schema()[Col.forecast_date] == pl.Date
    assert staged.collect().get_column(Col.forecast_date).to_list() == [
        date(2026, 12, 31)
    ]


def test_forecast_factor_staging_rejects_malformed_dates_on_collect() -> None:
    staged = stg_forecast_factors.Model.transform(
        pl.DataFrame(
            {
                Col.class_: ["COMM"],
                "office_iso2": ["DE"],
                Col.forecast_date: ["31/12/2026"],
                RawCol.factor: [2.5],
            }
        ).lazy()
    )

    with pytest.raises(pl.exceptions.PolarsError):
        staged.collect()


def test_forecast_dates_model_canonicalizes_date_output_schema() -> None:
    candidate = int_forecast_dates.Model.transform(
        pl.DataFrame({Col.forecast_date: ["2026-01-01"]}).lazy()
    )
    assert candidate.collect_schema() == int_forecast_dates.Model.schema()


def test_main_forecast_model_rejects_noncanonical_final_schema() -> None:
    with pytest.raises(
        ValueError, match="int_ylt_main_forecast.*output schema mismatch"
    ):
        int_ylt_main_forecast.Model.validate(
            pl.LazyFrame(
                schema={
                    **int_ylt_main_local_currency.Model.schema(),
                    Col.forecast_date: pl.String,
                }
            )
        )


def test_main_ylt_schema_contracts_accept_exact_empty_candidates() -> None:
    models: list[Any] = [
        int_ylt_enriched.Model,
        int_ylt_base_selected.Model,
        int_ylt_ranked.Model,
        int_ylt_main_blended.Model,
        int_ylt_main_local_currency.Model,
        int_ylt_main_forecast.Model,
        int_ylt_main_euws.Model,
        int_ylt_main_euws_override.Model,
        int_ylt_main_metric_stream.Model,
        int_ylt_dialsup_factor_base.Model,
        int_ylt_dialsup_original_metric.Model,
        int_ylt_dialsup_local_currency_metric.Model,
        int_ylt_dialsup_forecast_metric.Model,
        int_ylt_dialsup_metric_stream.Model,
    ]
    for model in models:
        candidate = pl.LazyFrame(schema=model.schema())
        model.validate(candidate)
        assert candidate.collect_schema() == model.schema()


def test_dialsup_factor_base_model_rejects_noncanonical_final_schema() -> None:
    with pytest.raises(
        ValueError, match="int_ylt_dialsup_factor_base.*output schema mismatch"
    ):
        int_ylt_dialsup_factor_base.Model.validate(
            pl.LazyFrame(schema={Col.loss: pl.Float64})
        )


def test_dialsup_models_preserve_factor_columns_and_metric_values() -> None:
    ylt = pl.DataFrame(
        {
            Col.vendor: ["verisk"],
            Col.analysis_id: ["analysis"],
            Col.modelled_lob: ["LOB"],
            Col.modelled_peril: ["PERIL"],
            Col.rollup_lob: ["ROLLUP_LOB"],
            Col.rollup_peril: ["ROLLUP_PERIL"],
            Col.region_peril_id: [1],
            Col.blend_subregion_peril_id: ["1"],
            Col.base_model: ["verisk"],
            Col.selection_priority: [1],
            Col.is_dialsup: [1],
            Col.is_euws: [0],
            Col.cds_cat_class_name: ["Wind"],
            Col.event_id: [1],
            Col.year_id: [2026],
            Col.model_code: [41],
            Col.currency: ["EUR"],
            Col.class_: ["COMM"],
            Col.office: ["DE"],
            Col.loss: [88.0],
            Col.metric: ["original"],
            Col.rnk: [1],
            Col.rp: [1.0],
            Col.rp_bucket: pl.Series([0], dtype=pl.Int32),
        }
    ).lazy()
    verisk_events = pl.DataFrame(
        {
            Col.model_event_id: [1001],
            Col.event_id: [1],
            Col.year_id: [2026],
            Col.model_code: [41],
            Col.event_day: [7],
        }
    ).lazy()
    fx_rates = pl.DataFrame(
        {
            Col.currency: ["EUR"],
            Col.target_currency: ["GBP"],
            Col.fx_rate_date: ["2026-01-01"],
            Col.fx_rate: [0.88],
        }
    ).lazy()
    forecast_dates = pl.DataFrame({Col.forecast_date: [date(2026, 1, 1)]}).lazy()
    forecast_factors = pl.DataFrame(
        {
            Col.class_: ["COMM"],
            Col.office: ["DE"],
            Col.forecast_date: [date(2026, 1, 1)],
            "_forecast_factor_raw": [2.5],
        }
    ).lazy()

    factor_base = int_ylt_dialsup_factor_base.Model.transform(
        ylt, verisk_events, fx_rates, forecast_dates, forecast_factors
    )
    original = int_ylt_dialsup_original_metric.Model.transform(factor_base)
    local_currency = int_ylt_dialsup_local_currency_metric.Model.transform(factor_base)
    forecast = int_ylt_dialsup_forecast_metric.Model.transform(factor_base)
    metric_stream = int_ylt_dialsup_metric_stream.Model.transform(
        original, local_currency, forecast
    )

    assert factor_base.collect_schema() == int_ylt_dialsup_factor_base.Model.schema()
    assert factor_base.select(
        Col.model_event_id,
        Col.event_day,
        Col.target_currency,
        Col.fx_rate_date,
        "_forecast_factor_raw",
        "_forecast_factor",
    ).collect().to_dict(as_series=False) == {
        Col.model_event_id: [1001],
        Col.event_day: [7],
        Col.target_currency: ["GBP"],
        Col.fx_rate_date: ["2026-01-01"],
        "_forecast_factor_raw": [2.5],
        "_forecast_factor": [2.5],
    }
    assert original.collect_schema() == int_ylt_dialsup_original_metric.Model.schema()
    assert (
        local_currency.collect_schema()
        == int_ylt_dialsup_local_currency_metric.Model.schema()
    )
    assert forecast.collect_schema() == int_ylt_dialsup_forecast_metric.Model.schema()
    assert (
        metric_stream.collect_schema() == int_ylt_dialsup_metric_stream.Model.schema()
    )
    assert metric_stream.select(Col.metric, Col.loss).collect().sort(
        Col.metric
    ).rows() == [
        ("dialsup_localccy", 100.0),
        ("dialsup_localccy_forecast", 250.0),
        ("dialsup_original", 88.0),
    ]


def test_normalize_ylt_accepts_padded_verisk_stc_and_strips_join_fields() -> None:
    verisk = pl.DataFrame(
        {
            RawCol.CatalogTypeCode: ["STC     ", "HIST    "],
            RawCol.ExposureAttribute: ["HIC_HH_UK   ", "HIC_HH_UK   "],
            RawCol.Analysis: ["UK_WSSS   ", "UK_WSSS   "],
            RawCol.ModelCode: [1, 1],
            RawCol.YearID: [2026, 2026],
            RawCol.EventID: [100, 101],
            RawCol.GroundUpLoss: [10.0, 20.0],
        }
    ).lazy()

    normalized = int_ylt_normalized.Model.transform(
        stg_verisk_ylt.Model.transform(verisk),
        stg_risklink_ylt.Model.transform(_empty_risklink_ylt()),
    ).collect()

    assert normalized.to_dict(as_series=False) == {
        Col.vendor: ["verisk"],
        Col.analysis_id: ["UK_WSSS"],
        Col.modelled_peril: ["UK_WSSS"],
        Col.modelled_lob: ["HIC_HH_UK"],
        Col.model_code: [1],
        Col.year_id: [2026],
        Col.event_id: [100],
        Col.loss: [10.0],
    }


def test_main_ylt_metrics_apply_fx_forecast_euws_and_rank_override() -> None:
    ylt_ranked = pl.DataFrame(
        {
            Col.vendor: ["verisk"],
            Col.modelled_lob: ["LOB"],
            Col.modelled_peril: ["PERIL"],
            Col.rollup_lob: ["HIC_HH_UK"],
            Col.rollup_peril: ["UK_WS"],
            Col.region_peril_id: [101],
            Col.blend_subregion_peril_id: ["101"],
            Col.base_model: ["verisk"],
            Col.analysis_id: ["analysis"],
            Col.selection_priority: [1],
            Col.is_dialsup: [0],
            Col.is_euws: [1],
            Col.cds_cat_class_name: ["Wind"],
            Col.class_: ["COMM"],
            Col.office: ["DE"],
            Col.currency: ["EUR"],
            Col.model_code: [41],
            Col.year_id: [2026],
            Col.event_id: [10],
            Col.rnk: [50],
            Col.rp: [1.0],
            Col.rp_bucket: pl.Series([0], dtype=pl.Int32),
            Col.metric: ["original"],
            Col.loss: [88.0],
        }
    ).lazy()
    ep_blending_targets = pl.DataFrame(
        {
            Col.rollup_lob: ["HIC_HH_UK"],
            Col.rollup_peril: ["UK_WS"],
            Col.region_peril_id: [101],
            Col.blend_subregion_peril_id: ["101"],
            Col.return_period: [0],
            Col.ep_type: ["AAL"],
            Col.risklink_loss: [0.0],
            Col.verisk_loss: [88.0],
            Col.risklink_blended_contribution: [0.0],
            Col.verisk_blended_contribution: [88.0],
            Col.target_loss: [88.0],
            Col.base_model: ["verisk"],
            Col.base_model_loss: [88.0],
            Col.uplift_factor_on_base_model: [1.0],
        }
    ).lazy()
    verisk_events = (
        pl.DataFrame(
            {
                RawCol.EventID: [1001],
                RawCol.ModelID: [41],
                RawCol.Event: [10],
                RawCol.Year: [2026],
                RawCol.Day: [1],
            }
        )
        .lazy()
        .select(
            pl.col(RawCol.EventID).alias(Col.model_event_id),
            pl.col(RawCol.ModelID).alias(Col.model_code),
            pl.col(RawCol.Event).alias(Col.event_id),
            pl.col(RawCol.Year).alias(Col.year_id),
            pl.col(RawCol.Day).alias(Col.event_day),
        )
    )
    seeds = {
        "euws_rate_factors": pl.DataFrame(
            {Col.model_event_id: [1001], RawCol.factor: [0.0]}
        ).lazy(),
        "euws_rank_overrides": pl.DataFrame(
            {
                Col.rollup_lob: ["HIC_HH_UK"],
                RawCol.max_rank: [100],
                RawCol.factor: [1.0],
            }
        ).lazy(),
    }

    fx_rates = stg_gbp_fx_rates.Model.transform(
        pl.DataFrame(
            {
                RawCol.currency_code: ["EUR"],
                Col.target_currency: ["GBP"],
                RawCol.rate_date: ["2026-01-01"],
                RawCol.rate: [0.88],
            }
        ).lazy()
    )
    forecast_factors = stg_forecast_factors.Model.transform(
        pl.DataFrame(
            {
                Col.class_: ["COMM"],
                Col.office: ["Germany"],
                "office_iso2": ["DE"],
                Col.forecast_date: ["2026-12-31"],
                RawCol.factor: [2.5],
            }
        ).lazy()
    )
    forecast_dates = int_forecast_dates.Model.transform(forecast_factors)
    ylt_blended = int_ylt_main_blended.Model.transform(ylt_ranked, ep_blending_targets)
    ylt_localccy = int_ylt_main_local_currency.Model.transform(ylt_blended, fx_rates)
    ylt_localccy_forecast = int_ylt_main_forecast.Model.transform(
        ylt_localccy, forecast_dates, forecast_factors
    )
    ylt_euws = int_ylt_main_euws.Model.transform(
        ylt_localccy_forecast, verisk_events, seeds
    )
    ylt_euws_override = int_ylt_main_euws_override.Model.transform(ylt_euws, seeds)
    combined = pl.concat(
        [
            ylt_ranked,
            ylt_blended,
            ylt_localccy,
            ylt_localccy_forecast,
            ylt_euws,
            ylt_euws_override,
        ],
        how="diagonal",
    )

    assert set(combined.select(Col.metric).collect().to_series().to_list()) == {
        "original",
        "blended",
        "localccy",
        "localccy_forecast",
        "euws",
        "euws_override",
    }
    assert ylt_localccy.select(Col.loss, Col.target_currency).collect().to_dict(
        as_series=False
    ) == {
        Col.loss: [100.0],
        Col.target_currency: ["EUR"],
    }
    assert ylt_localccy_forecast.select(Col.forecast_date, Col.loss).collect().to_dict(
        as_series=False
    ) == {
        Col.forecast_date: [date(2026, 12, 31)],
        Col.loss: [250.0],
    }
    assert ylt_euws_override.select(
        Col.rnk, "_euws_factor_raw", Col.loss
    ).collect().to_dict(as_series=False) == {
        Col.rnk: [50],
        "_euws_factor_raw": [0.0],
        Col.loss: [250.0],
    }


def test_forecast_factor_csv_strings_reach_fanout_as_date_like_schema() -> None:
    forecast_factors = stg_forecast_factors.Model.transform(
        pl.DataFrame(
            {
                Col.class_: ["COMM"],
                "office_iso2": ["DE"],
                Col.forecast_date: ["2026-01-01"],
                RawCol.factor: [2.0],
            }
        ).lazy()
    )
    forecast_dates = int_forecast_dates.Model.transform(forecast_factors)
    ylt_localccy = pl.DataFrame(
        {
            Col.class_: ["COMM"],
            Col.office: ["DE"],
            Col.loss: [100.0],
            Col.metric: ["localccy"],
            Col.base_model: ["verisk"],
            Col.event_id: [10],
            Col.year_id: [2026],
            Col.region_peril_id: [101],
            Col.model_event_id: [1001],
            Col.event_day: [42],
            Col.target_currency: ["GBP"],
            Col.cds_cat_class_name: ["Wind"],
        }
    ).lazy()
    ylt_forecast = int_ylt_main_forecast.Model.transform(
        pl.DataFrame(
            {
                **ylt_localccy.collect().to_dict(as_series=False),
                Col.vendor: ["verisk"],
                Col.analysis_id: ["analysis"],
                Col.modelled_lob: ["LOB"],
                Col.modelled_peril: ["PERIL"],
                Col.rollup_lob: ["LOB"],
                Col.rollup_peril: ["PERIL"],
                Col.blend_subregion_peril_id: ["101"],
                Col.selection_priority: [1],
                Col.is_dialsup: [0],
                Col.is_euws: [0],
                Col.rnk: [1],
                Col.rp: [1.0],
                Col.rp_bucket: [0],
                Col.risklink_blended_contribution: [0.0],
                Col.verisk_blended_contribution: [0.0],
                Col.uplift_factor_on_base_model: [1.0],
                Col.currency: ["GBP"],
                Col.model_code: [1],
            }
        ).lazy(),
        forecast_dates,
        forecast_factors,
    ).with_columns(pl.lit("euws_override").alias(Col.metric))
    assert ylt_forecast.collect_schema()[Col.forecast_date] == pl.Date
    assert ylt_forecast.select(Col.forecast_date).collect().item() == date(2026, 1, 1)


def test_apply_ep_blending_to_ylt_retains_blend_diagnostics() -> None:
    targets = pl.DataFrame(
        {
            Col.rollup_lob: ["Property"],
            Col.rollup_peril: ["Europe_FL"],
            Col.region_peril_id: [216],
            Col.blend_subregion_peril_id: ["216b"],
            Col.return_period: [0],
            Col.ep_type: ["AAL"],
            Col.rp_bucket: [0],
            Col.base_model: ["risklink"],
            Col.vendor: ["risklink"],
            Col.analysis_id: ["analysis"],
            Col.modelled_lob: ["LOB"],
            Col.modelled_peril: ["PERIL"],
            Col.selection_priority: [1],
            Col.is_dialsup: [0],
            Col.is_euws: [0],
            Col.cds_cat_class_name: ["Wind"],
            Col.class_: ["COMM"],
            Col.office: ["DE"],
            Col.currency: ["EUR"],
            Col.model_code: [1],
            Col.year_id: [2026],
            Col.event_id: [1],
            Col.rnk: [1],
            Col.rp: [1.0],
            Col.risklink_loss: [200.0],
            Col.verisk_loss: [100.0],
            Col.target_loss: [175.0],
            Col.base_model_loss: [200.0],
            Col.risklink_blended_contribution: [150.0],
            Col.verisk_blended_contribution: [25.0],
            Col.uplift_factor_on_base_model: [0.875],
        }
    ).lazy()
    ylt = pl.DataFrame(
        {
            Col.rollup_lob: ["Property"],
            Col.rollup_peril: ["Europe_FL"],
            Col.region_peril_id: [216],
            Col.blend_subregion_peril_id: ["216b"],
            Col.rp_bucket: [0],
            Col.base_model: ["risklink"],
            Col.loss: [10.0],
            Col.metric: ["original"],
            Col.vendor: ["risklink"],
            Col.analysis_id: ["analysis"],
            Col.modelled_lob: ["LOB"],
            Col.modelled_peril: ["PERIL"],
            Col.selection_priority: [1],
            Col.is_dialsup: [0],
            Col.is_euws: [0],
            Col.cds_cat_class_name: ["Wind"],
            Col.class_: ["COMM"],
            Col.office: ["DE"],
            Col.currency: ["EUR"],
            Col.model_code: [1],
            Col.year_id: [2026],
            Col.event_id: [1],
            Col.rnk: [1],
            Col.rp: [1.0],
        }
    ).lazy()

    blended_ylt = int_ylt_main_blended.Model.transform(ylt, targets).collect()

    assert Col.risklink_blended_contribution in blended_ylt.columns
    assert Col.verisk_blended_contribution in blended_ylt.columns
    assert Col.uplift_factor_on_base_model in blended_ylt.columns
    assert blended_ylt.item(0, Col.risklink_blended_contribution) == 150.0
    assert blended_ylt.item(0, Col.verisk_blended_contribution) == 25.0
    assert blended_ylt.item(0, Col.uplift_factor_on_base_model) == 0.875
    assert blended_ylt.item(0, Col.loss) == 8.75


def test_rank_ylt_deterministically_breaks_loss_ties() -> None:
    frame = pl.DataFrame(
        {
            Col.vendor: ["verisk", "verisk", "verisk", "verisk"],
            Col.modelled_lob: ["LOB", "LOB", "LOB", "LOB"],
            Col.rollup_peril: ["PERIL", "PERIL", "PERIL", "PERIL"],
            Col.loss: [100.0, 100.0, 100.0, 200.0],
            Col.year_id: [2026, 2025, 2025, 2026],
            Col.event_id: [2, 3, 1, 9],
            Col.analysis_id: ["b", "a", "c", "z"],
            Col.model_code: [2, 1, 1, 9],
            Col.modelled_peril: ["PERIL"] * 4,
            Col.rollup_lob: ["LOB"] * 4,
            Col.region_peril_id: [1] * 4,
            Col.blend_subregion_peril_id: ["1"] * 4,
            Col.base_model: ["verisk"] * 4,
            Col.selection_priority: [1] * 4,
            Col.is_dialsup: [0] * 4,
            Col.is_euws: [0] * 4,
            Col.cds_cat_class_name: ["Wind"] * 4,
            Col.class_: ["COMM"] * 4,
            Col.office: ["DE"] * 4,
            Col.currency: ["EUR"] * 4,
            Col.metric: ["original"] * 4,
        }
    )

    ranked = (
        int_ylt_ranked.Model.transform(frame.lazy(), RollupConfig())
        .collect()
        .sort(Col.rnk)
    )

    assert ranked.select(Col.loss, Col.year_id, Col.event_id, Col.rnk).rows() == [
        (200.0, 2026, 9, 1),
        (100.0, 2025, 1, 2),
        (100.0, 2025, 3, 3),
        (100.0, 2026, 2, 4),
    ]


def test_rank_ylt_tie_ranks_are_stable_for_shuffled_input() -> None:
    rows = {
        Col.vendor: ["verisk", "verisk", "verisk", "verisk"],
        Col.modelled_lob: ["LOB", "LOB", "LOB", "LOB"],
        Col.rollup_peril: ["PERIL", "PERIL", "PERIL", "PERIL"],
        Col.loss: [100.0, 100.0, 100.0, 200.0],
        Col.year_id: [2026, 2025, 2025, 2026],
        Col.event_id: [2, 3, 1, 9],
        Col.analysis_id: ["b", "a", "c", "z"],
        Col.model_code: [2, 1, 1, 9],
        Col.modelled_peril: ["PERIL"] * 4,
        Col.rollup_lob: ["LOB"] * 4,
        Col.region_peril_id: [1] * 4,
        Col.blend_subregion_peril_id: ["1"] * 4,
        Col.base_model: ["verisk"] * 4,
        Col.selection_priority: [1] * 4,
        Col.is_dialsup: [0] * 4,
        Col.is_euws: [0] * 4,
        Col.cds_cat_class_name: ["Wind"] * 4,
        Col.class_: ["COMM"] * 4,
        Col.office: ["DE"] * 4,
        Col.currency: ["EUR"] * 4,
        Col.metric: ["original"] * 4,
    }
    expected = (
        int_ylt_ranked.Model.transform(pl.DataFrame(rows).lazy(), RollupConfig())
        .collect()
        .select(Col.year_id, Col.event_id, Col.rnk)
        .sort(Col.year_id, Col.event_id)
    )
    shuffled = pl.DataFrame(rows).sample(fraction=1.0, shuffle=True, seed=7)

    actual = (
        int_ylt_ranked.Model.transform(shuffled.lazy(), RollupConfig())
        .collect()
        .select(Col.year_id, Col.event_id, Col.rnk)
        .sort(Col.year_id, Col.event_id)
    )

    assert actual.rows() == expected.rows()
