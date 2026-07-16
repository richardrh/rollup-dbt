from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from rollup.columns import Col, RawCol
from rollup.intermediate import (
    int_forecast_dates,
    int_ylt_dialsup_factor_base,
    int_ylt_main_blended,
    int_ylt_main_euws,
    int_ylt_main_euws_override,
    int_ylt_main_forecast,
    int_ylt_main_local_currency,
    int_ylt_normalized,
    int_ylt_ranked,
)
from rollup.marts import mart_main_fanout, mart_ylt_main_long
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
    staged = stg_forecast_factors.transform(
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
    staged = stg_forecast_factors.transform(
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


def test_forecast_dates_model_requires_date_like_forecast_date() -> None:
    with pytest.raises(ValueError, match="forecast_date.*date_like"):
        int_forecast_dates.validate(
            pl.DataFrame({Col.forecast_date: ["2026-01-01"]}).lazy()
        )


def test_main_forecast_model_requires_date_like_forecast_inputs() -> None:
    ylt = pl.DataFrame(
        {Col.class_: ["COMM"], Col.office: ["DE"], Col.loss: [1.0]}
    ).lazy()
    forecast_dates = pl.DataFrame({Col.forecast_date: ["2026-01-01"]}).lazy()
    forecast_factors = pl.DataFrame(
        {
            Col.class_: ["COMM"],
            Col.office: ["DE"],
            Col.forecast_date: ["2026-01-01"],
            "_forecast_factor_raw": [1.0],
        }
    ).lazy()

    with pytest.raises(ValueError, match="forecast_date.*date_like"):
        int_ylt_main_forecast.validate(ylt, forecast_dates, forecast_factors)


def test_dialsup_factor_base_model_requires_date_like_forecast_inputs() -> None:
    ylt = pl.DataFrame(
        {
            Col.event_id: [1],
            Col.year_id: [2026],
            Col.model_code: [41],
            Col.currency: ["EUR"],
            Col.class_: ["COMM"],
            Col.office: ["DE"],
        }
    ).lazy()
    verisk_events = pl.DataFrame(
        {Col.event_id: [1], Col.year_id: [2026], Col.model_code: [41]}
    ).lazy()
    fx_rates = pl.DataFrame({Col.currency: ["EUR"], Col.fx_rate: [0.88]}).lazy()
    forecast_dates = pl.DataFrame({Col.forecast_date: ["2026-01-01"]}).lazy()
    forecast_factors = pl.DataFrame(
        {
            Col.class_: ["COMM"],
            Col.office: ["DE"],
            Col.forecast_date: ["2026-01-01"],
            "_forecast_factor_raw": [1.0],
        }
    ).lazy()

    with pytest.raises(ValueError, match="forecast_date.*date_like"):
        int_ylt_dialsup_factor_base.validate(
            ylt, verisk_events, fx_rates, forecast_dates, forecast_factors
        )


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

    normalized = int_ylt_normalized.transform(
        stg_verisk_ylt.transform(verisk),
        stg_risklink_ylt.transform(_empty_risklink_ylt()),
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
            Col.class_: ["COMM"],
            Col.office: ["DE"],
            Col.currency: ["EUR"],
            Col.model_code: [41],
            Col.year_id: [2026],
            Col.event_id: [10],
            Col.rnk: [50],
            Col.rp: [1.0],
            Col.rp_bucket: [0],
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

    fx_rates = stg_gbp_fx_rates.transform(
        pl.DataFrame(
            {
                RawCol.currency_code: ["EUR"],
                Col.target_currency: ["GBP"],
                RawCol.rate_date: ["2026-01-01"],
                RawCol.rate: [0.88],
            }
        ).lazy()
    )
    forecast_factors = stg_forecast_factors.transform(
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
    forecast_dates = int_forecast_dates.transform(forecast_factors)
    ylt_blended = int_ylt_main_blended.transform(ylt_ranked, ep_blending_targets)
    ylt_localccy = int_ylt_main_local_currency.transform(ylt_blended, fx_rates)
    ylt_localccy_forecast = int_ylt_main_forecast.transform(
        ylt_localccy, forecast_dates, forecast_factors
    )
    ylt_euws = int_ylt_main_euws.transform(ylt_localccy_forecast, verisk_events, seeds)
    ylt_euws_override = int_ylt_main_euws_override.transform(ylt_euws, seeds)
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
    forecast_factors = stg_forecast_factors.transform(
        pl.DataFrame(
            {
                Col.class_: ["COMM"],
                "office_iso2": ["DE"],
                Col.forecast_date: ["2026-01-01"],
                RawCol.factor: [2.0],
            }
        ).lazy()
    )
    forecast_dates = int_forecast_dates.transform(forecast_factors)
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
    risklink_events = pl.DataFrame(
        schema={
            Col.event_id: pl.Int64,
            Col.model_occurrence_year: pl.Int64,
            Col.region_peril_id: pl.Int64,
            Col.risklink_event_day: pl.Int64,
        }
    ).lazy()

    ylt_forecast = int_ylt_main_forecast.transform(
        ylt_localccy, forecast_dates, forecast_factors
    ).with_columns(pl.lit("euws_override").alias(Col.metric))
    main_long = mart_ylt_main_long.transform(ylt_forecast, 0.0)
    fanout = mart_main_fanout.transform(main_long, risklink_events)

    assert fanout.collect_schema()[Col.forecast_date] == pl.Date
    assert fanout.select(Col.forecast_date).collect().item() == date(2026, 1, 1)


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
        }
    ).lazy()

    blended_ylt = int_ylt_main_blended.transform(ylt, targets).collect()

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
        }
    )

    ranked = int_ylt_ranked.transform(frame.lazy()).collect().sort(Col.rnk)

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
    }
    expected = (
        int_ylt_ranked.transform(pl.DataFrame(rows).lazy())
        .collect()
        .select(Col.year_id, Col.event_id, Col.rnk)
        .sort(Col.year_id, Col.event_id)
    )
    shuffled = pl.DataFrame(rows).sample(fraction=1.0, shuffle=True, seed=7)

    actual = (
        int_ylt_ranked.transform(shuffled.lazy())
        .collect()
        .select(Col.year_id, Col.event_id, Col.rnk)
        .sort(Col.year_id, Col.event_id)
    )

    assert actual.rows() == expected.rows()
