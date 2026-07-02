from __future__ import annotations

import polars as pl

from rollup.columns import Col, FanoutCol
from rollup.pipeline import (
    apply_event_loss_threshold,
    build_dialsup_fanout,
    build_main_fanout,
    enrich_risklink_event_days,
)


def test_risklink_event_enrichment_drops_unmatched_risklink_and_preserves_verisk() -> None:
    ylt = pl.DataFrame(
        {
            Col.base_model: ["risklink", "risklink", "verisk"],
            Col.event_id: [100, 100, 200],
            Col.year_id: [2025, 2026, 2025],
            Col.region_peril_id: [70, 70, 80],
            Col.event_day: [None, None, 12],
            Col.model_event_id: [None, None, 999],
            Col.forecast_date: ["2026-01-01", "2026-01-01", "2026-01-01"],
            Col.metric: ["euws_override", "euws_override", "euws_override"],
            Col.target_currency: ["GBP", "GBP", "GBP"],
            Col.loss: [1.0, 2.0, 3.0],
            Col.cds_cat_class_name: ["Flood", "Flood", "Wind"],
        }
    ).lazy()
    catalogue = pl.DataFrame(
        {
            Col.event_id: [100],
            Col.model_occurrence_year: [2025],
            Col.region_peril_id: [70],
            Col.risklink_event_day: [42],
        }
    ).lazy()

    result = enrich_risklink_event_days(ylt, catalogue).collect().sort(Col.base_model)

    assert result.get_column(Col.base_model).to_list() == ["risklink", "verisk"]
    assert result.filter(pl.col(Col.base_model) == "risklink").get_column(Col.risklink_event_day).to_list() == [42]


def test_cds_fanout_uses_historical_columns_after_risklink_year_join() -> None:
    ylt = pl.DataFrame(
        {
            Col.base_model: ["risklink", "verisk"],
            Col.event_id: [100, 200],
            Col.year_id: [2025, 2025],
            Col.region_peril_id: [70, 80],
            Col.event_day: [None, 12],
            Col.model_event_id: [None, 999],
            Col.forecast_date: ["2026-01-01", "2026-01-01"],
            Col.metric: ["euws_override", "euws_override"],
            Col.target_currency: ["GBP", "GBP"],
            Col.loss: [1.0, 3.0],
            Col.cds_cat_class_name: ["Flood", "Wind"],
        }
    ).lazy()
    catalogue = pl.DataFrame(
        {
            Col.event_id: [100],
            Col.model_occurrence_year: [2025],
            Col.region_peril_id: [70],
            Col.risklink_event_day: [42],
        }
    ).lazy()

    fanout = build_main_fanout(ylt, catalogue).collect()

    assert fanout.columns == [
        Col.forecast_date,
        Col.base_model,
        Col.metric,
        FanoutCol.ModelEventID,
        FanoutCol.ModelYear,
        FanoutCol.CurrencyCode,
        FanoutCol.ModelYOA,
        FanoutCol.ModelGrossLoss,
        FanoutCol.ModelInwardsReinstatement,
        FanoutCol.ModelEventDay,
        FanoutCol.LossClassName,
    ]
    assert fanout.filter(pl.col(Col.base_model) == "risklink").get_column(FanoutCol.ModelEventDay).to_list() == [42]


def test_event_loss_threshold_filters_only_final_metric_rows() -> None:
    ylt = pl.DataFrame(
        {
            Col.metric: ["original", "euws_override", "euws_override", "euws_override"],
            Col.loss: [None, None, 999.0, 1000.0],
        }
    )

    high_threshold = apply_event_loss_threshold(ylt, metric="euws_override", threshold=1000.0)
    non_positive_threshold = apply_event_loss_threshold(ylt, metric="euws_override", threshold=0.0)

    assert high_threshold.get_column(Col.loss).to_list() == [None, 1000.0]
    assert non_positive_threshold.get_column(Col.loss).to_list() == [None, 999.0, 1000.0]


def test_thresholded_main_and_dialsup_rows_drive_fanouts() -> None:
    ylt = pl.DataFrame(
        {
            Col.base_model: ["verisk", "verisk", "verisk"],
            Col.event_id: [100, 200, 300],
            Col.year_id: [2025, 2025, 2025],
            Col.region_peril_id: [70, 70, 70],
            Col.event_day: [1, 2, 3],
            Col.model_event_id: [1000, 2000, 3000],
            Col.forecast_date: ["2026-01-01", "2026-01-01", "2026-01-01"],
            Col.metric: ["euws_override", "euws_override", "dialsup_gbp_forecast"],
            Col.target_currency: ["GBP", "GBP", "GBP"],
            Col.loss: [999.0, 1000.0, 1000.0],
            Col.cds_cat_class_name: ["Wind", "Wind", "Wind"],
        }
    )
    risklink_events = pl.DataFrame(
        schema={
            Col.event_id: pl.Int64,
            Col.model_occurrence_year: pl.Int64,
            Col.region_peril_id: pl.Int64,
            Col.risklink_event_day: pl.Int64,
        }
    ).lazy()

    main = apply_event_loss_threshold(
        ylt.filter(pl.col(Col.metric) == "euws_override"),
        metric="euws_override",
        threshold=1000.0,
    )
    dialsup = apply_event_loss_threshold(
        ylt.filter(pl.col(Col.metric) == "dialsup_gbp_forecast"),
        metric="dialsup_gbp_forecast",
        threshold=1000.0,
    )
    main_fanout = build_main_fanout(main.lazy(), risklink_events).collect()
    dialsup_fanout = build_dialsup_fanout(dialsup.lazy(), risklink_events).collect()

    assert main_fanout.get_column(FanoutCol.ModelGrossLoss).to_list() == [1000.0]
    assert dialsup_fanout.get_column(FanoutCol.ModelGrossLoss).to_list() == [1000.0]
