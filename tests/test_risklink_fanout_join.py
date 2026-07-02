from __future__ import annotations

import polars as pl

from rollup.columns import Col, FanoutCol
from rollup.pipeline import build_main_fanout, enrich_risklink_event_days


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
