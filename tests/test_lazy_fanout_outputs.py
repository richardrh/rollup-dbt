from __future__ import annotations

import polars as pl

from rollup.columns import Col, FanoutCol
from rollup.pipeline import PipelineRunResult, PipelineStage, write_mart_outputs


def test_write_mart_outputs_lazily_writes_fanout_partitions(tmp_path) -> None:
    fanout = pl.DataFrame(
        {
            Col.forecast_date: ["2026-01-01", "2026-01-01", "2026-02-01"],
            Col.base_model: ["verisk", "verisk", "risklink"],
            Col.metric: ["euws_override", "euws_override", "dialsup_gbp_forecast"],
            FanoutCol.ModelEventID: [100, 200, 300],
            FanoutCol.ModelYear: [2025, 2025, 2026],
            FanoutCol.CurrencyCode: ["GBP", "GBP", "GBP"],
            FanoutCol.ModelYOA: [2025, 2025, 2026],
            FanoutCol.ModelGrossLoss: [10.0, 20.0, 30.0],
            FanoutCol.ModelInwardsReinstatement: [0.0, 0.0, 0.0],
            FanoutCol.ModelEventDay: [1, None, 3],
            FanoutCol.LossClassName: ["Wind", "Flood", "Quake"],
        }
    ).lazy()
    result = PipelineRunResult(
        seeds=PipelineStage({}),
        staging=PipelineStage({}),
        intermediate=PipelineStage({}),
        marts=PipelineStage({"main_fanout": fanout}),
    )

    write_mart_outputs(tmp_path, result)

    air = pl.read_parquet(tmp_path / "marts" / "HiscoAIR_202601_euws_override.parquet")
    rms = pl.read_parquet(tmp_path / "marts" / "HiscoRMS_202602_dialsup_gbp_forecast.parquet")
    validation = pl.read_parquet(tmp_path / "mts_event_validation.parquet").sort(
        Col.forecast_date, Col.base_model, Col.metric
    )

    assert air.columns == [
        FanoutCol.ModelEventID,
        FanoutCol.ModelYear,
        FanoutCol.CurrencyCode,
        FanoutCol.ModelYOA,
        FanoutCol.ModelGrossLoss,
        FanoutCol.ModelInwardsReinstatement,
        FanoutCol.ModelEventDay,
        FanoutCol.LossClassName,
    ]
    assert air.get_column(FanoutCol.ModelEventID).to_list() == [100, 200]
    assert rms.get_column(FanoutCol.ModelEventID).to_list() == [300]
    assert validation.select(
        Col.base_model,
        Col.metric,
        Col.forecast_date,
        Col.row_count,
        Col.missing_model_event_id,
        Col.missing_model_event_day,
    ).rows() == [
        ("verisk", "euws_override", "2026-01-01", 2, 0, 1),
        ("risklink", "dialsup_gbp_forecast", "2026-02-01", 1, 0, 0),
    ]
