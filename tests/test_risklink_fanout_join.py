from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from rollup.columns import Col, FanoutCol
from rollup.marts import (
    mart_event_validation,
    mart_ylt_dialsup_long,
    mart_ylt_main_long,
)
from rollup.marts._fanout_helpers import build_fanout, enrich_risklink_event_days
from rollup.output_contract import EVENT_VALIDATION_FILE, MARTS_DIR
from rollup.writers import fanout_partitions, parquet


def test_risklink_event_enrichment_drops_unmatched_risklink_and_preserves_verisk() -> (
    None
):
    ylt = pl.DataFrame(
        {
            Col.base_model: ["risklink", "risklink", "verisk"],
            Col.event_id: [100, 100, 200],
            Col.year_id: [2025, 2026, 2025],
            Col.region_peril_id: [70, 70, 80],
            Col.event_day: [None, None, 12],
            Col.model_event_id: [None, None, 999],
            Col.forecast_date: [
                date(2026, 1, 1),
                date(2026, 1, 1),
                date(2026, 1, 1),
            ],
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
    assert result.filter(pl.col(Col.base_model) == "risklink").get_column(
        Col.risklink_event_day
    ).to_list() == [42]


def test_cds_fanout_uses_historical_columns_after_risklink_year_join() -> None:
    ylt = pl.DataFrame(
        {
            Col.base_model: ["risklink", "verisk"],
            Col.event_id: [100, 200],
            Col.year_id: [2025, 2025],
            Col.region_peril_id: [70, 80],
            Col.event_day: [None, 12],
            Col.model_event_id: [None, 999],
            Col.forecast_date: [date(2026, 1, 1), date(2026, 1, 1)],
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

    fanout = build_fanout(ylt, catalogue).collect()

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
    assert fanout.filter(pl.col(Col.base_model) == "risklink").get_column(
        FanoutCol.ModelEventDay
    ).to_list() == [42]


def test_inline_event_loss_threshold_filters_only_final_metric_rows() -> None:
    ylt = pl.DataFrame(
        {
            Col.base_model: ["verisk", "verisk", "verisk", "verisk"],
            Col.metric: ["original", "euws_override", "euws_override", "euws_override"],
            Col.loss: [None, None, 999.0, 1000.0],
        }
    )

    high_threshold_value = 1000.0
    high_threshold = mart_ylt_main_long.transform(
        ylt.lazy(), high_threshold_value
    ).collect()
    non_positive_threshold_value = 0.0
    non_positive_threshold = mart_ylt_main_long.transform(
        ylt.lazy(), non_positive_threshold_value
    ).collect()

    assert high_threshold.get_column(Col.loss).to_list() == [None, 1000.0]
    assert non_positive_threshold.get_column(Col.loss).to_list() == [
        None,
        999.0,
        1000.0,
    ]


def test_thresholded_main_and_dialsup_rows_drive_fanouts() -> None:
    ylt = pl.DataFrame(
        {
            Col.base_model: ["verisk", "verisk", "verisk"],
            Col.event_id: [100, 200, 300],
            Col.year_id: [2025, 2025, 2025],
            Col.region_peril_id: [70, 70, 70],
            Col.event_day: [1, 2, 3],
            Col.model_event_id: [1000, 2000, 3000],
            Col.forecast_date: [
                date(2026, 1, 1),
                date(2026, 1, 1),
                date(2026, 1, 1),
            ],
            Col.metric: ["euws_override", "euws_override", "dialsup_localccy_forecast"],
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

    threshold = 1000.0
    main = (
        mart_ylt_main_long.transform(ylt.lazy(), threshold)
        .filter(pl.col(Col.metric) == "euws_override")
        .collect()
    )
    dialsup = (
        mart_ylt_dialsup_long.transform(ylt.lazy(), threshold)
        .filter(pl.col(Col.metric) == "dialsup_localccy_forecast")
        .collect()
    )
    main_fanout = build_fanout(main.lazy(), risklink_events).collect()
    dialsup_fanout = build_fanout(dialsup.lazy(), risklink_events).collect()

    assert main_fanout.get_column(FanoutCol.ModelGrossLoss).to_list() == [1000.0]
    assert dialsup_fanout.get_column(FanoutCol.ModelGrossLoss).to_list() == [1000.0]


def test_fanout_writer_lazily_writes_fanout_partitions(tmp_path) -> None:
    fanout = pl.DataFrame(
        {
            Col.forecast_date: [date(2026, 1, 1), date(2026, 1, 1), date(2026, 2, 1)],
            Col.base_model: ["verisk", "verisk", "risklink"],
            Col.metric: ["euws_override", "euws_override", "dialsup_localccy_forecast"],
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
    parquet.write(
        mart_event_validation.transform(fanout, fanout.filter(pl.lit(False))),
        tmp_path / EVENT_VALIDATION_FILE,
    )
    fanout_partitions.write({"main_fanout": fanout}, tmp_path / MARTS_DIR)

    assert sorted(path.name for path in (tmp_path / MARTS_DIR).glob("*.parquet")) == [
        "HiscoAIR_202601_euws_override.parquet",
        "HiscoRMS_202602_dialsup_localccy_forecast.parquet",
    ]
    assert not (tmp_path / MARTS_DIR / "main_fanout.parquet").exists()
    assert not (tmp_path / MARTS_DIR / "dialsup_fanout.parquet").exists()

    air = pl.read_parquet(tmp_path / "marts" / "HiscoAIR_202601_euws_override.parquet")
    rms = pl.read_parquet(
        tmp_path / "marts" / "HiscoRMS_202602_dialsup_localccy_forecast.parquet"
    )
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
        ("verisk", "euws_override", date(2026, 1, 1), 2, 0, 1),
        ("risklink", "dialsup_localccy_forecast", date(2026, 2, 1), 1, 0, 0),
    ]


def test_fanout_writer_materializes_temp_files_under_output_filesystem(
    tmp_path, monkeypatch
) -> None:
    fanout = pl.DataFrame(
        {
            Col.forecast_date: [date(2026, 1, 1)],
            Col.base_model: ["verisk"],
            Col.metric: ["euws_override"],
            FanoutCol.ModelEventID: [100],
            FanoutCol.ModelYear: [2025],
            FanoutCol.CurrencyCode: ["GBP"],
            FanoutCol.ModelYOA: [2025],
            FanoutCol.ModelGrossLoss: [10.0],
            FanoutCol.ModelInwardsReinstatement: [0.0],
            FanoutCol.ModelEventDay: [1],
            FanoutCol.LossClassName: ["Wind"],
        }
    ).lazy()
    output_dir = tmp_path / MARTS_DIR
    observed_dirs = []
    real_temporary_directory = fanout_partitions.tempfile.TemporaryDirectory

    def recording_temporary_directory(*args, **kwargs):
        observed_dirs.append(kwargs.get("dir"))
        return real_temporary_directory(*args, **kwargs)

    monkeypatch.setattr(
        fanout_partitions.tempfile,
        "TemporaryDirectory",
        recording_temporary_directory,
    )

    fanout_partitions.write({"main_fanout": fanout}, output_dir)

    assert observed_dirs == [tmp_path]


def test_fanout_writer_preserves_existing_outputs_when_generation_fails(
    tmp_path,
) -> None:
    output_dir = tmp_path / MARTS_DIR
    output_dir.mkdir()
    existing = output_dir / "existing.parquet"
    pl.DataFrame({"sentinel": ["keep"]}).write_parquet(existing)
    fanout = pl.DataFrame(
        {
            Col.forecast_date: [date(2026, 1, 1)],
            Col.base_model: ["unknown"],
            Col.metric: ["euws_override"],
            FanoutCol.ModelEventID: [1],
            FanoutCol.ModelYear: [2026],
            FanoutCol.CurrencyCode: ["GBP"],
            FanoutCol.ModelYOA: [0],
            FanoutCol.ModelGrossLoss: [1.0],
            FanoutCol.ModelInwardsReinstatement: [0],
            FanoutCol.ModelEventDay: [1],
            FanoutCol.LossClassName: ["Wind"],
        }
    ).lazy()

    with pytest.raises(ValueError, match="unknown base model"):
        fanout_partitions.write({"main_fanout": fanout}, output_dir)

    assert pl.read_parquet(existing).to_dict(as_series=False) == {"sentinel": ["keep"]}
