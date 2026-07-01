from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from rollup.columns import Col, FanoutCol
from rollup.config import RollupConfig
from rollup.intermediate.build_dialsup import dialsup_metric
from rollup.metrics import final_main_metric, forecast_metric, metric_specs
from rollup.marts.fanouts import (
    INTERNAL_FANOUT_SOURCE_FILE,
    validate_cds_fanout_frame,
    write_fanouts,
)
from rollup.marts.wide import wide_column_name
from rollup.marts.write_marts import write_marts


ORIGINAL_METRIC = metric_specs("GBP")[0].name
FINAL_MAIN_METRIC = final_main_metric("GBP")
DIALSUP_METRIC = dialsup_metric("GBP")
FORECAST_METRIC = forecast_metric("GBP")


def test_write_marts_streams_large_outputs_and_writes_operational_final_marts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    combined = internal_combined_metric_frame().lazy()
    dialsup = internal_dialsup_metric_frame().lazy()
    main_fanout = combined_metric_frame().lazy()
    dialsup_fanout = dialsup_metric_frame().lazy()
    original_collect = pl.LazyFrame.collect

    def guarded_collect(self: pl.LazyFrame, *args: Any, **kwargs: Any) -> pl.DataFrame:
        column_names = set(self.collect_schema().names())
        if {Col.metric, Col.loss}.issubset(column_names):
            raise AssertionError("full metric lazy frames must not be collected during mart writes")
        return original_collect(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", guarded_collect)

    paths = write_marts(
        tmp_path,
        combined,
        dialsup,
        RollupConfig(),
        main_fanout=main_fanout,
        dialsup_fanout=dialsup_fanout,
    )
    monkeypatch.undo()

    combined_out = pl.read_parquet(paths["combined"])
    dialsup_out = pl.read_parquet(paths["dialsup"])
    wide_out = pl.read_parquet(paths["wide"])

    assert combined_out.height == 5
    assert dialsup_out.select(Col.metric).unique().to_series().to_list() == [DIALSUP_METRIC]
    assert dialsup_out.select(Col.event_id, Col.loss).rows() == [(1, 10.0)]
    forecast_column = wide_column_name(FORECAST_METRIC, "2026-01-01")
    final_column = wide_column_name(FINAL_MAIN_METRIC, "2026-01-01")
    original_column = wide_column_name(ORIGINAL_METRIC, "2026-01-01")
    dialsup_column = wide_column_name(DIALSUP_METRIC, "2026-01-01")
    assert Col.metric not in wide_out.columns
    assert Col.forecast_date not in wide_out.columns
    assert set(wide_out.columns) >= {forecast_column, final_column, original_column, Col.target_currency}
    assert dialsup_column not in wide_out.columns
    assert wide_out.sort(Col.event_id).select(
        Col.event_id,
        forecast_column,
        final_column,
        original_column,
    ).rows() == [(1, 10.0, 15.0, 5.0), (2, 20.0, 25.0, None)]
    for metric, column in [
        (FORECAST_METRIC, forecast_column),
        (FINAL_MAIN_METRIC, final_column),
        (ORIGINAL_METRIC, original_column),
    ]:
        combined_sum = combined_out.filter(
            (pl.col(Col.metric) == metric) & (pl.col(Col.forecast_date) == "2026-01-01")
        ).select(pl.col(Col.loss).sum()).item()
        wide_sum = wide_out.select(pl.col(column).sum()).item()
        assert wide_sum == combined_sum
    fanouts = paths["fanouts"]
    assert isinstance(fanouts, tuple)
    assert [path.name for path in fanouts] == [
        "HiscoAIR_20260101_main.parquet",
        "HiscoAIR_20260101_dialsup.parquet",
    ]
    assert not (tmp_path / "marts" / INTERNAL_FANOUT_SOURCE_FILE).exists()
    assert pl.read_parquet(fanouts[0]).columns == cds_fanout_columns()
    assert pl.read_parquet(fanouts[0]).sort(FanoutCol.ModelYear).rows() == [
        (101, 1, "GBP", 0, 15.0, 0, 10, "CDS Fine Art"),
        (102, 2, "GBP", 0, 25.0, 0, 20, "CDS Fine Art"),
    ]
    assert pl.read_parquet(fanouts[1]).columns == cds_fanout_columns()
    assert pl.read_parquet(fanouts[1]).rows() == [(101, 1, "GBP", 0, 10.0, 0, 10, "CDS Fine Art")]


def test_write_marts_outputs_match_expected_row_counts(tmp_path: Path) -> None:
    combined = internal_combined_metric_frame().lazy()

    paths = write_marts(
        tmp_path,
        combined,
        internal_dialsup_metric_frame().lazy(),
        RollupConfig(),
        main_fanout=combined_metric_frame().lazy(),
        dialsup_fanout=dialsup_metric_frame().lazy(),
    )

    row_counts = {
        name: pl.scan_parquet(path).select(pl.len()).collect().item()
        for name, path in paths.items()
        if isinstance(path, Path)
    }

    assert row_counts == {
        "combined": 5,
        "wide": 2,
        "dialsup": 1,
    }


def test_write_fanouts_rejects_unknown_base_model(tmp_path: Path) -> None:
    frame = combined_metric_frame().with_columns(
        pl.lit("katrisk").alias(Col.base_model)
    )

    with pytest.raises(ValueError, match="unsupported base model for fanout 'katrisk'"):
        write_fanouts(
            tmp_path,
            frame.lazy(),
            {"verisk": "HiscoAIR", "risklink": "HiscoRMS"},
            "GBP",
        )


def test_write_fanouts_defaults_to_main_suffix_and_metric(tmp_path: Path) -> None:
    fanouts = write_fanouts(
        tmp_path,
        combined_metric_frame().lazy(),
        {"verisk": "HiscoAIR", "risklink": "HiscoRMS"},
        "GBP",
    )

    assert [path.name for path in fanouts] == ["HiscoAIR_20260101_main.parquet"]
    assert not (tmp_path / INTERNAL_FANOUT_SOURCE_FILE).exists()
    output = pl.read_parquet(fanouts[0])
    assert output.columns == cds_fanout_columns()
    assert output.sort(FanoutCol.ModelYear).rows() == [
        (101, 1, "GBP", 0, 15.0, 0, 10, "CDS Fine Art"),
        (102, 2, "GBP", 0, 25.0, 0, 20, "CDS Fine Art"),
    ]


def test_write_fanouts_can_write_dialsup_suffix(tmp_path: Path) -> None:
    fanouts = write_fanouts(
        tmp_path,
        dialsup_metric_frame().lazy(),
        {"verisk": "HiscoAIR", "risklink": "HiscoRMS"},
        "GBP",
        suffix="dialsup",
    )

    assert [path.name for path in fanouts] == ["HiscoAIR_20260101_dialsup.parquet"]
    assert not (tmp_path / INTERNAL_FANOUT_SOURCE_FILE).exists()
    output = pl.read_parquet(fanouts[0])
    assert output.columns == cds_fanout_columns()
    assert output.rows() == [(101, 1, "GBP", 0, 10.0, 0, 10, "CDS Fine Art")]


def test_write_fanouts_uses_risklink_event_id_and_flood_event_day(tmp_path: Path) -> None:
    frame = combined_metric_frame().filter(pl.col(Col.metric) == FINAL_MAIN_METRIC).with_columns(
        pl.lit("risklink").alias(Col.base_model),
        pl.lit(216).alias(Col.region_peril_id),
        pl.lit(None).cast(pl.Int64).alias(Col.model_event_id),
        pl.lit(None).cast(pl.Int64).alias(Col.event_day),
    )
    risklink_events = pl.DataFrame(
        {
            Col.event_id: [1, 2],
            Col.region_peril_id: [216, 216],
            Col.risklink_event_day: [42, 84],
        }
    )

    fanouts = write_fanouts(
        tmp_path,
        frame.lazy(),
        {"verisk": "HiscoAIR", "risklink": "HiscoRMS"},
        "GBP",
        risklink_flood_events=risklink_events.lazy(),
    )

    assert [path.name for path in fanouts] == ["HiscoRMS_20260101_main.parquet"]
    output = pl.read_parquet(fanouts[0])
    assert output.columns == cds_fanout_columns()
    assert output.sort(FanoutCol.ModelYear).rows() == [
        (1, 1, "GBP", 0, 15.0, 0, 42, "CDS Fine Art"),
        (2, 2, "GBP", 0, 25.0, 0, 84, "CDS Fine Art"),
    ]


def test_write_fanouts_validates_air_events_against_verisk_catalogue(tmp_path: Path) -> None:
    fanouts = write_fanouts(
        tmp_path,
        combined_metric_frame().lazy(),
        {"verisk": "HiscoAIR", "risklink": "HiscoRMS"},
        "GBP",
        verisk_events=verisk_events_frame().lazy(),
    )

    assert [path.name for path in fanouts] == ["HiscoAIR_20260101_main.parquet"]


def test_write_fanouts_rejects_air_event_catalogue_mismatch(tmp_path: Path) -> None:
    verisk_events = verisk_events_frame().with_columns(
        pl.when(pl.col(Col.model_event_id) == 102)
        .then(999)
        .otherwise(pl.col(Col.event_day))
        .alias(Col.event_day)
    )

    with pytest.raises(ValueError, match="Verisk fanout event validation failed: 1 mismatch"):
        write_fanouts(
            tmp_path,
            combined_metric_frame().lazy(),
            {"verisk": "HiscoAIR", "risklink": "HiscoRMS"},
            "GBP",
            verisk_events=verisk_events.lazy(),
        )

    assert not (tmp_path / INTERNAL_FANOUT_SOURCE_FILE).exists()


def test_write_fanouts_rejects_risklink_event_catalogue_mismatch(tmp_path: Path) -> None:
    frame = combined_metric_frame().filter(pl.col(Col.metric) == FINAL_MAIN_METRIC).with_columns(
        pl.lit("risklink").alias(Col.base_model),
        pl.lit(216).alias(Col.region_peril_id),
        pl.lit(None).cast(pl.Int64).alias(Col.model_event_id),
        pl.lit(None).cast(pl.Int64).alias(Col.event_day),
    )
    risklink_events = pl.DataFrame(
        {
            Col.event_id: [1],
            Col.region_peril_id: [999],
            Col.risklink_event_day: [42],
        }
    )

    with pytest.raises(ValueError, match="RiskLink fanout event validation failed: 2 mismatch"):
        write_fanouts(
            tmp_path,
            frame.lazy(),
            {"verisk": "HiscoAIR", "risklink": "HiscoRMS"},
            "GBP",
            risklink_flood_events=risklink_events.lazy(),
        )

    assert not (tmp_path / INTERNAL_FANOUT_SOURCE_FILE).exists()


def test_cds_fanout_validation_rejects_schema_mismatch() -> None:
    bad = pl.DataFrame({FanoutCol.ModelEventID: [101]})

    with pytest.raises(ValueError, match="CDS fanout schema mismatch"):
        validate_cds_fanout_frame(bad.lazy())


def test_write_fanouts_rejects_required_nulls(tmp_path: Path) -> None:
    frame = combined_metric_frame().with_columns(pl.lit(None).alias(Col.target_currency))

    with pytest.raises(ValueError, match="CDS fanout required field nulls: CurrencyCode=2"):
        write_fanouts(
            tmp_path,
            frame.lazy(),
            {"verisk": "HiscoAIR", "risklink": "HiscoRMS"},
            "GBP",
        )

    assert not (tmp_path / INTERNAL_FANOUT_SOURCE_FILE).exists()


def combined_metric_frame() -> pl.DataFrame:
    base = {
        Col.vendor: "verisk",
        Col.base_model: "verisk",
        Col.analysis_id: "EQ",
        Col.modelled_lob: "Fine Art",
        Col.modelled_peril: "EQ",
        Col.rollup_lob: "Fine Art",
        Col.rollup_peril: "Earthquake",
        Col.region_peril_id: 205,
        Col.cds_cat_class_name: "CDS Fine Art",
        Col.class_: "ART",
        Col.office: "London",
        Col.currency: "GBP",
        Col.target_currency: "GBP",
        Col.forecast_date: "2026-01-01",
    }
    rows = [
        {**base, Col.year_id: 1, Col.event_id: 1, Col.model_event_id: 101, Col.event_day: 10, Col.is_dialsup: 1, Col.metric: FORECAST_METRIC, Col.loss: 10.0},
        {**base, Col.year_id: 1, Col.event_id: 1, Col.model_event_id: 101, Col.event_day: 10, Col.is_dialsup: 1, Col.metric: FINAL_MAIN_METRIC, Col.loss: 15.0},
        {**base, Col.year_id: 1, Col.event_id: 1, Col.model_event_id: 101, Col.event_day: 10, Col.is_dialsup: 1, Col.metric: ORIGINAL_METRIC, Col.loss: 5.0},
        {**base, Col.year_id: 2, Col.event_id: 2, Col.model_event_id: 102, Col.event_day: 20, Col.is_dialsup: 0, Col.metric: FORECAST_METRIC, Col.loss: 20.0},
        {**base, Col.year_id: 2, Col.event_id: 2, Col.model_event_id: 102, Col.event_day: 20, Col.is_dialsup: 0, Col.metric: FINAL_MAIN_METRIC, Col.loss: 25.0},
    ]
    return pl.DataFrame(rows)


def dialsup_metric_frame() -> pl.DataFrame:
    base = {
        Col.vendor: "verisk",
        Col.base_model: "verisk",
        Col.analysis_id: "EQ",
        Col.modelled_lob: "Fine Art",
        Col.modelled_peril: "EQ",
        Col.rollup_lob: "Fine Art",
        Col.rollup_peril: "Earthquake",
        Col.region_peril_id: 205,
        Col.cds_cat_class_name: "CDS Fine Art",
        Col.class_: "ART",
        Col.office: "London",
        Col.currency: "GBP",
        Col.target_currency: "GBP",
        Col.year_id: 1,
        Col.event_id: 1,
        Col.model_event_id: 101,
        Col.event_day: 10,
        Col.forecast_date: "2026-01-01",
        Col.is_dialsup: 1,
        Col.metric: DIALSUP_METRIC,
        Col.loss: 10.0,
    }
    return pl.DataFrame([base])


def verisk_events_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            Col.model_event_id: [101, 102],
            Col.model_code: [1, 1],
            Col.event_id: [1, 2],
            Col.year_id: [1, 2],
            Col.event_day: [10, 20],
        }
    )


def internal_combined_metric_frame() -> pl.DataFrame:
    return combined_metric_frame().drop(
        Col.cds_cat_class_name,
        Col.model_event_id,
        Col.event_day,
    )


def internal_dialsup_metric_frame() -> pl.DataFrame:
    return dialsup_metric_frame().drop(
        Col.cds_cat_class_name,
        Col.model_event_id,
        Col.event_day,
    )


def cds_fanout_columns() -> list[str]:
    return [
        FanoutCol.ModelEventID,
        FanoutCol.ModelYear,
        FanoutCol.CurrencyCode,
        FanoutCol.ModelYOA,
        FanoutCol.ModelGrossLoss,
        FanoutCol.ModelInwardsReinstatement,
        FanoutCol.ModelEventDay,
        FanoutCol.LossClassName,
    ]
