from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from rollup.columns import Col
from rollup.config import RollupConfig
from rollup.intermediate.build_dialsup import dialsup_metric
from rollup.metrics import final_main_metric, forecast_metric, metric_specs
from rollup.marts.fanouts import write_fanouts
from rollup.marts.wide import wide_column_name
from rollup.marts.write_marts import write_marts


ORIGINAL_METRIC = metric_specs("GBP")[0].name
FINAL_MAIN_METRIC = final_main_metric("GBP")
DIALSUP_METRIC = dialsup_metric("GBP")
FORECAST_METRIC = forecast_metric("GBP")


def test_write_marts_streams_large_outputs_and_writes_operational_final_marts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    rollup_config: RollupConfig,
) -> None:
    combined = combined_metric_frame().lazy()
    original_collect = pl.LazyFrame.collect

    def guarded_collect(self: pl.LazyFrame, *args: Any, **kwargs: Any) -> pl.DataFrame:
        column_names = set(self.collect_schema().names())
        if {Col.metric, Col.loss}.issubset(column_names):
            raise AssertionError("full metric lazy frames must not be collected during mart writes")
        return original_collect(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", guarded_collect)

    paths = write_marts(
        tmp_path, combined, dialsup_metric_frame().lazy(), rollup_config
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
    assert [path.name for path in fanouts] == ["HiscoAIR_20260101_main.parquet"]
    assert pl.read_parquet(fanouts[0]).select(Col.metric).unique().to_series().to_list() == [
        FINAL_MAIN_METRIC
    ]


def test_write_marts_outputs_match_expected_row_counts(
    tmp_path: Path, rollup_config: RollupConfig
) -> None:
    combined = combined_metric_frame().lazy()

    paths = write_marts(
        tmp_path, combined, dialsup_metric_frame().lazy(), rollup_config
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
        Col.class_: "ART",
        Col.office: "London",
        Col.currency: "GBP",
        Col.target_currency: "GBP",
        Col.forecast_date: "2026-01-01",
    }
    rows = [
        {**base, Col.year_id: 1, Col.event_id: 1, Col.is_dialsup: 1, Col.metric: FORECAST_METRIC, Col.loss: 10.0},
        {**base, Col.year_id: 1, Col.event_id: 1, Col.is_dialsup: 1, Col.metric: FINAL_MAIN_METRIC, Col.loss: 15.0},
        {**base, Col.year_id: 1, Col.event_id: 1, Col.is_dialsup: 1, Col.metric: ORIGINAL_METRIC, Col.loss: 5.0},
        {**base, Col.year_id: 2, Col.event_id: 2, Col.is_dialsup: 0, Col.metric: FORECAST_METRIC, Col.loss: 20.0},
        {**base, Col.year_id: 2, Col.event_id: 2, Col.is_dialsup: 0, Col.metric: FINAL_MAIN_METRIC, Col.loss: 25.0},
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
        Col.class_: "ART",
        Col.office: "London",
        Col.currency: "GBP",
        Col.target_currency: "GBP",
        Col.year_id: 1,
        Col.event_id: 1,
        Col.forecast_date: "2026-01-01",
        Col.is_dialsup: 1,
        Col.metric: DIALSUP_METRIC,
        Col.loss: 10.0,
    }
    return pl.DataFrame([base])
