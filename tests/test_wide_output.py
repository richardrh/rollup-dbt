from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rollup.columns import Col
from rollup.marts import mart_ylt_dialsup_long, mart_ylt_main_long
from rollup.output_contract import (
    COMBINED_YLT_FILE,
    DIALSUP_YLT_FILE,
    WIDE_DIAGNOSTIC_COLUMNS,
    WIDE_IDENTITY_DIMENSIONS,
    WIDE_YLT_FILE,
)
from rollup.writers import parquet, wide_output


small_loss = st.floats(
    min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False
)


def _complete_wide_input(frame: pl.DataFrame) -> pl.DataFrame:
    defaults = {
        Col.analysis_id: "analysis",
        Col.modelled_lob: "LOB_A",
        Col.modelled_peril: "PERIL_A",
        Col.currency: "GBP",
        Col.blend_subregion_peril_id: "1",
        Col.class_: "CLASS",
        Col.office: "Office",
        Col.rnk: 1,
        Col.rp: 1.0,
        Col.rp_bucket: 1,
        Col.selection_priority: 1,
        Col.is_dialsup: 0,
        Col.is_euws: 1,
        Col.fx_rate_date: "2026-01-01",
        Col.fx_rate: 1.0,
        "_euws_factor_raw": 1.0,
        "_localccy_forecast_loss": 1.0,
    }
    required_columns = dict.fromkeys(
        [*WIDE_IDENTITY_DIMENSIONS, *WIDE_DIAGNOSTIC_COLUMNS, *defaults]
    )
    schema_types = {
        **mart_ylt_dialsup_long.schema(),
        **mart_ylt_main_long.schema(),
    }
    additions = [
        pl.lit(defaults.get(column)).cast(schema_types[column]).alias(column)
        for column in required_columns
        if column not in frame.columns
    ]
    return frame.with_columns(*additions) if additions else frame


def _write_wide_fixture_outputs(
    output_root: Path, ylt: pl.DataFrame, dialsup: pl.DataFrame
) -> None:
    main = mart_ylt_main_long.transform(_complete_wide_input(ylt).lazy(), 0.0)
    dialsup_long = mart_ylt_dialsup_long.transform(
        _complete_wide_input(dialsup).lazy(), 0.0
    )
    parquet.write(main, output_root / COMBINED_YLT_FILE)
    parquet.write(dialsup_long, output_root / DIALSUP_YLT_FILE)
    wide_output.write(
        output_root / COMBINED_YLT_FILE,
        output_root / DIALSUP_YLT_FILE,
        output_root / WIDE_YLT_FILE,
    )


def _expected_wide_columns(months: list[str]) -> list[str]:
    columns = [str(column) for column in WIDE_IDENTITY_DIMENSIONS]
    columns.insert(3, str(Col.output_use))
    columns.extend(str(column) for column in WIDE_DIAGNOSTIC_COLUMNS)
    columns.extend(f"euws_override_{month}_loss" for month in months)
    columns.extend(f"dialsup_localccy_forecast_{month}_loss" for month in months)
    return columns


def _persisted_wide_input_rows(
    metric: str, forecast_dates: list[date]
) -> dict[str, list[object]]:
    values: dict[str, object] = {
        Col.vendor: "verisk",
        Col.analysis_id: "analysis",
        Col.base_model: "verisk",
        Col.output_use: "cds_main" if metric == "euws_override" else "cds_dialsup",
        Col.model_code: 1,
        Col.year_id: 2026,
        Col.event_id: 10,
        Col.model_event_id: 1001,
        Col.event_day: 42,
        Col.rollup_lob: "LOB_A",
        Col.rollup_peril: "PERIL_A",
        Col.modelled_lob: "MODEL_LOB_A",
        Col.modelled_peril: "MODEL_PERIL_A",
        Col.region_peril_id: 7,
        Col.blend_subregion_peril_id: "7",
        Col.cds_cat_class_name: "Class",
        Col.class_: "CLASS",
        Col.office: "Office",
        Col.currency: "EUR",
        Col.target_currency: "GBP",
        Col.rnk: 1,
        Col.rp: 10.0,
        Col.rp_bucket: 10,
        Col.selection_priority: 1,
        Col.is_dialsup: 1,
        Col.is_euws: 1,
        Col.metric: metric,
        Col.loss: 100.0 if metric == "euws_override" else 50.0,
    }
    rows: dict[str, list[object]] = {
        column: [value] * len(forecast_dates) for column, value in values.items()
    }
    rows[str(Col.forecast_date)] = [value for value in forecast_dates]
    if metric == "euws_override":
        rows[Col.risklink_blended_contribution] = [75.0] * len(forecast_dates)
        rows[Col.verisk_blended_contribution] = [25.0] * len(forecast_dates)
        rows[Col.uplift_factor_on_base_model] = [1.25] * len(forecast_dates)
    return rows


def test_wide_output_orders_output_use_diagnostics_final_and_dialsup() -> None:
    ylt = pl.DataFrame(
        {
            Col.vendor: ["risklink"],
            Col.base_model: ["risklink"],
            Col.region_peril_id: [1],
            Col.rollup_peril: ["PERIL_A"],
            Col.rollup_lob: ["LOB_A"],
            Col.cds_cat_class_name: ["Class"],
            Col.model_code: [1],
            Col.year_id: [1],
            Col.event_id: [1],
            Col.model_event_id: [1],
            Col.event_day: [1],
            Col.target_currency: ["GBP"],
            Col.risklink_loss: [80.0],
            Col.verisk_loss: [20.0],
            Col.base_model_loss: [80.0],
            Col.target_loss: [100.0],
            Col.metric: ["euws_override"],
            Col.forecast_date: [date(2026, 1, 1)],
            Col.loss: [125.0],
            Col.risklink_blended_contribution: [75.0],
            Col.verisk_blended_contribution: [25.0],
            Col.uplift_factor_on_base_model: [1.25],
        }
    )
    dialsup = ylt.drop(
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    ).with_columns(pl.lit("dialsup_localccy_forecast").alias(Col.metric))

    with TemporaryDirectory() as temp_dir:
        _write_wide_fixture_outputs(Path(temp_dir), ylt, dialsup)
        wide = pl.read_parquet(Path(temp_dir) / WIDE_YLT_FILE)

    columns = wide.columns
    assert columns.count(Col.output_use) == 1
    assert columns.index(Col.output_use) < columns.index("euws_override_202601_loss")
    assert columns.index(Col.risklink_blended_contribution) < columns.index(
        "euws_override_202601_loss"
    )
    assert columns.index(Col.uplift_factor_on_base_model) < columns.index(
        "euws_override_202601_loss"
    )
    assert columns.index("euws_override_202601_loss") < columns.index(
        "dialsup_localccy_forecast_202601_loss"
    )
    assert columns[-1] == "dialsup_localccy_forecast_202601_loss"
    assert len(columns) == len(set(columns))
    assert wide.item(0, Col.output_use) == "cds_wide_analysis"


def test_wide_output_matches_exact_three_date_product_contract() -> None:
    forecast_dates = [date(2026, 1, 1), date(2026, 7, 1), date(2026, 12, 1)]

    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        main_path = root / COMBINED_YLT_FILE
        dialsup_path = root / DIALSUP_YLT_FILE
        output_path = root / WIDE_YLT_FILE
        pl.DataFrame(
            _persisted_wide_input_rows("euws_override", forecast_dates)
        ).write_parquet(main_path)
        pl.DataFrame(
            _persisted_wide_input_rows("dialsup_localccy_forecast", forecast_dates)
        ).write_parquet(dialsup_path)

        wide_output.validate(main_path, dialsup_path, output_path)
        wide_output.write(main_path, dialsup_path, output_path)
        wide = pl.read_parquet(output_path)

    assert wide.columns == _expected_wide_columns(["202601", "202607", "202612"])
    assert len(wide.columns) == 35
    assert wide.item(0, Col.output_use) == "cds_wide_analysis"
    assert wide.item(0, "euws_override_202601_loss") == 100.0
    assert wide.item(0, "dialsup_localccy_forecast_202612_loss") == 50.0


def test_wide_output_validate_requires_parquet_suffixes() -> None:
    with pytest.raises(ValueError, match="output_path must have a .parquet suffix"):
        wide_output.validate(
            Path("main.parquet"), Path("dialsup.parquet"), Path("wide.csv")
        )


def test_wide_output_rejects_duplicate_identity_metric_date_grain() -> None:
    forecast_dates = [date(2026, 1, 1), date(2026, 1, 1)]

    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        main_path = root / COMBINED_YLT_FILE
        dialsup_path = root / DIALSUP_YLT_FILE
        output_path = root / WIDE_YLT_FILE
        pl.DataFrame(
            _persisted_wide_input_rows("euws_override", forecast_dates)
        ).write_parquet(main_path)
        pl.DataFrame(
            _persisted_wide_input_rows("dialsup_localccy_forecast", [date(2026, 1, 1)])
        ).write_parquet(dialsup_path)

        with pytest.raises(RuntimeError, match="duplicate grain rows"):
            wide_output.write(main_path, dialsup_path, output_path)


def test_wide_output_preserves_existing_output_when_duplicate_validation_fails() -> (
    None
):
    forecast_dates = [date(2026, 1, 1), date(2026, 1, 1)]

    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        main_path = root / COMBINED_YLT_FILE
        dialsup_path = root / DIALSUP_YLT_FILE
        output_path = root / WIDE_YLT_FILE
        pl.DataFrame({"sentinel": ["keep"]}).write_parquet(output_path)
        pl.DataFrame(
            _persisted_wide_input_rows("euws_override", forecast_dates)
        ).write_parquet(main_path)
        pl.DataFrame(
            _persisted_wide_input_rows("dialsup_localccy_forecast", [date(2026, 1, 1)])
        ).write_parquet(dialsup_path)

        with pytest.raises(RuntimeError, match="duplicate grain rows"):
            wide_output.write(main_path, dialsup_path, output_path)

        assert pl.read_parquet(output_path).to_dict(as_series=False) == {
            "sentinel": ["keep"]
        }


def test_combined_outputs_label_output_use() -> None:
    ylt = pl.DataFrame(
        {
            Col.vendor: ["verisk", "verisk"],
            Col.base_model: ["verisk", "verisk"],
            Col.region_peril_id: [1, 1],
            Col.rollup_peril: ["PERIL_A", "PERIL_A"],
            Col.rollup_lob: ["LOB_A", "LOB_A"],
            Col.cds_cat_class_name: ["Class", "Class"],
            Col.model_code: [1, 1],
            Col.year_id: [1, 1],
            Col.event_id: [1, 1],
            Col.model_event_id: [1, 1],
            Col.event_day: [1, 1],
            Col.target_currency: ["GBP", "GBP"],
            Col.metric: ["original", "euws_override"],
            Col.forecast_date: [date(2026, 1, 1), date(2026, 1, 1)],
            Col.loss: [90.0, 100.0],
        }
    )
    dialsup = ylt.filter(pl.col(Col.metric) == "euws_override").with_columns(
        pl.lit("dialsup_localccy_forecast").alias(Col.metric)
    )

    with TemporaryDirectory() as temp_dir:
        _write_wide_fixture_outputs(Path(temp_dir), ylt, dialsup)
        main = pl.read_parquet(Path(temp_dir) / COMBINED_YLT_FILE)
        exported = pl.read_parquet(Path(temp_dir) / DIALSUP_YLT_FILE)
        wide = pl.read_parquet(Path(temp_dir) / WIDE_YLT_FILE)

    output_use_by_metric = dict(main.select(Col.metric, Col.output_use).iter_rows())
    assert output_use_by_metric == {
        "original": "intermediate_audit",
        "euws_override": "cds_main",
    }
    assert exported[Col.metric].unique().to_list() == ["dialsup_localccy_forecast"]
    assert exported[Col.output_use].unique().to_list() == ["cds_dialsup"]
    assert wide[Col.output_use].unique().to_list() == ["cds_wide_analysis"]
    assert wide.height == 1


@pytest.mark.fuzz
@settings(max_examples=20, deadline=None)
@given(losses=st.lists(small_loss, min_size=1, max_size=10))
def test_fuzz_wide_output_preserves_total_loss(losses: list[float]) -> None:
    ylt = pl.DataFrame(
        {
            Col.vendor: ["verisk"] * len(losses),
            Col.base_model: ["verisk"] * len(losses),
            Col.region_peril_id: [1] * len(losses),
            Col.rollup_peril: ["PERIL_A"] * len(losses),
            Col.rollup_lob: ["LOB_A"] * len(losses),
            Col.cds_cat_class_name: ["Class"] * len(losses),
            Col.model_code: [1] * len(losses),
            Col.year_id: list(range(1, len(losses) + 1)),
            Col.event_id: list(range(1, len(losses) + 1)),
            Col.model_event_id: list(range(1, len(losses) + 1)),
            Col.event_day: [1] * len(losses),
            Col.target_currency: ["GBP"] * len(losses),
            Col.metric: ["euws_override"] * len(losses),
            Col.forecast_date: [date(2026, 1, 1)] * len(losses),
            Col.loss: losses,
            Col.risklink_blended_contribution: [10.0] * len(losses),
            Col.verisk_blended_contribution: [20.0] * len(losses),
            Col.uplift_factor_on_base_model: [1.5] * len(losses),
        }
    )
    dialsup = ylt.drop(
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    ).with_columns(pl.lit("dialsup_localccy_forecast").alias(Col.metric))

    with TemporaryDirectory() as temp_dir:
        _write_wide_fixture_outputs(Path(temp_dir), ylt, dialsup)
        wide = pl.read_parquet(Path(temp_dir) / WIDE_YLT_FILE)

    loss_columns = [
        "euws_override_202601_loss",
        "dialsup_localccy_forecast_202601_loss",
    ]
    assert set(loss_columns).issubset(wide.columns)
    assert Col.risklink_blended_contribution in wide.columns
    assert Col.verisk_blended_contribution in wide.columns
    assert Col.uplift_factor_on_base_model in wide.columns
    assert wide.get_column(Col.output_use).unique().to_list() == ["cds_wide_analysis"]
    assert "rl_blended_contribution" not in wide.columns
    assert "vk_blended_contribution" not in wide.columns
    assert wide.select(pl.sum_horizontal(*loss_columns).sum()).item() == pytest.approx(
        sum(losses) * 2
    )


def test_wide_output_includes_main_blend_diagnostics_when_dialsup_lacks_them() -> None:
    ylt = pl.DataFrame(
        {
            Col.vendor: ["risklink"],
            Col.base_model: ["risklink"],
            Col.region_peril_id: [1],
            Col.rollup_peril: ["PERIL_A"],
            Col.rollup_lob: ["LOB_A"],
            Col.cds_cat_class_name: ["Class"],
            Col.model_code: [1],
            Col.year_id: [1],
            Col.event_id: [1],
            Col.model_event_id: [1],
            Col.event_day: [1],
            Col.target_currency: ["GBP"],
            Col.metric: ["euws_override"],
            Col.forecast_date: [date(2026, 1, 1)],
            Col.loss: [100.0],
            Col.risklink_blended_contribution: [75.0],
            Col.verisk_blended_contribution: [25.0],
            Col.uplift_factor_on_base_model: [2.0],
        }
    )
    dialsup = ylt.drop(
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    ).with_columns(pl.lit("dialsup_localccy_forecast").alias(Col.metric))

    with TemporaryDirectory() as temp_dir:
        _write_wide_fixture_outputs(Path(temp_dir), ylt, dialsup)
        wide = pl.read_parquet(Path(temp_dir) / WIDE_YLT_FILE)

    assert Col.risklink_blended_contribution in wide.columns
    assert Col.verisk_blended_contribution in wide.columns
    assert Col.uplift_factor_on_base_model in wide.columns
    assert wide.item(0, Col.output_use) == "cds_wide_analysis"
    assert "rl_blended_contribution" not in wide.columns
    assert "vk_blended_contribution" not in wide.columns
    assert wide.filter(pl.col(Col.risklink_blended_contribution) == 75.0).height == 1
    assert wide.height == 1
    assert wide.item(0, "euws_override_202601_loss") == 100.0
    assert wide.item(0, "dialsup_localccy_forecast_202601_loss") == 100.0
