from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from rollup.columns import Col, RawCol
from rollup.intermediate import int_ep_blending_targets
from rollup.model_validation import collect_lazy_schema, require_join_key_compatible
from rollup.staging import stg_risklink_flood_events, stg_verisk_ylt


def test_validation_catches_missing_select_column_with_model_context() -> None:
    frame = pl.DataFrame({RawCol.CatalogTypeCode: ["STC"]}).lazy()

    with pytest.raises(
        ValueError, match="stg_verisk_ylt.*raw_ylt.*missing required columns"
    ):
        stg_verisk_ylt.validate(frame)


def test_validation_catches_important_incompatible_dtype() -> None:
    frame = pl.DataFrame(
        {
            RawCol.CatalogTypeCode: ["STC"],
            RawCol.Analysis: ["PERIL"],
            RawCol.ExposureAttribute: ["LOB"],
            RawCol.ModelCode: ["not-int"],
            RawCol.YearID: [1],
            RawCol.EventID: [1],
            RawCol.GroundUpLoss: [1.0],
        }
    ).lazy()

    with pytest.raises(ValueError, match="stg_verisk_ylt.*ModelCode.*integer"):
        stg_verisk_ylt.validate(frame)


def test_validation_catches_join_key_dtype_mismatch() -> None:
    target_points = pl.DataFrame(
        {
            Col.region_peril_id: [1],
            Col.blend_subregion_peril_id: [10],
            Col.base_model: ["verisk"],
            Col.risklink_loss: [1.0],
            Col.verisk_loss: [2.0],
        }
    ).lazy()
    weights = pl.DataFrame(
        {
            Col.region_peril_id: ["1"],
            Col.blend_subregion_peril_id: [10],
            Col.risklink_weight: [0.5],
            Col.verisk_weight: [0.5],
        }
    ).lazy()

    with pytest.raises(
        ValueError, match="int_ep_blending_targets.*join key dtype mismatch"
    ):
        int_ep_blending_targets.validate(target_points, weights)


def test_join_key_validation_accepts_different_numeric_widths() -> None:
    left = pl.DataFrame({"join_id": pl.Series([1], dtype=pl.Int32)}).lazy()
    right = pl.DataFrame({"join_id": pl.Series([1], dtype=pl.Int64)}).lazy()

    require_join_key_compatible(
        "test_model",
        "left",
        collect_lazy_schema("test_model", "left", left),
        "right",
        collect_lazy_schema("test_model", "right", right),
        ["join_id"],
    )


def test_valid_lazyframe_schema_validation_does_not_collect_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pl.DataFrame(
        {
            RawCol.CatalogTypeCode: ["STC"],
            RawCol.Analysis: ["PERIL"],
            RawCol.ExposureAttribute: ["LOB"],
            RawCol.ModelCode: [1],
            RawCol.YearID: [1],
            RawCol.EventID: [1],
            RawCol.GroundUpLoss: [1.0],
        }
    ).lazy()

    def fail_collect(
        self: pl.LazyFrame, *args: object, **kwargs: object
    ) -> pl.DataFrame:
        raise AssertionError("collect must not be called by schema validation")

    monkeypatch.setattr(pl.LazyFrame, "collect", fail_collect)

    stg_verisk_ylt.validate(frame)


def test_risklink_flood_event_validation_rejects_string_occurrence_dates() -> None:
    frame = pl.DataFrame(
        {
            "ModelEventID": [1],
            RawCol.ModelOccurrenceYear: [2026],
            RawCol.RegionPerilID: [70],
            RawCol.ModelOccurrenceDate: ["2026-01-01"],
        }
    ).lazy()

    with pytest.raises(
        ValueError, match="stg_risklink_flood_events.*ModelOccurrenceDate.*date_like"
    ):
        stg_risklink_flood_events.validate(frame)


@pytest.mark.parametrize("value", [date(2026, 1, 1), datetime(2026, 1, 1, 12, 30)])
def test_risklink_flood_event_validation_accepts_date_and_datetime_occurrence_dates(
    value: object,
) -> None:
    frame = pl.DataFrame(
        {
            "ModelEventID": [1],
            RawCol.ModelOccurrenceYear: [2026],
            RawCol.RegionPerilID: [70],
            RawCol.ModelOccurrenceDate: [value],
        }
    ).lazy()

    stg_risklink_flood_events.validate(frame)


def test_base_selection_validation_error_uses_owning_model_name() -> None:
    frame = pl.DataFrame({Col.vendor: ["risklink"]}).lazy()

    with pytest.raises(ValueError) as exc:
        from rollup.intermediate import int_ylt_base_selected

        int_ylt_base_selected.validate(frame)

    message = str(exc.value)
    assert "int_ylt_base_selected" in message
    assert "enriched_ylt" in message
    assert "int_ylt_main_base_selected" not in message
