from __future__ import annotations

import polars as pl
import pytest

from rollup.columns import RawCol
from rollup.model_validation import validate_schema
from rollup.staging import stg_risklink_flood_events, stg_verisk_ylt


def test_transform_catches_missing_raw_column_with_model_context() -> None:
    frame = pl.DataFrame({RawCol.CatalogTypeCode: ["STC"]}).lazy()

    with pytest.raises(
        ValueError, match="stg_verisk_ylt.*could not resolve output schema"
    ):
        stg_verisk_ylt.Model.transform(frame)


def test_transform_resolves_casts_for_raw_input_dtypes() -> None:
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

    assert (
        stg_verisk_ylt.Model.transform(frame).collect_schema()
        == stg_verisk_ylt.Model.schema()
    )


def test_validate_schema_accepts_exact_ordered_schema() -> None:
    expected = pl.Schema({"name": pl.String, "count": pl.Int64})

    validate_schema(
        "test_model", expected, pl.DataFrame({"name": ["a"], "count": [1]}).lazy()
    )


def test_validate_schema_rejects_exact_schema_mismatch_with_context() -> None:
    expected = pl.Schema({"name": pl.String, "count": pl.Int64})
    actual = pl.DataFrame({"count": [1], "name": ["a"]}).lazy()

    with pytest.raises(ValueError) as exc:
        validate_schema("test_model", expected, actual)

    message = str(exc.value)
    assert "test_model" in message
    assert str(expected) in message
    assert str(actual.collect_schema()) in message


def test_validate_schema_reports_schema_resolution_failure_with_model_context() -> None:
    frame = pl.DataFrame({"name": ["a"]}).lazy().select("missing")

    with pytest.raises(ValueError, match="test_model.*could not resolve output schema"):
        validate_schema("test_model", pl.Schema({"name": pl.String}), frame)


def test_validate_schema_does_not_collect_rows(
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

    validate_schema("test_model", frame.collect_schema(), frame)


def test_risklink_flood_event_transform_rejects_string_occurrence_dates_on_collect() -> (
    None
):
    frame = pl.DataFrame(
        {
            "ModelEventID": [1],
            RawCol.ModelOccurrenceYear: [2026],
            RawCol.RegionPerilID: [70],
            RawCol.ModelOccurrenceDate: ["2026-01-01"],
        }
    ).lazy()

    candidate = stg_risklink_flood_events.Model.transform(frame)
    assert candidate.collect_schema() == stg_risklink_flood_events.Model.schema()
    with pytest.raises(pl.exceptions.PolarsError):
        candidate.collect()
