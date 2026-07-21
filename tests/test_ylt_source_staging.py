from __future__ import annotations

import polars as pl
import pytest

from rollup.columns import Col, RawCol
from rollup.intermediate import int_ylt_normalized
from rollup.sources.ylt import load
from rollup.staging import stg_risklink_ylt, stg_verisk_ylt


def _write_ylt_fixture(data_root):
    verisk_dir = data_root / "ylt" / "verisk"
    risklink_dir = data_root / "ylt" / "risklink"
    verisk_dir.mkdir(parents=True)
    risklink_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            RawCol.CatalogTypeCode: ["STC"],
            RawCol.ExposureAttribute: ["LOB"],
            RawCol.Analysis: ["PERIL"],
            RawCol.ModelCode: [1],
            RawCol.YearID: [2026],
            RawCol.EventID: [10],
            RawCol.GroundUpLoss: [100.0],
        }
    ).write_parquet(verisk_dir / "part.parquet")
    pl.DataFrame(
        {
            RawCol.anlsid: [9001],
            RawCol.yearid: [2026],
            RawCol.eventid: [20],
            RawCol.loss: [200.0],
        }
    ).write_parquet(risklink_dir / "part.parquet")


def test_loads_direct_vendor_folders(tmp_path) -> None:
    _write_ylt_fixture(tmp_path)

    frames = load(tmp_path)

    assert sorted(frames) == ["risklink", "verisk"]
    assert frames["verisk"].collect().height == 1
    assert frames["risklink"].collect().height == 1


def test_load_raises_when_vendor_folder_has_no_parquet(tmp_path) -> None:
    (tmp_path / "ylt" / "verisk").mkdir(parents=True)

    with pytest.raises(FileNotFoundError, match="no verisk YLT parquet files found"):
        load(tmp_path)


def test_normalize_ylt_returns_combined_plain_lazy_frame(tmp_path) -> None:
    _write_ylt_fixture(tmp_path)

    ylts = load(tmp_path)
    normalized = (
        int_ylt_normalized.Model.transform(
            stg_verisk_ylt.Model.transform(ylts["verisk"]),
            stg_risklink_ylt.Model.transform(ylts["risklink"]),
        )
        .collect()
        .sort(Col.vendor)
    )

    assert normalized[Col.vendor].to_list() == ["risklink", "verisk"]
    assert normalized[Col.loss].to_list() == [200.0, 100.0]
