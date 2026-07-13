from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from rollup.columns import Col
from rollup.staging.stg_ep_summaries import load_ep_summaries


def test_load_ep_summaries_concatenates_simple_vendor_folders(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    risklink = data_root / "ep_summaries" / "risklink"
    verisk = data_root / "ep_summaries" / "verisk"
    risklink.mkdir(parents=True)
    verisk.mkdir(parents=True)
    pl.DataFrame({Col.vendor: ["wrong"], "loss": [1.0]}).write_csv(risklink / "a.long.csv")
    pl.DataFrame({Col.vendor: ["also_wrong"], "loss": [2.0]}).write_csv(verisk / "b.long.csv")

    result = load_ep_summaries(data_root).collect().sort("loss")

    assert result.select(Col.vendor, "loss").rows() == [("risklink", 1.0), ("verisk", 2.0)]
    assert "_source_path" not in result.columns
    assert "_path_vendor" not in result.columns


def test_load_ep_summaries_supports_hive_style_vendor_folders(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    risklink = data_root / "ep_summaries" / "vendor=risklink"
    verisk = data_root / "ep_summaries" / "vendor=verisk"
    risklink.mkdir(parents=True)
    verisk.mkdir(parents=True)
    pl.DataFrame({"loss": [1.0]}).write_csv(risklink / "a.long.csv")
    pl.DataFrame({"loss": [2.0]}).write_csv(verisk / "b.long.csv")

    result = load_ep_summaries(data_root).select(Col.vendor, "loss").collect().sort("loss")

    assert result.rows() == [("risklink", 1.0), ("verisk", 2.0)]


def test_load_ep_summaries_unrecognised_vendor_folder_raises(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    unknown = data_root / "ep_summaries" / "unknown"
    unknown.mkdir(parents=True)
    path = unknown / "a.long.csv"
    pl.DataFrame({"loss": [1.0]}).write_csv(path)

    with pytest.raises(ValueError, match="recognised vendor folder") as exc:
        load_ep_summaries(data_root)

    assert str(path) in str(exc.value)


def test_load_ep_summaries_no_files_returns_empty_lazyframe(tmp_path: Path) -> None:
    result = load_ep_summaries(tmp_path / "data")

    assert isinstance(result, pl.LazyFrame)
    assert result.collect().is_empty()
