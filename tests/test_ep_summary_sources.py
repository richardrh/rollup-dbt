from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from rollup.columns import Col
from rollup.sources.ep_summaries import load


def test_load_concatenates_simple_vendor_folders(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    risklink = data_root / "ep_summaries" / "risklink"
    verisk = data_root / "ep_summaries" / "verisk"
    risklink.mkdir(parents=True)
    verisk.mkdir(parents=True)
    canonical = {
        Col.vendor: ["wrong"],
        Col.analysis_id: ["analysis"],
        Col.modelled_lob: ["LOB"],
        Col.modelled_peril: ["PERIL"],
        Col.ep_type: ["AAL"],
        Col.return_period: [0],
        "loss": [1.0],
    }
    pl.DataFrame(canonical).write_csv(risklink / "a.long.csv")
    pl.DataFrame({**canonical, Col.vendor: ["also_wrong"], "loss": [2.0]}).write_csv(
        verisk / "b.long.csv"
    )

    result = load(data_root).collect().sort("loss")

    assert result.select(Col.vendor, "loss").rows() == [
        ("risklink", 1.0),
        ("verisk", 2.0),
    ]
    assert "_source_path" not in result.columns
    assert "_path_vendor" not in result.columns


def test_load_unrecognised_vendor_folder_raises(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    unknown = data_root / "ep_summaries" / "unknown"
    unknown.mkdir(parents=True)
    path = unknown / "a.long.csv"
    pl.DataFrame({"loss": [1.0]}).write_csv(path)

    with pytest.raises(ValueError, match="recognised vendor folder") as exc:
        load(data_root)

    assert str(path) in str(exc.value)


def test_load_rejects_root_level_long_csv(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    root = data_root / "ep_summaries"
    root.mkdir(parents=True)
    path = root / "root.long.csv"
    pl.DataFrame({"loss": [1.0]}).write_csv(path)

    with pytest.raises(ValueError, match="recognised vendor folder"):
        load(data_root)


def test_load_rejects_nested_vendor_name_under_unknown_folder(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    nested = data_root / "ep_summaries" / "unknown" / "verisk"
    nested.mkdir(parents=True)
    pl.DataFrame({"loss": [1.0]}).write_csv(nested / "a.long.csv")

    with pytest.raises(ValueError, match="recognised vendor folder"):
        load(data_root)


def test_load_rejects_case_variant_vendor_folder(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    folder = data_root / "ep_summaries" / "Verisk"
    folder.mkdir(parents=True)
    pl.DataFrame({"loss": [1.0]}).write_csv(folder / "a.long.csv")

    with pytest.raises(ValueError, match="recognised vendor folder"):
        load(data_root)


def test_load_requires_each_canonical_vendor_folder(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="verisk"):
        load(tmp_path / "data")
