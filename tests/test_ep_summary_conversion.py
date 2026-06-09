from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from rollup import write_ep_summaries, write_ep_summary
from rollup.ep_summary_generator import build_ep_summary_from_wide_csv


def test_build_ep_summary_from_wide_csv_converts_metrics_to_canonical_long_rows(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "verisk_clean.csv"
    csv_path.write_text(
        "id,ExposureAttribute,Analysis,CatalogTypeCode,AAL_0,AEP_50,OEP_100,Ignored\n"
        " EQ , Fine Art , WS ,STC, 1, 2, 3,unused\n"
        " EQ , Fine Art , WS ,NDC, 10, 20, 30,unused\n"
        " EQ , Fine Art , WS ,STC,, 200, 300,unused\n"
        " , Fine Art , WS ,STC, 4, 5, 6,unused\n",
        encoding="utf-8",
    )

    result = build_ep_summary_from_wide_csv(csv_path, "verisk").sort(
        ["ep_type", "loss"]
    )

    assert result.columns == [
        "vendor",
        "analysis_id",
        "modelled_lob",
        "modelled_peril",
        "ep_type",
        "return_period",
        "loss",
    ]
    assert result.rows() == [
        ("verisk", "EQ", "Fine Art", "WS", "AAL", 0, 1.0),
        ("verisk", "EQ", "Fine Art", "WS", "AEP", 50, 2.0),
        ("verisk", "EQ", "Fine Art", "WS", "AEP", 50, 200.0),
        ("verisk", "EQ", "Fine Art", "WS", "OEP", 100, 3.0),
        ("verisk", "EQ", "Fine Art", "WS", "OEP", 100, 300.0),
    ]


def test_build_ep_summary_from_wide_csv_accepts_comma_formatted_losses(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "risklink_clean.csv"
    csv_path.write_text(
        'id,modelled_lob,modelled_peril,AAL_0,AEP_250.0\n9001,Property,Flood,"1,234.50"," 2,500 "\n',
        encoding="utf-8",
    )

    result = build_ep_summary_from_wide_csv(csv_path, "risklink").sort("ep_type")

    assert result.select("ep_type", "return_period", "loss").rows() == [
        ("AAL", 0, 1234.5),
        ("AEP", 250, 2500.0),
    ]


def test_build_ep_summary_from_wide_csv_requires_metrics(tmp_path: Path) -> None:
    csv_path = tmp_path / "no_metrics.csv"
    csv_path.write_text(
        "id,modelled_lob,modelled_peril\nEQ,Fine Art,WS\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="does not contain metric columns"):
        build_ep_summary_from_wide_csv(csv_path, "verisk")


def test_public_write_ep_summary_writes_vendor_long_csv(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    csv_path = data_root / "ep_summaries" / "verisk" / "verisk_clean.csv"
    csv_path.parent.mkdir(parents=True)
    csv_path.write_text(
        "id,modelled_lob,modelled_peril,AAL_0\nEQ,Fine Art,WS,1\n", encoding="utf-8"
    )

    output_path = write_ep_summary(data_root, "verisk", csv_path)

    assert (
        output_path
        == data_root / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv"
    )
    assert pl.read_csv(output_path).rows() == [
        ("verisk", "EQ", "Fine Art", "WS", "AAL", 0, 1.0)
    ]


def test_public_write_ep_summaries_scans_each_vendor_and_ignores_existing_long_csvs(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    verisk_dir = data_root / "ep_summaries" / "verisk"
    risklink_dir = data_root / "ep_summaries" / "risklink"
    verisk_dir.mkdir(parents=True)
    risklink_dir.mkdir(parents=True)
    (verisk_dir / "existing.long.csv").write_text("not,a,source\n", encoding="utf-8")
    (verisk_dir / "verisk_clean.csv").write_text(
        "id,modelled_lob,modelled_peril,AAL_0\nEQ,Fine Art,WS,1\n",
        encoding="utf-8",
    )
    (risklink_dir / "risklink_clean.csv").write_text(
        "id,modelled_lob,modelled_peril,AAL_0\n9001,Property,Flood,2\n",
        encoding="utf-8",
    )

    output_paths = write_ep_summaries(data_root)

    assert output_paths == [
        verisk_dir / "verisk_ep_summary.long.csv",
        risklink_dir / "rms_ep_summary.long.csv",
    ]
    assert pl.read_csv(output_paths[0]).select(
        "vendor", "analysis_id", "loss"
    ).rows() == [("verisk", "EQ", 1.0)]
    assert pl.read_csv(output_paths[1]).select(
        "vendor", "analysis_id", "loss"
    ).rows() == [("risklink", 9001, 2.0)]


def test_public_write_ep_summaries_requires_explicit_file_when_multiple_sources_exist(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    verisk_dir = data_root / "ep_summaries" / "verisk"
    risklink_dir = data_root / "ep_summaries" / "risklink"
    verisk_dir.mkdir(parents=True)
    risklink_dir.mkdir(parents=True)
    (verisk_dir / "first.csv").write_text(
        "id,modelled_lob,modelled_peril,AAL_0\nEQ,Fine Art,WS,1\n", encoding="utf-8"
    )
    (verisk_dir / "second.csv").write_text(
        "id,modelled_lob,modelled_peril,AAL_0\nEQ,Fine Art,WS,2\n", encoding="utf-8"
    )
    (risklink_dir / "risklink_clean.csv").write_text(
        "id,modelled_lob,modelled_peril,AAL_0\n9001,Property,Flood,2\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Multiple source CSV files found for verisk"):
        write_ep_summaries(data_root)
