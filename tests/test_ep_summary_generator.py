from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.ep_summary_generator import (
    build_ep_summary_from_wide_csv,
    generate_vendor_ep_summary,
    scan_ep_summary_csvs,
)


def _write_canonical_wide_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "id": "ANALYSIS_1",
                "modelled_lob": "LOB_A",
                "modelled_peril": "PERIL_A",
                "segment": "ignored",
                "AAL_0": 12.5,
                "AEP_50": 500.0,
                "OEP_100": 1000.0,
                "sd_0": 99.0,
            },
        ]
    ).write_csv(path)


def test_build_ep_summary_from_wide_csv_converts_canonical_schema(tmp_path: Path) -> None:
    csv_path = tmp_path / "canonical.csv"
    _write_canonical_wide_csv(csv_path)

    frame = build_ep_summary_from_wide_csv(csv_path, "verisk")

    assert frame.to_dicts() == [
        {
            "vendor": "verisk",
            "analysis_id": "ANALYSIS_1",
            "modelled_lob": "LOB_A",
            "modelled_peril": "PERIL_A",
            "ep_type": "AAL",
            "return_period": 0,
            "loss": 12.5,
        },
        {
            "vendor": "verisk",
            "analysis_id": "ANALYSIS_1",
            "modelled_lob": "LOB_A",
            "modelled_peril": "PERIL_A",
            "ep_type": "AEP",
            "return_period": 50,
            "loss": 500.0,
        },
        {
            "vendor": "verisk",
            "analysis_id": "ANALYSIS_1",
            "modelled_lob": "LOB_A",
            "modelled_peril": "PERIL_A",
            "ep_type": "OEP",
            "return_period": 100,
            "loss": 1000.0,
        },
    ]


def test_build_ep_summary_from_wide_csv_accepts_aliases_dot_zero_and_comma_losses(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "verisk_export.csv"
    csv_path.write_text(
        "id,segment,Analysis,ExposureAttribute,CatalogTypeCode,aal_0.0,aep_50.0,oep_100.0,sd_0.0\n"
        '101,ignored, US_WS , Property , STC ," 1,750,472 ",2500," 3,000 ",9\n'
        '102,ignored,US_WS,Property,WSC,99,99,99,9\n',
        encoding="utf-8",
    )

    frame = build_ep_summary_from_wide_csv(csv_path, "verisk")

    assert frame.to_dicts() == [
        {
            "vendor": "verisk",
            "analysis_id": "101",
            "modelled_lob": "Property",
            "modelled_peril": "US_WS",
            "ep_type": "AAL",
            "return_period": 0,
            "loss": 1750472.0,
        },
        {
            "vendor": "verisk",
            "analysis_id": "101",
            "modelled_lob": "Property",
            "modelled_peril": "US_WS",
            "ep_type": "AEP",
            "return_period": 50,
            "loss": 2500.0,
        },
        {
            "vendor": "verisk",
            "analysis_id": "101",
            "modelled_lob": "Property",
            "modelled_peril": "US_WS",
            "ep_type": "OEP",
            "return_period": 100,
            "loss": 3000.0,
        },
    ]


def test_scan_csvs_excludes_existing_long_outputs_and_sorts(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    source_dir = data_root / "ep_summaries" / "verisk"
    first_csv = source_dir / "b.csv"
    second_csv = source_dir / "A.csv"
    long_csv = source_dir / "verisk_ep_summary.long.csv"
    _write_canonical_wide_csv(first_csv)
    _write_canonical_wide_csv(second_csv)
    long_csv.write_text("vendor,analysis_id\nverisk,old\n", encoding="utf-8")

    assert scan_ep_summary_csvs(data_root, "verisk") == [second_csv, first_csv]


def test_generate_vendor_ep_summary_writes_canonical_output(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    source_dir = data_root / "ep_summaries" / "verisk"
    csv_path = source_dir / "selected.csv"
    _write_canonical_wide_csv(csv_path)

    output_path = generate_vendor_ep_summary(data_root, "verisk", csv_path)

    assert output_path == source_dir / "verisk_ep_summary.long.csv"
    output = pl.read_csv(output_path)
    assert output.columns == [
        "vendor",
        "analysis_id",
        "modelled_lob",
        "modelled_peril",
        "ep_type",
        "return_period",
        "loss",
    ]
    assert output.select("vendor", "analysis_id", "loss").to_dicts() == [
        {"vendor": "verisk", "analysis_id": "ANALYSIS_1", "loss": 12.5},
        {"vendor": "verisk", "analysis_id": "ANALYSIS_1", "loss": 500.0},
        {"vendor": "verisk", "analysis_id": "ANALYSIS_1", "loss": 1000.0},
    ]
