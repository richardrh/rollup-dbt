"""Tests for rollup.io.ep_summary — vendor EP-summary xlsx to long CSV."""

from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl
import pytest
from openpyxl import Workbook

from rollup.config import VendorName
from rollup.io.ep_summary import (
    convert_ep_summaries_to_csv,
    read_risklink_ep_summary,
    read_verisk_ep_summary,
)
from rollup.schemas import frames as F
from rollup.schemas.columns import CanonicalEpSummaryCol as EP


# ---------------------------------------------------------------------------
# Shared fixture: path to the real risklink EP-summary xlsx.
# ---------------------------------------------------------------------------

_EP_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "ep_summaries" / "risklink" / "Hiscox RNL26 RMS by LOB (EDM & RDM).xlsx"
)
_VERISK_EP = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "ep_summaries" / "verisk" / "verisk.xlsx"
)

_SKIP_IF_MISSING = pytest.mark.skipif(
    not _EP_DIR.exists(),
    reason=f"real xlsx not found at {_EP_DIR}",
)
_SKIP_VERISK_IF_MISSING = pytest.mark.skipif(
    not _VERISK_EP.exists(),
    reason=f"real Verisk xlsx not found at {_VERISK_EP}",
)


def _write_verisk_fixture(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "PML by LOB"
    ws.append(["meta"])
    ws.append(["meta"])
    ws.append(["meta"])
    ws.append(["meta"])
    ws.append(["meta"])
    ws.append(["meta"])
    ws.append([
        None,
        "segment",
        "Analysis",
        "ExposureAttribute",
        "CatalogTypeCode",
        "aal_0.0",
        "sd_0.0",
        "aep_2.0",
        "oep_2.0",
        "oep_200.0",
    ])
    ws.append([None, "EU_WS_GCAdj|LOB_A|STC", "EU_WS_GCAdj", "LOB_A", "STC", 1.5, 0.0, 2.5, 3.5, 4.5])
    ws.append([None, "EU_WS|LOB_B|NON", "EU_WS", "LOB_B", "NON", 99.0, 0.0, 99.0, 99.0, 99.0])
    wb.save(path)


def _write_risklink_numeric_header_fixture(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "OEPAEP Curves"
    ws.append([None])
    ws.append([None])
    ws.append([None])
    ws.append([None])
    ws.append([None, None, None, None, None, None, None, None, "OEP", "OEP", "AEP"])
    ws.append([None, None, "ID", " Segment ", "LOB", "RegionPeril", " AAL ", " STD ", 2, 200, 2])
    ws.append([None, None, 101, "LOB_A-GB FL", "LOB_A", "GB FL", 10.0, 1.0, 20.0, 30.0, 40.0])
    wb.save(path)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@_SKIP_IF_MISSING
def test_read_risklink_ep_summary_returns_long_format():
    """Result has canonical EP-summary schema and at least 1 000 rows."""
    df = read_risklink_ep_summary(_EP_DIR)

    # Schema must match the canonical analyst EP-summary CSV exactly.
    assert df.schema == F.CANONICAL_EP_SUMMARY, (
        f"schema mismatch.\nExpected: {F.CANONICAL_EP_SUMMARY}\nGot:      {df.schema}"
    )

    # 417 data rows × ~25 ep columns (1 AAL + 12 OEP + 12 AEP) > 1000.
    assert df.height >= 1000, f"expected >= 1000 rows, got {df.height}"


@_SKIP_IF_MISSING
def test_read_risklink_ep_summary_aal_rows_have_rp_zero():
    """All rows with ep_type == 'AAL' must have rp == 0."""
    df = read_risklink_ep_summary(_EP_DIR)
    aal = df.filter(pl.col(EP.EP_TYPE) == "AAL")

    assert aal.height > 0, "no AAL rows found"
    bad = aal.filter(pl.col(EP.RETURN_PERIOD) != 0)
    assert bad.height == 0, (
        f"{bad.height} AAL rows have rp != 0:\n{bad.head(5)}"
    )


@_SKIP_IF_MISSING
def test_read_risklink_ep_summary_oep_rps_are_correct():
    """Distinct rp values for ep_type == 'OEP' must include delivered RPs."""
    expected_rps = {2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000}
    df = read_risklink_ep_summary(_EP_DIR)
    oep_rps = set(
        df.filter(pl.col(EP.EP_TYPE) == "OEP")[EP.RETURN_PERIOD].unique().to_list()
    )
    missing = expected_rps - oep_rps
    assert not missing, (
        f"OEP return periods missing from output: {sorted(missing)}"
    )


def test_read_risklink_ep_summary_supports_numeric_rp_headers(tmp_path: Path):
    xlsx = tmp_path / "risklink_numeric_header.xlsx"
    _write_risklink_numeric_header_fixture(xlsx)

    df = read_risklink_ep_summary(xlsx)

    assert df.schema == F.CANONICAL_EP_SUMMARY
    assert df.height == 4
    assert set(df[EP.EP_TYPE].to_list()) == {"AAL", "AEP", "OEP"}
    assert set(df[EP.RETURN_PERIOD].to_list()) == {0, 2, 200}
    assert set(df[EP.ANALYSIS_ID].to_list()) == {"101"}
    assert set(df[EP.MODELLED_LOB].to_list()) == {"LOB_A"}


@_SKIP_IF_MISSING
def test_convert_ep_summaries_writes_csv(tmp_path: Path):
    """copy xlsx → tmp_path, run converter, assert .long.csv exists and is readable."""
    # Copy the real xlsx into a temp directory.
    dest_xlsx = tmp_path / "rms_ep_summary.xlsx"
    shutil.copy2(_EP_DIR, dest_xlsx)

    written = convert_ep_summaries_to_csv(tmp_path, VendorName.RISKLINK)

    assert len(written) == 1, f"expected 1 csv written, got {len(written)}: {written}"
    csv_path = written[0]
    assert csv_path.exists(), f"csv not found at {csv_path}"
    assert csv_path.suffix == ".csv"
    assert csv_path.name == "rms_ep_summary.long.csv"

    # Must be readable as a polars DataFrame with the correct schema.
    df = pl.read_csv(csv_path, schema=F.CANONICAL_EP_SUMMARY)
    assert df.height >= 1000, f"expected >= 1000 rows, got {df.height}"


def test_read_verisk_ep_summary_filters_to_stc_rows(tmp_path: Path):
    xlsx = tmp_path / "verisk.xlsx"
    _write_verisk_fixture(xlsx)

    df = read_verisk_ep_summary(xlsx)

    assert df.schema == F.CANONICAL_EP_SUMMARY
    assert df.height == 4
    assert set(df[EP.EP_TYPE].to_list()) == {"AAL", "AEP", "OEP"}
    assert set(df[EP.RETURN_PERIOD].to_list()) == {0, 2, 200}
    assert set(df[EP.MODELLED_PERIL].to_list()) == {"EU_WS_GCAdj"}
    assert set(df[EP.MODELLED_LOB].to_list()) == {"LOB_A"}


def test_convert_ep_summaries_writes_verisk_fixture_csv(tmp_path: Path):
    xlsx = tmp_path / "verisk.xlsx"
    _write_verisk_fixture(xlsx)

    written = convert_ep_summaries_to_csv(tmp_path, VendorName.VERISK)

    assert [p.name for p in written] == ["verisk.long.csv"]
    df = pl.read_csv(written[0], schema=F.CANONICAL_EP_SUMMARY)
    assert df.height == 4


@_SKIP_VERISK_IF_MISSING
def test_real_verisk_ep_summary_analysis_labels_match_expected():
    df = read_verisk_ep_summary(_VERISK_EP)

    assert df.schema == F.CANONICAL_EP_SUMMARY
    assert set(df[EP.MODELLED_PERIL].unique().to_list()) == {
        "EU_EQ",
        "EU_FL",
        "EU_WS",
        "EU_WS_GCAdj",
        "UK_FL",
        "UK_WSSS",
        "UK_WSSS_GCAdj",
    }
    assert df.filter(pl.col(EP.EP_TYPE) == "AAL").select(EP.RETURN_PERIOD).unique().item() == 0


@_SKIP_VERISK_IF_MISSING
def test_read_real_verisk_ep_summary_returns_long_format():
    df = read_verisk_ep_summary(_VERISK_EP)

    assert df.schema == F.CANONICAL_EP_SUMMARY
    assert df.height > 1000


@_SKIP_VERISK_IF_MISSING
def test_read_verisk_ep_summary_aal_rows_have_rp_zero():
    df = read_verisk_ep_summary(_VERISK_EP)
    aal = df.filter(pl.col(EP.EP_TYPE) == "AAL")

    assert aal.height > 0
    assert aal.filter(pl.col(EP.RETURN_PERIOD) != 0).height == 0


@_SKIP_VERISK_IF_MISSING
def test_read_verisk_ep_summary_oep_rps_are_correct():
    df = read_verisk_ep_summary(_VERISK_EP)
    expected_rps = {2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000}
    oep_rps = set(df.filter(pl.col(EP.EP_TYPE) == "OEP")[EP.RETURN_PERIOD].unique().to_list())

    assert expected_rps <= oep_rps


@_SKIP_VERISK_IF_MISSING
def test_convert_ep_summaries_writes_verisk_csv(tmp_path: Path):
    dest_xlsx = tmp_path / "verisk.xlsx"
    shutil.copy2(_VERISK_EP, dest_xlsx)

    written = convert_ep_summaries_to_csv(tmp_path, VendorName.VERISK)

    assert len(written) == 1
    csv_path = written[0]
    assert csv_path.name == "verisk.long.csv"
    df = pl.read_csv(csv_path, schema=F.CANONICAL_EP_SUMMARY)
    assert df.height > 1000
