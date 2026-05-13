"""Property-based tests for ``rollup.stages.staging``.

Targets ``normalize_risklink_ylt`` and ``normalize_verisk_ylt``.

All tests are marked ``@pytest.mark.fuzz`` and skipped unless
``--run-fuzz`` is passed.
"""

from __future__ import annotations

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import rollup.schemas.frames as F
from rollup.config import VendorName
from rollup.schemas.columns import (
    AnalysesCol as AN,
    NormalizedYltCol as Y,
    PerilsCol as P,
    RawRisklinkYltCol as RLK,
    RawVeriskYltCol as VK,
    RefLobsCol as LB,
)
from rollup.stages.staging import (
    normalize_risklink_ylt,
    normalize_verisk_ylt,
)
from rollup.validate import validate_schema

from .strategies import lazyframe_from_schema


# ---------------------------------------------------------------------------
# Consistent-input strategy for normalize_risklink_ylt
# ---------------------------------------------------------------------------

@st.composite
def _consistent_risklink_input(draw: st.DrawFn) -> tuple[
    pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame
]:
    """Generate (raw_rl, analyses, perils, lobs) where every FK resolves.

    Build bottom-up:
    1. N peril_ids
    2. N lob_ids with distinct modelled_lob strings
    3. N risklink analyses mapping each peril_id and lob_id
    4. Raw YLT rows referencing those analysis_ids
    """
    n = draw(st.integers(min_value=1, max_value=4))

    peril_ids = list(range(100, 100 + n))
    lob_ids = list(range(200, 200 + n))

    # peril_family closed set — keep short to avoid confusing downstream logic
    peril_families = draw(
        st.lists(
            st.sampled_from(["EQ", "TC", "FL", "WS", "CS", "WF"]),
            min_size=n,
            max_size=n,
        )
    )
    peril_names = [f"peril_{pid}" for pid in peril_ids]
    regions = draw(st.lists(st.sampled_from(["US", "EU", "UK", "AP"]), min_size=n, max_size=n))

    perils_df = pl.LazyFrame({
        P.PERIL_ID:     peril_ids,
        P.NAME:         peril_names,
        P.REGION:       regions,
        P.PERIL_FAMILY: peril_families,
    }, schema=F.PERILS)

    # Each lob has a distinct modelled_lob string — used as join key in verisk path.
    modelled_lobs = [f"lob_{lid}" for lid in lob_ids]
    rollup_lobs   = [f"rlob_{lid}" for lid in lob_ids]
    lobs_df = pl.LazyFrame({
        LB.LOB_ID:             lob_ids,
        LB.MODELLED_LOB:       modelled_lobs,
        LB.ROLLUP_LOB:         rollup_lobs,
        LB.LOB_TYPE:           ["primary"] * n,
        LB.CDS_CAT_CLASS_NAME: [f"CDS UK {lid}" for lid in lob_ids],
        LB.OFFICE:             ["UK"] * n,
        LB.CLASS:              [f"class_{lid}" for lid in lob_ids],
    }, schema=F.REF_LOBS)

    # One RiskLink analysis per (lob, peril) pair.
    rl_analysis_ids = [str(1000 + i) for i in range(n)]
    analyses_df = pl.LazyFrame({
        AN.VENDOR:         [VendorName.RISKLINK] * n,
        AN.ANALYSIS_ID:    rl_analysis_ids,
        AN.MODELLED_LABEL: [f"RL_{pid}" for pid in peril_ids],
        AN.PERIL_ID:       peril_ids,
        AN.LOB_ID:         lob_ids,
    }, schema=F.ANALYSES)

    # Raw RiskLink YLT rows: anlsid matches the integer portion of analysis_id.
    n_rows = draw(st.integers(min_value=1, max_value=20))
    row_indices = draw(st.lists(st.integers(min_value=0, max_value=n - 1), min_size=n_rows, max_size=n_rows))

    raw_rl_df = pl.LazyFrame({
        RLK.SIMULATION_SET_ID: [1] * n_rows,
        RLK.YEAR_ID:           list(range(1, n_rows + 1)),
        RLK.EVENT_ID:          list(range(101, 101 + n_rows)),
        RLK.DATE:              ["2024-01-01"] * n_rows,
        RLK.P_VALUE:           [0.01] * n_rows,
        RLK.ANLS_ID:           [1000 + row_indices[i] for i in range(n_rows)],
        RLK.NAME:              ["test"] * n_rows,
        RLK.DESCRIPTION:       ["desc"] * n_rows,
        RLK.RATE:              [1.0] * n_rows,
        RLK.MEAN_LOSS:         [1000.0] * n_rows,
        RLK.STD_DEV:           [100.0] * n_rows,
        RLK.EXP_VALUE:         [500.0] * n_rows,
        RLK.LOSS:              [1500.0] * n_rows,
    }, schema=F.RAW_RISKLINK_YLT)

    return raw_rl_df, analyses_df, perils_df, lobs_df


@st.composite
def _consistent_verisk_input(draw: st.DrawFn) -> tuple[
    pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame
]:
    """Generate (raw_vk, analyses, perils, lobs) where every FK resolves.

    Build bottom-up:
    1. N peril_ids → perils
    2. N lob_ids with distinct modelled_lob strings → lobs
    3. N verisk analyses (lob_id=null) referencing peril_ids
    4. Raw Verisk YLT rows with CatalogTypeCode containing 'STC' referencing
       analysis IDs and ExposureAttribute matching modelled_lob.
    """
    n = draw(st.integers(min_value=1, max_value=4))

    peril_ids = list(range(100, 100 + n))
    lob_ids = list(range(200, 200 + n))

    peril_families = draw(
        st.lists(
            st.sampled_from(["EQ", "TC", "FL", "WS", "CS", "WF"]),
            min_size=n, max_size=n,
        )
    )

    perils_df = pl.LazyFrame({
        P.PERIL_ID:     peril_ids,
        P.NAME:         [f"peril_{pid}" for pid in peril_ids],
        P.REGION:       draw(st.lists(st.sampled_from(["US", "EU", "UK"]), min_size=n, max_size=n)),
        P.PERIL_FAMILY: peril_families,
    }, schema=F.PERILS)

    modelled_lobs = [f"lob_{lid}" for lid in lob_ids]
    lobs_df = pl.LazyFrame({
        LB.LOB_ID:             lob_ids,
        LB.MODELLED_LOB:       modelled_lobs,
        LB.ROLLUP_LOB:         [f"rlob_{lid}" for lid in lob_ids],
        LB.LOB_TYPE:           ["primary"] * n,
        LB.CDS_CAT_CLASS_NAME: [f"CDS UK {lid}" for lid in lob_ids],
        LB.OFFICE:             ["UK"] * n,
        LB.CLASS:              [f"class_{lid}" for lid in lob_ids],
    }, schema=F.REF_LOBS)

    # Verisk analyses: lob_id is null (pl.Int64 null).
    vk_analysis_ids = [f"VK_ANALYSIS_{pid}" for pid in peril_ids]
    analyses_df = pl.LazyFrame({
        AN.VENDOR:         [VendorName.VERISK] * n,
        AN.ANALYSIS_ID:    vk_analysis_ids,
        AN.MODELLED_LABEL: vk_analysis_ids,
        AN.PERIL_ID:       peril_ids,
        AN.LOB_ID:         [None] * n,
    }, schema=F.ANALYSES)

    n_rows = draw(st.integers(min_value=1, max_value=20))
    row_indices = draw(st.lists(st.integers(min_value=0, max_value=n - 1), min_size=n_rows, max_size=n_rows))

    raw_vk_df = pl.LazyFrame({
        VK.ANALYSIS:           [vk_analysis_ids[row_indices[i]] for i in range(n_rows)],
        VK.EXPOSURE_ATTRIBUTE: [modelled_lobs[row_indices[i]] for i in range(n_rows)],
        VK.CATALOG_TYPE_CODE:  ["STC"] * n_rows,
        VK.EVENT_ID:           list(range(201, 201 + n_rows)),
        VK.MODEL_CODE:         [0] * n_rows,
        VK.YEAR_ID:            list(range(1, n_rows + 1)),
        VK.PERILSET_CODE:      [1] * n_rows,
        VK.GROUND_UP_LOSS:     [1000.0] * n_rows,
        VK.GROSS_LOSS:         [1100.0] * n_rows,
        VK.NET_PRE_CAT_LOSS:   [1050.0] * n_rows,
        VK.FILENAME:           ["test.parquet"] * n_rows,
    }, schema=F.RAW_VERISK_YLT)

    return raw_vk_df, analyses_df, perils_df, lobs_df


# ---------------------------------------------------------------------------
# Tests for normalize_risklink_ylt
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(inputs=_consistent_risklink_input())
def test_normalize_risklink_output_matches_normalized_ylt_schema(
    inputs: tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame],
) -> None:
    """For any valid (raw_rl, analyses, perils, lobs) where every FK resolves,
    ``normalize_risklink_ylt`` output schema matches ``NORMALIZED_YLT`` exactly."""
    raw, analyses, perils, lobs = inputs
    out = normalize_risklink_ylt(raw, analyses, perils, lobs)
    validate_schema(out, F.NORMALIZED_YLT, name="rl.normalized")
    assert out.collect_schema() == F.NORMALIZED_YLT


@pytest.mark.fuzz
@given(inputs=_consistent_risklink_input())
def test_normalize_risklink_vendor_column_is_risklink(
    inputs: tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame],
) -> None:
    """Every row in the normalized RiskLink YLT has vendor == 'risklink'."""
    raw, analyses, perils, lobs = inputs
    out = normalize_risklink_ylt(raw, analyses, perils, lobs).collect()
    if out.height > 0:
        assert out[Y.VENDOR].unique().to_list() == [VendorName.RISKLINK]


# ---------------------------------------------------------------------------
# Tests for normalize_verisk_ylt
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(inputs=_consistent_verisk_input())
def test_normalize_verisk_output_matches_normalized_ylt_schema(
    inputs: tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame],
) -> None:
    """For any valid (raw_vk, analyses, perils, lobs) where every FK resolves,
    ``normalize_verisk_ylt`` output schema matches ``NORMALIZED_YLT`` exactly."""
    raw, analyses, perils, lobs = inputs
    out = normalize_verisk_ylt(raw, analyses, perils, lobs)
    validate_schema(out, F.NORMALIZED_YLT, name="vk.normalized")
    assert out.collect_schema() == F.NORMALIZED_YLT


@pytest.mark.fuzz
@given(inputs=_consistent_verisk_input())
def test_normalize_verisk_vendor_column_is_verisk(
    inputs: tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame],
) -> None:
    """Every row in the normalized Verisk YLT has vendor == 'verisk'."""
    raw, analyses, perils, lobs = inputs
    out = normalize_verisk_ylt(raw, analyses, perils, lobs).collect()
    if out.height > 0:
        assert out[Y.VENDOR].unique().to_list() == [VendorName.VERISK]


@pytest.mark.fuzz
@given(inputs=_consistent_verisk_input())
def test_normalize_verisk_non_stc_rows_dropped(
    inputs: tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame],
) -> None:
    """Rows whose normalized CatalogTypeCode lacks 'STC' are silently dropped.
    Since the strategy always generates STC rows, row count should be preserved
    (modulo inner-join drops from mismatched FKs — impossible here by construction).
    """
    raw, analyses, perils, lobs = inputs
    # Add a non-STC row — it must disappear.
    extra = raw.collect()
    if extra.height > 0:
        first_row = extra.head(1).with_columns(
            pl.lit("DLM").alias(VK.CATALOG_TYPE_CODE)
        )
        combined = pl.concat([extra, first_row]).lazy()
        out_with = normalize_verisk_ylt(combined, analyses, perils, lobs).collect().height
        out_without = normalize_verisk_ylt(raw, analyses, perils, lobs).collect().height
        assert out_with == out_without

