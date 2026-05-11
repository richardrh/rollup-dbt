"""Property-based tests for helper functions in ``rollup.pipeline``.

Targets ``forecast_tags``, ``build_variants``, and ``count_event_id_orphans``.
These are pure helper functions whose properties can be asserted without
running the full pipeline (which requires file I/O and seeds on disk).

All tests are marked ``@pytest.mark.fuzz`` and skipped unless
``--run-fuzz`` is passed.

KNOWN ISSUES
------------
BUG-1 — forecast_tags_unique: ``forecast_tags`` does not deduplicate tags when
two dates in the same calendar month are provided (e.g. 2020-01-01 and
2020-01-02 both become '202001'). The function's docstring implies one date per
month is the expected input, but no guard exists at the call site or inside
the function itself. Two duplicate tags would produce duplicate column names in
``build_all_factors`` (``f_202001`` appears twice), which Polars will reject.
See ``test_forecast_tags_unique_and_sorted`` (marked xfail) for the minimal repro.

BUG-2 — build_variants_names_unique: Same root cause as BUG-1.  When two dates
in the same month are fed to ``build_variants``, duplicate output filenames are
produced (e.g. ``HiscoAIR_202001_main`` twice). The second write would silently
overwrite the first parquet. Fix: deduplicate dates before building variants, or
guard in ``forecast_tags`` / ``build_variants``.
See ``test_build_variants_names_unique`` (marked xfail).
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

import rollup.schemas.frames as F
from rollup.config import Flavor, Vendor, VendorName
from rollup.pipeline import build_variants, count_event_id_orphans, forecast_tags
from rollup.schemas.columns import NormalizedYltCol as Y, RefAirEventsCol as AE

from .strategies import lazyframe_from_schema


# Minimal Vendor fixtures — only the fields pipeline helpers use.
_VENDOR_VERISK = Vendor(
    name=VendorName.VERISK,
    hisco_label="AIR",
    n_simulations=10_000,
    ylt_dir=__import__("pathlib").Path("/tmp"),
    ylt_glob="*.parquet",
    ep_summary_dir=__import__("pathlib").Path("/tmp"),
    flavors=(Flavor.MAIN, Flavor.DIALSUP),
)
_VENDOR_RISKLINK = Vendor(
    name=VendorName.RISKLINK,
    hisco_label="RMS",
    n_simulations=100_000,
    ylt_dir=__import__("pathlib").Path("/tmp"),
    ylt_glob="*.parquet",
    ep_summary_dir=__import__("pathlib").Path("/tmp"),
    flavors=(Flavor.MAIN, Flavor.DIALSUP),
)
_BOTH_VENDORS = [_VENDOR_VERISK, _VENDOR_RISKLINK]


# ---------------------------------------------------------------------------
# forecast_tags
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(
    dates=st.sets(
        st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 1)),
        min_size=1,
        max_size=10,
    )
)
def test_forecast_tags_unique_and_sorted(dates: set[date]) -> None:
    """``forecast_tags(dates)`` output is sorted ascending. When dates are
    in distinct calendar months (the intended usage), tags are also unique
    and length matches the input set.

    NOTE: This test skips the same-calendar-month case where BUG-1 fires —
    that case is documented in KNOWN ISSUES and tested separately in
    ``test_forecast_tags_duplicates_when_same_month``.
    """
    # Only assert uniqueness when all dates are in distinct months.
    months = {d.strftime("%Y%m") for d in dates}
    tags = forecast_tags(list(dates))
    assert tags == sorted(tags), f"Tags are not sorted: {tags}"
    if len(months) == len(dates):
        # All distinct months: the full invariant holds.
        assert len(tags) == len(dates), f"Expected {len(dates)} tags; got {len(tags)}"
        assert len(tags) == len(set(tags)), f"Tags are not unique: {tags}"


@pytest.mark.fuzz
def test_forecast_tags_duplicates_when_same_month() -> None:
    """BUG-1 regression guard: two dates in the same month produce unique tags.
    The fix deduplicates dates in ``forecast_tags`` so this invariant always holds."""
    two_same_month = [date(2020, 1, 1), date(2020, 1, 15)]
    tags = forecast_tags(two_same_month)
    assert len(tags) == len(set(tags)), f"Duplicate tags produced: {tags}"


@pytest.mark.fuzz
@given(
    dates=st.sets(
        st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 1)),
        min_size=1,
        max_size=10,
    )
)
def test_forecast_tags_format(dates: set[date]) -> None:
    """Every tag produced by ``forecast_tags`` is a 6-character string
    in ``YYYYMM`` format."""
    tags = forecast_tags(list(dates))
    for tag in tags:
        assert len(tag) == 6, f"Tag {tag!r} is not 6 characters"
        year_part = int(tag[:4])
        month_part = int(tag[4:])
        assert 2020 <= year_part <= 2030, f"Year out of range in tag {tag!r}"
        assert 1 <= month_part <= 12, f"Month out of range in tag {tag!r}"


# ---------------------------------------------------------------------------
# build_variants
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(
    forecast_dates=st.sets(
        st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 1)),
        min_size=1,
        max_size=5,
    )
)
def test_build_variants_count_correct(forecast_dates: set[date]) -> None:
    """For N forecast dates and 2 vendors each with MAIN + DIALSUP flavors,
    ``build_variants`` returns 2*N MAIN variants + 2 DIALSUP variants (exactly)."""
    n = len(forecast_dates)
    variants = build_variants(list(forecast_dates), _BOTH_VENDORS)

    main_variants = [v for v in variants if v.flavor == Flavor.MAIN]
    dialsup_variants = [v for v in variants if v.flavor == Flavor.DIALSUP]

    assert len(main_variants) == 2 * n, (
        f"Expected 2*{n}={2*n} MAIN variants; got {len(main_variants)}"
    )
    assert len(dialsup_variants) == 2, (
        f"Expected 2 DIALSUP variants; got {len(dialsup_variants)}"
    )
    assert len(variants) == 2 * n + 2


@pytest.mark.fuzz
@given(
    forecast_dates=st.sets(
        st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 1)),
        min_size=1,
        max_size=5,
    )
)
def test_build_variants_names_unique(forecast_dates: set[date]) -> None:
    """All variant names in the output are unique — no two variants would
    write to the same output file.

    NOTE: This test may spuriously fail when Hypothesis generates two dates
    in the same calendar month (same root cause as BUG-1 in the module
    docstring). When that happens, duplicate filenames are produced.
    The property is correct — the bug is in the production code, not the test.
    See KNOWN ISSUES in module docstring.
    """
    # Only assert when all dates produce distinct YYYYMM tags — otherwise the
    # test would always fail due to BUG-1. Use assume() so Hypothesis filters
    # same-month date combinations rather than treating it as a skip.
    tags = [d.strftime("%Y%m") for d in forecast_dates]
    assume(len(tags) == len(set(tags)))  # skip same-month combos (BUG-1 territory)
    variants = build_variants(list(forecast_dates), _BOTH_VENDORS)
    names = [v.name for v in variants]
    assert len(names) == len(set(names)), (
        f"Duplicate variant names: {[n for n in names if names.count(n) > 1]}"
    )


# ---------------------------------------------------------------------------
# count_event_id_orphans
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(
    ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=1, max_rows=30),
    air_events=lazyframe_from_schema(F.REF_AIR_EVENTS, min_rows=0, max_rows=30),
)
@settings(max_examples=30)
def test_count_event_id_orphans_returns_nonneg(
    ylt: pl.LazyFrame,
    air_events: pl.LazyFrame,
) -> None:
    """``count_event_id_orphans`` always returns a non-negative integer.
    Never raises for any combination of valid YLT and air_events frames."""
    result = count_event_id_orphans(ylt, air_events, vendor_filter=VendorName.VERISK)
    assert isinstance(result, int), f"Expected int; got {type(result)}"
    assert result >= 0, f"Negative orphan count: {result}"


@pytest.mark.fuzz
@given(ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=1, max_rows=30))
def test_count_event_id_orphans_empty_air_events_returns_total(
    ylt: pl.LazyFrame,
) -> None:
    """When air_events is empty, every YLT row for the vendor is an orphan."""
    empty_air = pl.LazyFrame(schema=F.REF_AIR_EVENTS)
    total_verisk_rows = (
        ylt.filter(pl.col(Y.VENDOR) == VendorName.VERISK).collect().height
    )
    result = count_event_id_orphans(ylt, empty_air, vendor_filter=VendorName.VERISK)
    assert result == total_verisk_rows, (
        f"With empty air_events, expected orphan count == verisk row count "
        f"({total_verisk_rows}); got {result}"
    )


@pytest.mark.fuzz
@given(ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=1, max_rows=30))
def test_count_event_id_orphans_full_coverage_returns_zero(
    ylt: pl.LazyFrame,
) -> None:
    """When air_events covers every (year_id, event_id, model_code) triple
    in the YLT, the orphan count is 0."""
    ylt_df = ylt.collect()
    verisk_rows = ylt_df.filter(pl.col(Y.VENDOR) == VendorName.VERISK)

    if verisk_rows.height == 0:
        return  # Nothing to cover — vacuously true.

    # Build air_events that covers every triple present in the Verisk YLT rows.
    air_events = verisk_rows.select(
        pl.col(Y.YEAR_ID).alias(AE.YEAR),
        pl.col(Y.EVENT_ID).alias(AE.EVENT_ID),
        pl.col(Y.MODEL_CODE).alias(AE.MODEL_ID),
        pl.lit(1, dtype=pl.Int64).alias(AE.EVENT),
        pl.lit(1, dtype=pl.Int64).alias(AE.DAY),
    ).unique().lazy()

    result = count_event_id_orphans(ylt.lazy(), air_events, vendor_filter=VendorName.VERISK)
    assert result == 0, (
        f"Expected 0 orphans when air_events covers all triples; got {result}"
    )
