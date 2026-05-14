"""Compatibility wrapper for intermediate factor models.

New code should import from :mod:`rollup.intermediate` or
:mod:`rollup.intermediate.factors`.
"""

from rollup.intermediate.factors import (
    MissingFxRateError,
    _blend_weights_by_peril_bucket,
    attach_currency,
    attach_euws,
    attach_forecast_factors,
    attach_rank,
    attach_uplift,
    validate_fx_coverage,
)

__all__ = [
    "MissingFxRateError",
    "_blend_weights_by_peril_bucket",
    "attach_currency",
    "attach_euws",
    "attach_forecast_factors",
    "attach_rank",
    "attach_uplift",
    "validate_fx_coverage",
]
