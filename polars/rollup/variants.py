"""Compatibility wrapper for mart variant definitions.

New code should import from :mod:`rollup.marts` or :mod:`rollup.marts.variants`.
"""

from rollup.marts.variants import VariantSpec, build_variants, forecast_dates_from_seed, forecast_tags

__all__ = [
    "VariantSpec",
    "build_variants",
    "forecast_dates_from_seed",
    "forecast_tags",
]
