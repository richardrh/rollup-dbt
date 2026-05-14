"""Compatibility wrapper for MAIN metric models.

New code should import from :mod:`rollup.intermediate.metrics`.
"""

from rollup.intermediate.metrics import add_main_metrics

__all__ = ["add_main_metrics"]
