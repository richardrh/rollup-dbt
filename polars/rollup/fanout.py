"""Compatibility wrapper for Hisco mart fanout models.

New code should import from :mod:`rollup.marts` or :mod:`rollup.marts.hisco`.
"""

from rollup.marts.hisco import fanout_hisco

__all__ = ["fanout_hisco"]
