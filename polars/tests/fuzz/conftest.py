"""Hypothesis profile configuration for fuzz tests.

Three profiles are registered:
- ``dev``   (default): 50 examples per property — fast enough for the
  normal ``pytest`` loop when ``--run-fuzz`` is given.
- ``ci``:   500 examples, no per-test deadline — used in CI via
  ``HYPOTHESIS_PROFILE=ci pytest --run-fuzz``.
- ``debug``: 10 examples, verbose — useful when shrinking a failure.

The active profile is selected from the ``HYPOTHESIS_PROFILE`` environment
variable, falling back to ``dev`` when it is not set.
"""

from __future__ import annotations

import os

from hypothesis import HealthCheck, Verbosity, settings


settings.register_profile(
    "dev",
    max_examples=50,
    suppress_health_check=[HealthCheck.too_slow],
)

settings.register_profile(
    "ci",
    max_examples=500,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

settings.register_profile(
    "debug",
    max_examples=10,
    verbosity=Verbosity.verbose,
    suppress_health_check=[HealthCheck.too_slow],
)

settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))
