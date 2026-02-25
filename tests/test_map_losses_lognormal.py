import sys
from pathlib import Path
import numpy as np
import polars as pl
from scipy.stats import norm


import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

from simulation__generate_losses_elt_to_base_ylt import map_losses


def _expected_lognormal_losses(
    meanloss: np.ndarray, stddev: np.ndarray, p: np.ndarray
) -> np.ndarray:
    mu = np.log(meanloss**2 / np.sqrt(stddev**2 + meanloss**2))
    sigma = np.sqrt(np.log(1 + (stddev**2 / meanloss**2)))
    z = norm.ppf(p)
    return np.exp(mu + sigma * z)


def test_map_losses_lognormal_matches_expected_values() -> None:
    ylt = pl.DataFrame(
        {
            "yearid": [1, 1, 1],
            "eventid": [1, 2, 3],
            "p_value": [0.5, 0.5, 0.5],
        }
    )
    elt = pl.DataFrame(
        {
            "eventid": [1, 2, 3],
            "meanloss": [100.0, 100.0, 100.0],
            "stddev": [5.0, 20.0, 60.0],
            "expvalue": [1_000.0, 1_000.0, 1_000.0],
        }
    )

    result = map_losses(ylt, elt, dist_type="lognormal")
    expected = _expected_lognormal_losses(
        meanloss=elt["meanloss"].to_numpy(),
        stddev=elt["stddev"].to_numpy(),
        p=ylt["p_value"].to_numpy(),
    )

    assert np.allclose(result["loss"].to_numpy(), expected, rtol=1e-10, atol=1e-10)


def test_map_losses_lognormal_spread_increases_with_stddev() -> None:
    ylt = pl.DataFrame(
        {
            "yearid": [1, 1, 1, 1, 1, 1],
            "eventid": [1, 1, 2, 2, 3, 3],
            "p_value": [0.1, 0.9, 0.1, 0.9, 0.1, 0.9],
        }
    )
    elt = pl.DataFrame(
        {
            "eventid": [1, 2, 3],
            "meanloss": [100.0, 100.0, 100.0],
            "stddev": [5.0, 20.0, 60.0],
            "expvalue": [1_000.0, 1_000.0, 1_000.0],
        }
    )

    result = map_losses(ylt, elt, dist_type="lognormal")
    loss_by_event_p = {
        (row["eventid"], row["p_value"]): row["loss"]
        for row in result.select(["eventid", "p_value", "loss"]).iter_rows(named=True)
    }

    spread_low = loss_by_event_p[(1, 0.9)] - loss_by_event_p[(1, 0.1)]
    spread_mid = loss_by_event_p[(2, 0.9)] - loss_by_event_p[(2, 0.1)]
    spread_high = loss_by_event_p[(3, 0.9)] - loss_by_event_p[(3, 0.1)]

    assert spread_low < spread_mid < spread_high
