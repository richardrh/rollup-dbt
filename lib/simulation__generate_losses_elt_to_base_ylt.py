import polars as pl
import numpy as np
from scipy.stats import norm, beta


def map_losses(
    ylt: pl.DataFrame, elt: pl.DataFrame, dist_type="lognormal"
) -> pl.DataFrame:
    """
    Map p-values to losses using inverse CDF of lognormal or beta, capped by expvalue.
    Args:
        ylt: Polars DF [yearid, eventid, date, p_value]
        elt: Polars DF [eventid, meanloss, stddev, expvalue]
        dist_type: "lognormal" or "beta"
    Returns:
        Polars DF with 'loss' column
    """

    assert dist_type in ["lognormal", "beta"]

    ylt = ylt.with_columns(pl.col("eventid").cast(pl.Int64))
    elt = elt.with_columns(pl.col("eventid").cast(pl.Int64))

    # Join parameters onto YLT
    df = ylt.join(elt, on="eventid", how="inner")

    p = np.clip(df["p_value"].to_numpy(), 1e-12, 1 - 1e-12)
    meanloss = df["meanloss"].to_numpy()
    stddev = df["stddev"].to_numpy()
    expvalue = df["expvalue"].to_numpy()

    if dist_type == "lognormal":
        # Convert mean/std to mu/sigma
        mu = np.log(meanloss**2 / np.sqrt(stddev**2 + meanloss**2))
        sigma = np.sqrt(np.log(1 + (stddev**2 / meanloss**2)))
        z = norm.ppf(p)
        losses = np.exp(mu + sigma * z)
    elif dist_type == "beta":
        # Method-of-moments for alpha/beta
        m = np.clip(meanloss / expvalue, 1e-12, 1 - 1e-12)
        v = np.maximum((stddev / expvalue) ** 2, 1e-12)
        t = np.maximum((m * (1 - m) / v) - 1, 1e-12)
        alpha = m * t
        beta_param = (1 - m) * t
        losses = beta.ppf(p, alpha, beta_param) * expvalue
    else:
        raise ValueError("dist_type must be 'lognormal' or 'beta'")

    # Cap by expvalue
    losses = np.minimum(losses, expvalue)

    return df.with_columns(pl.Series("loss", losses))
