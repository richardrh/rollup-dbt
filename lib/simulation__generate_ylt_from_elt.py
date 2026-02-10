import polars as pl
import numpy as np


def simulate_ylt(
        elt: pl.DataFrame,
        n_sims: int,
        seed: int | None = None
) -> pl.DataFrame:
    """
        Generate simulated YLT from ELT using Poisson model

        Args:
            elt: Data frame with columns [eventid, rate]
            n_sims: number of simulations
            seed: random seed

        Returns:
            pl Dataframe with columns [yearid, eventid, p_value]
    """

    rng = np.random.default_rng(seed)

    event_ids=elt["eventid"].to_numpy()
    rates = elt["rate"].to_numpy()

    total_occurrences = rng.poisson(rates * n_sims)
    event_ids_expanded = np.repeat(event_ids, total_occurrences)
    n_total = total_occurrences.sum()

    print("simulating years")
    years = rng.integers(1, n_sims + 1 , size=n_total)
    print("simulating p-values")
    p_values = rng.uniform(0, 1, size=n_total)
    return pl.DataFrame({
                        "yearid": years,
                        "eventid": event_ids_expanded,
                        "p_value": p_values
    })


def test_simulate_ylt():
    elt = pl.DataFrame({"eventid": [1,2,3], "rate":[0.1,0.5, 1.0]})
    n_sims=1_000_000
    n_trials=5
    occ_per_trial = []
    for seed in range(n_trials):
        ylt=simulate_ylt(elt, n_sims, seed=seed)
        counts=ylt.group_by("eventid").len().sort("eventid")
        occ_per_trial.append(counts["len"].to_numpy())
    occs = np.array(occ_per_trial)
    expected=elt["rate"].to_numpy() * n_sims

    # mean matches poisson
    empirical_means = occs.mean(axis=0)
    for i, (emp, exp) in enumerate(zip(empirical_means, expected)):
        pct_error = abs(emp - exp) / exp
        assert pct_error < 0.02

    # var matches poisson
    empirical_vars = occs.var(axis=0)
    for i, (var, exp) in enumerate(zip(empirical_vars, expected)):
        ratio = var/exp
        assert 0.5 < ratio < 2.0

        # chi-sq test for uniformity
    expected_per_year = len(ylt) / n_sims
    chi2, p_value = stats.chisquare(year_counts, f_exp=[expected_per_year] * len(year_counts))
    assert p_value > 0.01

    # p-values are uniform kolmogorov-sminov test
    ks_stat, p_value = stats.kstest(ylt["p_value"].to_numpy(), "uniform")
    assert p_value > 0.01

    # sanity check
    total=len(ylt)
    expected_total = elt["rate"].sum() * n_sims
    pct_error = abs(total - expected_total) / expected_total
    assert pct_error < 0.05

if __name__ == "__main__":
    elt = pl.DataFrame({
                       "eventid": [1, 2,3,4,5],
                       "rate": [0.0002, 0.00012, 0.0002, 0.005, 0.0009]
    })
    n_sims = 10_000_000
    ylt = simulate_ylt(elt, n_sims, seed=42)

    ylt.head()

    ylt.write_csv('ylt.csv')
    print(ylt)
