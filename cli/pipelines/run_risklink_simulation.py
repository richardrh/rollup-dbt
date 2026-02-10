"""Run Risklink simulation and load result to staging."""

from __future__ import annotations

import sys
from pathlib import Path


# TODO: Fix this so it correctly imports straight from lib
# Add lib directory to path for simulation imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "lib"))

import polars as pl
import dlt

from config.loader import load_config
from simulation__generate_ylt_from_elt import simulate_ylt
from simulation__generate_losses_elt_to_base_ylt import map_losses


def run_risklink_simulation(
    elt_table: str | None = None,
    output_table: str | None = None,
) -> None:
    """
    Run Risklink simulation on loaded ELT data.

    This function:
    1. Reads ELT from staging table
    2. Runs Poisson simulation to generate YLT
    3. Maps p-values to losses using lognormal/beta distribution
    4. Loads result to staging table
    """
    config = load_config()
    elt_table = elt_table or config["staging"]["risklink_elts_table"]
    output_table = output_table or config["staging"]["risklink_ylts_table"]

    print(f"Running Risklink simulation...")
    print(f"  Simulations: {config['simulation']['n_simulations']:,}")
    print(f"  Random seed: {config['simulation']['random_seed']}")
    print(f"  Distribution: {config['simulation']['distribution_type']}")

    # TODO: Connect to database and read ELT
    # For now, this is a stub that shows the workflow
    print(f"  Reading from: {elt_table}")
    print(f"  Writing to: {output_table}")

    # Placeholder - in reality would:
    # 1. Query ELT from database
    # 2. Run simulate_ylt()
    # 3. Run map_losses()
    # 4. Write to output_table

    print("\nSimulation workflow steps:")
    print("  1. Extract ELT (eventid, rate, meanloss, stddev, expvalue)")
    print("  2. Run Poisson simulation to generate year-event pairs")
    print("  3. Map p-values to losses using inverse CDF")
    print("  4. Load YLT to staging table")

    print("\n(Simulation integration pending - workflow steps shown)")


if __name__ == "__main__":
    run_risklink_simulation()
