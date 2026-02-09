import polars as pl
from simulation_inverse_cdf import map_losses

ylt = pl.read_csv("../data/simulation/European_Flood_V8_simulation_table.csv")
elt = pl.read_csv("../data/simulation/rdm_results.csv")

ylt = ylt.rename(
    {
        "EventId": "eventid",
        "SimulationPeriodIndex": "yearid",
        "LossQuantile": "p_value",
        "EventDate": "date",
    }
)
