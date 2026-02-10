# What is Laiter
Aside from being Retail nearly backwards.

Laiter is a periodic rollup analytics project for the HISCO UK and EU
Retail modelling team.

## Core capabilities
Laiter is designed to be run locally (or on Databricks with minor
modifications). It uses modern (2026) analytics platforms to
manage data sources, seed and reference data and output
the required analysis for Hiscox group modelling.

The Laiter project:

| Category | Feature | Yea/No |
| --- | ---- | --- |
| Data sources | Defines required data sources | yes |
|               | Connects to Hiscox central data stores | yes |

- Defines its' required data sources, e.g. exposure and catastrophe
modelling data.
- Automatically runs and transfers data to CDS staging tables
- Automatically produces RDS, Global Exposures and modelling
- Applies Hiscox VOR, forecast factors and contains a
placeholder transformation step that can be implemented to run
future VOR or future modifications.
- Makes rationale technology choices that can be used locally or
compatible with Hiscox Databricks.
- Outputs summary metrics and comparisons to previous Laiter
model runs before importing to CDS.
- Runs a simulation on top of Risklink ELT which bypasses the need
to use Hiscox Tomcat system
- Automatically formats and validates input data for CDS.

