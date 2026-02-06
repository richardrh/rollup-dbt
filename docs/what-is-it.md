# What is HREU

ReTale is a periodic rollup analytics project for the HISCO UK and EU
Retail modelling team.

## Core capabilities
ReTale is designed to be run locally (or on Databricks with minor
modifications). It uses modern (2026) analytics platforms to
manage data sources, seed and reference data and output
the required analysis for Hiscox group modelling.

The ReTale project:
- Defines its' required data sources, e.g. exposure and catastrophe
modelling data.
- Automatically runs and transfers data to CDS staging tables
- Automatically produces RDS, Global Exposures and modelling
- Applies Hiscox VOR, forecast factors and contains a
placeholder transformation step that can be implemented to run
future VOR or future modifications.
- Makes rationale technology choices that can be used locally or
compatible with Hiscox Databricks.

## Requirements
All source code is written in SQL, Python and relatively small number
of yaml files to enable orchestration and file structure.

## Technology
Orchestration: Dagu - lightweight FOSS alternative to Airflow
Data modelling ELT: Dbt - market standard for analytics and data modelling
Data transform and ETL: Dlt + Python
soure control: Hiscox Bitbucket account
package management: uv

## Recommended skills/knowledge
Users will prefer to have knowledge of Python and SQL.
In particular users will need to know how to use command line
tools for installing packages via uv and running dbt commands.

**Most of the commands will be run automatically or served to user
in cli tool**
