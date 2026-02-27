# Core Concepts


## Requirements
All source code is written in SQL, Python and relatively small number
of yaml files to enable orchestration and file structure.

## Technology
**Data modelling ELT**: Dbt - market standard for analytics and data modelling
**Data transform and ETL**: Dlt + Python
**Orchestration**: Dagu - lightweight FOSS alternative to Airflow
**Soure control**: Git on Hiscox Bitbucket account
**Package management**: uv + packages.yml for dbt specific packages


## Overall design
Laiter uses dbt as the primary tool to organize the analysis.
Laiter always tries to leverage dbt best practice guides, if in doubt,
you may refer to the dbt website which adds further documentation to how
this project was built.

The workflow relies on seed files and sources. Seed files are the lookup/reference
tables that change infrequently, they are stored as .csv files and are source
controlled.

Sources are larger data sources, not in source control which are pulled into
the project either as raw cat modelling data sources or larger reference tables
that could be updated by third-party departments and therefore need to be
refreshed.

## ETL Process
This is not an ETL project. it relies on clean data coming in. ETL is a
separate code base but part of the same rollup workstream.

## Custom Processes
The Risklink workflow and Global Exposures are custom workflows.


- Risklink: Takes an ELT as a source, a RMS Simulation Set as a source
and runs a custom Python utility to create the Risklink YLT.

- Global Exposures: Takes an EDM and the Global Exposures shapefiles as a source
and runs custom Python utility to create the exposures.

- Lloyd's RDS: Uses the Risklink and Verisk modelling sources to extract
the RDS

## Recommended skills/knowledge
Users will prefer to have knowledge of Python and SQL.
In particular users will need to know how to use command line
tools for installing packages via uv and running dbt commands.

**Most of the commands will be run automatically or served to user
in cli tool** so this will not be necessary unless there are
significant changes required to the process.

Laiter was designed to deal with reasonable process changes
using different data sources or applying adjustment factors
in various ways.
