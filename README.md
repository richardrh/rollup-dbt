Things to arrange in the project:

## ETL
How do we get Verisk and Risklink data into the dbt models.
Define dbt sources, load an ETL process that creates the stg_ tables

## Risklink ETL
The risklink process should run the simulation as part of the ETL process
this way we aren't actually loading the event loss table or summary ep.

We are going to get a list of analysis ids and database/flat file location
extract the event loss table then use
./lib/simulation__gernerae losses from elt to base ylt - to simulate
and generate an output format that looks similar to verisk.

once generated we can then import to stg_

we need a table or parameter in config that stores the number of sims
for risklink.

## Verisk ETL
the event loss mapping process is not necessary for verisk.

number of sims is always 10000.

load straight from flat file into stg_


## Post stg_ for verisk and risklink
once the YLTs for both are loaded we run an ep summary calculation
this generates a summary table for each which can be compared.

## Blending process
The first analysis step is to join the two ep summary outputs from
verisk and risklink. then join this to the blending factors seed table.

Before doing this we want to generate the intermediate models for
the stg_ ylt inputs and also join the input lobs and region/perils to
the seed tables in hisco-org. This will give us unique hash values
for lob and region_peril. we can then get rid of those columns while
we process the next several steps and just retail lob_id and region_peril_id

## blending factors
blending process applies the weighting in the vor__blending_factors seed table

## forecast factors
we also in the subsequent steps apply forecast factors in
forecast-factors seed table. this can be done via a cross join
as this will allow an arbitrary number of forecast factors
to be applies simultaneously.

## post blending and forecast
these are the only steps that can be done on ep summary sheet.
technically some custom factors could be, but we will keep them in one
section of the analysis sequence.

once the blending and forecast is done we need to apply them to the
YLT. This is done by this means:
Get the YLT for both verisk and risklink, rank order it largest to
smallest.

ranke >= 10000 are called the rp_bucket = 10000
ranks >= 1000 are rp_bucket = 1000
ranks >= 200 are rp_bucket = 200
ranks < 200 are rp_bucket = 0

there might be a better way of doing this. where we specify the buckets
derived from the vor blending factors seed table, since they should align.

using this rp_bucket column we can then join to the return period (rp column)
in the blending and forecast output and this gives us the blending factor
to apply on an event basis.

## custom factors
a series of configurable custom factors is contained within vor seed
schema. each table allows users to customize adjustment factors
at different levels of grain.

// TODO: Unclear how to do this and run through series of custom factors
when a factor is not specified it should simply default to 1.0.

## fx rates
we have a very simple method that applies fx rates based on office
the office is specified in hisco-orgs in lobs or offices seed table.
we need to get the office from the modelled_lob in the verisk and
risklink stg_ files - the ETL process needs to ensure the stg_
tables are cleansed so we get that modelled_lob column.

## euws rate factors
one of the final steps is to use a euws rate factors table which is in
the vor seed folder. this simply tells the analysis that there are some
events that will be zero'd out. an event not in this table at all will
incur a 1.0 factor (i.e. no change)

## custom eu ws rate factor logic
a final step is to override the euws rate factors
for certain lob_ids if the rate factors are having too much impact
where they shouldn't be.

I'd therefore like to add a custom eu ws rate factor override table.
separate from the custom vor factors as this specifically deals with this
one issue and having it there sign posts it to users.


# sources
some sources we need to deal with are:
- the aforementioned risklink and verisk source files / grabbing this data
dynamically from a database or flat file.

- dynamically updating the euws rate factors

- pulling through the model event id table from CDS.
this is used to validate we have the correct eventids and dayid
combinations prior to importing to the CDS system (where we terminate)

# destinations
the destination is a database called CDS Staging on prod-group-uw
sql server.

## DLT
i want to use dlt (data load tool) to define the source load and
destination exports in pipelines.

## orchestrator
use dagu to orchestrate the ETL process + risklink simulation
and the dlt pipelines in and out of dbt models.
it is similar to airflow but without the hassle and a good option
for small single department projects.

## data verification
leverage dbt data checks, dlt data checks as much as possible
to ensure clean formats.

## result verification
we need a generate ep curve calculator that can aggregate
using multiple aggregation columns, aggregation sets or something
induckdb is a good option for this.

at each transformation stage we need an ep curve summary output
generated and dumped to a results verification folder so we
can follow the process through.

## human in the loop
critical checkpoints we need a HITL check.
e.g. initial data load + initial ep curve summary
post blending process and review blending + forecast factors

check the HITL wants to apply custom vor or not
check if they want to apply euews override

check results before running the dlt export pipeline to
the CDS destination.



