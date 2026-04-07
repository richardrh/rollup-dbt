# What is Laiter
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
|               | Automatically loads data to CDS Staging | yes |

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



# Things to arrange in the project:


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


## Excali draw project diagram
https://excalidraw.com/#json=vS8H21UObanim0APNXAE0,4KeJKrmS5EJH1eBi7v8P3


## Other issues
- Retail needs a utils library that can extract, perform agg calculations,
calculate EP curves for various grains.

- Need to understand the databricks capture process for UK, EU

# Proposed workflow
### Phase 1
#### Workstream A: Tech debt cleanup
Cleaning up existing January 2026 rollup

#### Workstream B: Fact find
Fact finding:
    1. How are other teams in Hiscox solving issues if possible?
    2. Find out how the source rollup data was generated
    3. Find out what GC do to the data before modelling
    4. Collate the likely data sources for UK/EU
    5. How international perils are generated and vor
    6. Tomcat replacement?

### Phase 2
#### Workstream A: Warehouse
Generate rationale input seed values
Hisco lib/utils build

#### Workstream B: ETL Build
Generate ETL, pre and post-modelling tools
to format data pre-warehouse

### Workstream C: Responsibilities
Agree which team is responsible for running the process
in future


### Phase 3
#### Workstream A: Testing
- Source 1.4.2026 data from above sources


#### Workstram B: Nice-to-haves
Various EDM outputs

# Issues with proposed workflow to be dealt with
## Tomcat
- Tomcat replacement: Tomcat's functionality was replaced by
Retail tool that maps to RMS Simulation set in an automated workflow
Runtime approx 3seconds and cut down the need for
an analyst to use the system/click through GUI.

- London Market have a different process again.

## Databricks system
- Limited implementation, but this would be the ideal
system +/- snowflake to stage data in and out

- January 2026 rollup used Duckdb and continue to work
on Duckdb as Databricks is not currently operational fo me
Duckdb runs the rollup in c.1-15minutes end to end.

Depends upon network / citrix speed

##



# Proposal: Fixing the Retail Rollup Process (UK & EU)

## Executive Proposal

Hiscox Retail UK & EU currently operates a rollup process that is
**slow, fragile, opaque, and operationally risky**.
Key modelling steps are implicit, manual, and difficult to reproduce or validate.

Additionally as the business evolves the required analysis changes which means
the process is difficult to reproduce, examples of this are:
- Model settings changing year to year
- Naming conventions, entities, portfolios changing
- Processes to generate inputs like forecast factors are undefined and change year to year


As a result, the business is exposed to:
- Unclear ownership and application of modelling adjustments
- Limited auditability
- Difficulty explaining *why* results changed between runs
- Friction and rework at CDS ingestion if requirements or inputs change

To overcome these issues in full there are a combination of things Hiscox could do:
### Agree how the business structure is defined
This step means Hiscox/Retail will define how the business is structured. The
naming conventions used and by extension the grain at which various inputs
are to be defined. Immediate examples of this are:
- Define a process for the grain and input data used to generate forecast factors
and do this programmatically with human review.

### Implement checkpoints which MUST pass
Human in the loop checkpoitns should be officially defined during the
rollup process. The managerial and human review should happen early-on in
the process and not only at the end.o

### Define data sources and responsibilities
Document this - in modern documentation systems which can be audited and traced
where data is derived, who produced it and when.

### Implement appropriate technologies
This proposal delivers a **modernised Retail rollup capability**
that turns the rollup from a *bespoke analyst exercise* into
a **repeatable, governed modelling process**.

---

## What Hiscox Will Be Able To Do
The intention of the project should be to build the base for a rollup
that can be performed more frequently (quarterly) and is open to be translated
to other systems to meet Hiscox's longer term goals.

## What Hiscox Will Explicitly *Not* Get
This project is deliberately scoped to avoid over‑reach.

- ❌ It does **not** unify process between Retail and other teams
- ❌ It does **not** require Databricks to be fully operational on day one

Instead, it **stabilises and professionalises** the Retail rollup layer that already exists.


## What Hiscox shuld expect
Catch issues earlier - whether that be in the source data sets or
modelling outputs.

Retail team internally defines its' review practices and produces sign off
templates which MUST be done prior to uploading to CDS.


### 1. Run Retail Rollups Reliably and Repeatedly
- Execute the full UK/EU Retail rollup on demandd
- Re-run prior periods and explain differences
- Produce consistent outputs regardless of who runs the process

### 2. Explain Results — Not Just Produce Them
- Trace losses from raw model output → blended → forecast → CDS
- Show exactly which factors moved results and why
- Provide defensible explanations to Group, Risk, and Finance

### 3. Control and Govern Modelling Adjustments
- Manage blending, forecast, EUWS, FX, and custom factors as **data**
- Apply scenario-based forecasts without code changes
- Override factors deliberately, visibly, and with sign‑off

### 4. Reduce Operational and Key‑Person Risk
- Remove dependency on manual systems and undocumented steps
- Reduce reliance on specialist “how it works” knowledge
- Make the process supportable as a BAU activity

### 5. Catch Issues Earlier
- Validate event IDs, EP curves, and aggregates *before* CDS export
- Review results at agreed checkpoints, not after the fact
- Avoid late-stage rework and failed CDS ingestions

---

## What Hiscox Will Explicitly *Not* Get

This project is deliberately scoped to avoid over‑reach.

- ❌ It does **not** redesign vendor models (Verisk / Risklink)
- ❌ It does **not** change Group Cat methodologies
- ❌ It does **not** unify Retail and London Market processes
- ❌ It does **not** require Databricks to be fully operational on day one

Instead, it **stabilises and professionalises** the Retail rollup layer that already exists.

---

## How This Moves the Dial Forward

| Dimension | Before | After |
|--------|-------|------|
| Reproducibility | Low | High |
| Transparency | Implicit | Explicit & documented |
| Speed | Analyst / system dependent | Minutes, repeatable |
| Governance | Informal | Seed- and checkpoint-driven |
| Auditability | Limited | Built-in artefacts |
| Operational Risk | High | Materially reduced |

This is a **capability uplift**, not just a technical refactor.

---

## Key Deliverables to the Business

By the end of the project, Hiscox will have:

1. **A defined Retail rollup process**
   - Inputs, transformations, outputs clearly documented

2. **A production-ready rollup pipeline**
   - From vendor outputs to CDS Staging

3. **Governed adjustment logic**
   - Blending, forecast, EUWS, FX, custom factors

4. **Results verification artefacts**
   - EP curves and summaries at each stage

5. **Clear ownership and run responsibility**
   - Enabling BAU execution

---

## Dependencies & Preconditions

### Business Dependencies
- Agreement on:
  - Blending methodology
  - Forecast factor usage
  - EUWS handling and override policy
- Alignment with CDS on acceptance criteria

### Data & System Dependencies
- Access to Verisk and Risklink outputs
- Access to CDS event ID reference data
- Stable CDS Staging environment

### Organisational Dependencies
- Named owners for:
  - Factor governance
  - Run execution
  - Result sign‑off

---

## Key Challenges & How They Are Addressed

### Legacy Knowledge Risk
- Challenge: Important logic exists only in analyst workflows
- Response: Structured fact‑finding and side‑by‑side validation

### Adjustment Complexity
- Challenge: Multiple interacting factors can obscure impact
- Response: Explicit sequencing, defaults, and EP checkpoints

### Change Adoption
- Challenge: Trusting automation over manual review
- Response: Human‑in‑the‑loop checkpoints retained by design

### Platform Constraints
- Challenge: Databricks availability is limited
- Response: Local-first design with future portability

---

## Success Criteria (Business-Focused)

The project is successful when:

- Retail rollups can be run confidently without specialist intervention
- Result movements can be clearly explained and defended
- CDS ingestion becomes routine rather than a risk point
- The process is owned, documented, and supportable long‑term

---

*This proposal turns the Retail rollup from a fragile operational task into a governed modelling capability that the business can rely on.*
