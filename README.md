# Proposal: Fixing the Retail Rollup Process (UK & EU)

## Executive Proposal

Hiscox Retail UK & EU currently operates a rollup process that is **slow, fragile, opaque, and operationally risky**. Key modelling steps are implicit, partially manual, and difficult to reproduce or validate. As a result, the business is exposed to:

- Unclear ownership of modelling adjustments
- Limited auditability of results
- High dependency on individual analysts and legacy tooling
- Difficulty explaining *why* results changed between runs
- Friction and rework at CDS ingestion

This proposal delivers a **modernised Retail rollup capability** that turns the rollup from a *bespoke analyst exercise* into a **repeatable, governed modelling process**.

The focus is not on introducing a new tool for its own sake, but on **changing what the business can reliably do** with Retail model data.

---

## What Changes for Hiscox

### Today
- Rollups are hard to reproduce end‑to‑end
- Adjustments (blending, forecast, EUWS, FX) are difficult to trace
- Manual systems (e.g. Tomcat) create bottlenecks and operational risk
- Results are reviewed late, often after CDS ingestion issues arise
- Knowledge of “how it works” sits with a small number of individuals

### After This Project
- Retail rollups become **repeatable, explainable, and auditable**
- Adjustments are **explicit, configurable, and versioned**
- Manual GUI steps are removed from the critical path
- Results can be reviewed **before** CDS ingestion
- Ownership shifts from individuals to a **defined process**

---

## What Hiscox Will Be Able To Do

### 1. Run Retail Rollups Reliably and Repeatedly
- Execute the full UK/EU Retail rollup on demand
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
