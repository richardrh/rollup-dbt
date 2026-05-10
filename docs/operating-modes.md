# Operating modes — how the pipeline decides which analyses to run

The rollup pipeline decides which vendor analyses execute via a **selection mechanism** that sits between the raw YLT data and the factor chain. Today it uses a two-file lookup (Mode A); the team is considering two future alternatives (Modes B and C) that trade flexibility for simplicity or vice versa.

This doc captures the current behaviour and both future options so the conversation has a shared reference before anyone implements.

---

## Mode A (current): "Supply many, filter via lookup"

The pipeline ingests analyses from two sources and filters them via an inner-join.

### How it works

| Step | What | File | Rows | Purpose |
|---|---|---|---|---|
| 1. Load | Raw YLT (both vendors) | `data/ylt/{vendor}/*.parquet` | ~10 000 years × ~200 analyses | Loss events per simulation year & analysis |
| 2. Normalize | Both vendors → canonical schema | (staging) | (same) | Add peril_id, lob_id, lob metadata |
| 3. Join analyses | Look up peril + lob per analysis | `data/seeds/business/analyses.csv` | **424 rows** (7 Verisk + 417 RiskLink) | Metadata catalogue: every analysis the vendors COULD produce |
| 4. **Filter scope** | Keep only official (lob, vendor, analysis_id) triples | `data/seeds/business/rollup_scope.csv` | **95 rows** (all `in_rollup=True`) | Official run plan: which variants are canonical per LOB |

### Why two files?

**`analyses.csv`** is the **catalogue** — a stable, complete list of every analysis each vendor supports. Multiple analyses per (lob, peril) are allowed; for example, both `UK_WSSS` and `UK_WSSS_GCAdj` map to peril 206 but represent different model variants.

**`rollup_scope.csv`** is the **filter** — the official decision per rollup cycle on which (modelled_lob, vendor, analysis_id) triples are live. Only rows with `in_rollup=True` survive the inner-join in `apply_rollup_scope`.

The separation lets the team:
- Keep the full catalogue in git (a static, auditable reference)
- Change which variants are canonical each cycle (edit only `rollup_scope.csv`)

### Schema

#### analyses.csv (sample rows)

| vendor | analysis_id | modelled_label | peril_id | lob_id |
|---|---|---|---|---|
| verisk | EU_EQ | EU_EQ | 1 | (null) |
| verisk | UK_WSSS_GCAdj | UK_WSSS_GCAdj | 5 | (null) |
| risklink | 1 | EU FL HD | 2 | 38 |

Composite key: `(vendor, analysis_id)`.
`lob_id` is populated for RiskLink (one analysis = one (lob, peril)); NULL for Verisk (lob lives on the YLT row).

#### rollup_scope.csv (sample rows)

| modelled_lob | vendor | analysis_id | in_rollup |
|---|---|---|---|
| S33_FA_Lloyds | verisk | EU_EQ | true |
| S33_FA_Lloyds | verisk | UK_WSSS_GCAdj | true |
| HIC_COMM_UK | risklink | EU FL HD | true |

Composite key: `(modelled_lob, vendor, analysis_id)` — the triple is unique.
The grain is **analysis_id**, not peril_id, so two analyses sharing a peril_id can be individually toggled.

### Code

The filter happens in `polars/rollup/stages/staging.py::apply_rollup_scope` (line 214–245). It is called in the pipeline chain at `polars/rollup/pipeline.py::build_all_factors` (line 229).

The join is keyed on `(modelled_lob, vendor, modelled_region_peril)` — a readable triple without lookups — and is an inner-join, so any row not in `rollup_scope` drops out.

---

## Mode B (future option 1): "Supply only what should run"

Eliminate `rollup_scope.csv` entirely. The user provides only the analyses they want run inside `analyses.csv`. No filter step.

### Change summary

| Component | Today | In Mode B |
|---|---|---|
| `analyses.csv` | Stable catalogue (424 rows) | **Live run plan** (e.g. ~95 rows) |
| `rollup_scope.csv` | Filter lookup (95 rows) | **Deleted** |
| `RollupScopeCol` enum | Defines rollup_scope schema | **Deleted** |
| `apply_rollup_scope` function | Inner-join stage | **Deleted** |
| `Seeds.rollup_scope` field | Loaded from seed | **Removed** |
| Pipeline chain | ... → normalize → join scope → normalize factors ... | ... → normalize → attach factors ... |

### Trade-offs

**Advantages:**
- Simpler mental model — one source of truth, no filter dance
- Easier to reason about — the catalogue IS the run plan
- Fewer moving parts — one less seed, one less enum, one less pipeline stage

**Disadvantages:**
- Loses the ability to track "potential" analyses for future use. For example, you can no longer keep `UK_WSSS_GCAdj` in the catalogue as a known-but-not-active alternative; deleting it forgets that the variant exists
- Adding/removing analyses each cycle means editing `analyses.csv` — a more sensitive seed (today it's stable across cycles)
- Harder to diff "what changed this cycle?" — the entire catalogue changes if you add/remove one analysis

### Migration path

The team would need to commit to "the catalogue equals the live run at all times." This is a change in how `analyses.csv` is owned: from static reference to cycle-dependent artifact.

To migrate:
1. Inline `rollup_scope.csv` into `analyses.csv` (union of in-rollup rows only)
2. Delete `rollup_scope.csv` from `data/seeds/business/`
3. Remove `RollupScopeCol`, `apply_rollup_scope`, and the seed from the codebase
4. Update pipeline chain to skip the scope filter

---

## Mode C (future option 2): "Pass analysis IDs at runtime"

Keep `analyses.csv` as the metadata catalogue, drop `rollup_scope.csv`, and add a CLI flag to specify scope.

### Usage

```bash
# Supply a comma-separated list
rollup --analysis-ids 1,2,3,42,99 --yes

# Or supply a text file (one ID per line)
rollup --analysis-ids-file scope_2026Q1.txt --yes
```

The pipeline reads the IDs, looks them up in `analyses.csv` to get the peril/lob metadata, then filters the YLT to those IDs only.

### Change summary

| Component | Today | In Mode C |
|---|---|---|
| `analyses.csv` | Stable catalogue (424 rows) | **Unchanged** — metadata reference |
| `rollup_scope.csv` | Filter lookup (95 rows) | **Deleted** |
| CLI | `rollup --yes` | `rollup --analysis-ids <IDs \| file> --yes` |
| `_cmd_run` function | Reads seeds from disk | **Also reads `--analysis-ids` flag** |
| `apply_rollup_scope` call | Passed `rollup_scope` seed | **Replaced with `apply_analysis_ids(ids=[...])`** |

Implementation sketch:
1. New argument in `cli.py::_build_parser`: `--analysis-ids` (comma-separated) and `--analysis-ids-file` (text file path)
2. `_cmd_run` parses the flag and builds an ID list
3. New function `apply_analysis_ids(ylt, ids: list[str]) → ylt_filtered` in `stages/staging.py`
4. Pipeline calls `apply_analysis_ids` instead of `apply_rollup_scope`
5. `Seeds.rollup_scope` becomes optional (or reads from an env var as fallback)

### Trade-offs

**Advantages:**
- Maximum flexibility — different runs, different scope, without editing seeds
- Trivial to script different rollup cycles or ad-hoc what-if runs
- The list of IDs becomes a tracked artefact: commit `scope_2026Q1.txt` to git per cycle, so you have an audit trail
- The catalogue (`analyses.csv`) stays stable and reference-only

**Disadvantages:**
- No persistent record in `data/seeds/` of what scope was last run — you must check the committed scope file
- Trusts the operator — if they pass a wrong ID (e.g. a typo), there is no validation against "officially approved" analyses
- Adds a required CLI argument (though `--analysis-ids-file` could read from `data/seeds/` by convention)

---

## Comparison table

| | Mode A (today) | Mode B (catalogue = live) | Mode C (CLI IDs) |
|---|---|---|---|
| **Files** | analyses.csv + rollup_scope.csv | analyses.csv only | analyses.csv only |
| **Scope source** | rollup_scope.csv seed | implicit (catalogue) | CLI flag `--analysis-ids` or `--analysis-ids-file` |
| **Audit trail** | Both files in git history | analyses.csv history only | Committed scope files (e.g. `scope_2026Q1.txt`) in git |
| **Cycle-to-cycle change** | Edit rollup_scope.csv (safe) | Edit analyses.csv (sensitive) | Create new scope file (safe) |
| **Can track "future" analyses?** | Yes — keep unused rows in catalogue | No — must delete to remove | Yes — keep in analyses.csv, omit from scope file |
| **Best for** | Many model variants per LOB | Small fixed set; never changes | Frequent scope changes; rapid iteration |

---

## When to switch

**Mode A is right when the catalogue is significantly larger than the active set.** Today: 424 catalogue rows, 95 in-rollup rows — Mode A earns its weight because it separates concerns and lets you see the "dark matter" (known-but-inactive analyses).

**Mode B becomes attractive** once the team treats `analyses.csv` as the single source of truth and rarely deactivates rows. If the catalogue is already ~95 rows and stable, the extra filter adds no value.

**Mode C becomes attractive** if scope changes more frequently than the catalogue (e.g. every month or per-portfolio run). The CLI makes scope a first-class runtime decision, separate from the seed catalog.

---

## Recommendation

No recommendation yet — the team will decide based on operational needs. This doc exists so the conversation has a shared, detailed reference.
