# Data Sources

Raw catastrophe modelling data arrives via etlval, which ingests, validates, and pushes vendor files to a hive-partitioned parquet store on the filesystem. dbt reads these parquet files and transforms them into staging and mart layers.

---

## 1. Data Ingestion Pipeline

The data flow is:

1. **Vendor files** — CSV or Parquet files from RiskLink, Verisk, or other vendors
2. **etlval upload** — Validates schema, normalises column names to snake_case, stores in registry
3. **etlval push** — Copies validated files to hive-partitioned parquet store
4. **dbt raw models** — Read parquet files directly using DuckDB's `read_parquet()` with hive partitioning
5. **dbt staging models** — Generate surrogate keys, rename columns, prepare for downstream use

---

## 2. Raw Models

dbt has two raw models that read directly from the hive-partitioned parquet store:

### `raw.hive_storage__raw_ylts`

Reads all YLT (Year Loss Table) parquet files:

```sql
select *
from read_parquet(
    '{{ var("cat_results_path") }}/date=*/vendor=*/type=ylt/*.parquet',
    hive_partitioning = true,
    filename = true,
    union_by_name = true
)
```

**Columns:**
- `date` — run date (from hive partition)
- `vendor` — data source: `risklink`, `verisk`, etc. (from hive partition)
- `type` — always `ylt` (from hive partition)
- `filename` — source parquet filename
- `analysis_id` — analysis identifier (scoped per vendor)
- `year_id` — year of loss event
- `event_id` — event identifier
- `loss` — loss amount
- Any additional columns from the vendor file

### `raw.hive_storage__raw_analysis_lists`

Reads all analysis list parquet files:

```sql
select *
from read_parquet(
    '{{ var("cat_results_path") }}/date=*/vendor=*/type=analysis_list/*.parquet',
    hive_partitioning = true,
    filename = true,
    union_by_name = true
)
```

**Columns:**
- `date` — run date (from hive partition)
- `vendor` — data source (from hive partition)
- `type` — always `analysis_list` (from hive partition)
- `filename` — source parquet filename
- `analysis_id` — analysis identifier (scoped per vendor)
- `modelled_lob` — line of business
- `region_peril` — region and peril code
- `analysis_modifications` — modification notes

---

## 3. Staging Models

Staging models clean and enrich the raw data:

### `staging.stg_cat_modelling_results__ylts`

Transforms YLT data:

```sql
select
    {{ dbt_utils.generate_surrogate_key(['vendor', 'year_id', 'event_id', 'analysis_id']) }} as pk,
    filename as source_file,
    analysis_id,
    year_id,
    event_id,
    loss,
    vendor as source_vendor,
    date as run_date
from {{ ref('hive_storage__raw_ylts') }}
```

**Key transformations:**
- **Surrogate key** — `pk` is a hash of `(vendor, year_id, event_id, analysis_id)` for uniqueness
- **Column renames** — `vendor` → `source_vendor`, `date` → `run_date`, `filename` → `source_file`
- **Vendor scoping** — The surrogate key includes `vendor` because `analysis_id` is scoped per vendor

### `staging.stg_cat_modelling_results__analysis_lists`

Transforms analysis list data:

```sql
select
    {{ dbt_utils.generate_surrogate_key(['vendor', 'analysis_id']) }} as pk,
    filename as source_file,
    analysis_id,
    modelled_lob,
    region_peril,
    analysis_modifications,
    vendor as source_vendor,
    date as run_date
from {{ ref('hive_storage__raw_analysis_lists') }}
```

**Key transformations:**
- **Surrogate key** — `pk` is a hash of `(vendor, analysis_id)` for uniqueness
- **Column renames** — Same as YLT model
- **Vendor scoping** — The surrogate key includes `vendor` to ensure uniqueness across vendors

---

## 4. Schema Contract

The schema contract between etlval and dbt is defined in etlval's configuration.
etlval validates that uploaded files contain the required columns and normalises
column names to snake_case before writing.

**YLT required columns:**
- `analysis_id` — BIGINT, INTEGER, VARCHAR, or STRING
- `year_id` — BIGINT or INTEGER (normalised from `yearid`)
- `event_id` — BIGINT or INTEGER (normalised from `eventid`)
- `loss` — DOUBLE, FLOAT, or REAL

**Analysis list required columns:**
- `analysis_id` — BIGINT, INTEGER, VARCHAR, or STRING
- `modelled_lob` — VARCHAR or STRING
- `region_peril` — VARCHAR or STRING
- `analysis_modifications` — VARCHAR or STRING

For the full schema contract and column normalisation rules, see [etlval's Downstream Integration guide](../etlval/docs/downstream-integration.md).

---

## 5. Configuration

### `cat_results_path` Variable

The `cat_results_path` dbt variable points to the hive-partitioned parquet store. It is defined in `dbt_project.yml`:

```yaml
vars:
  cat_results_path: >-
    {%- if target.name == 'prod' -%}
      {{ env_var('CAT_RESULTS_PATH') }}
    {%- else -%}
      {{ env_var('CAT_RESULTS_PATH', '../rollup-data/cat_results') }}
    {%- endif -%}
```

**Development:**
- If `CAT_RESULTS_PATH` environment variable is not set, defaults to `../rollup-data/cat_results`
- Allows local testing without environment variables

**Production:**
- Requires `CAT_RESULTS_PATH` environment variable to be set
- Fails if the variable is not provided

**Setting the variable:**

```bash
# Development (optional)
export CAT_RESULTS_PATH=/data/cat_results
dbt run

# Production (required)
export CAT_RESULTS_PATH=/data/cat_results
dbt run --target prod
```

---

## 6. Important Notes

### Vendor-Scoped Analysis ID

`analysis_id` is not globally unique — it is scoped per vendor. Both RiskLink and Verisk can have `analysis_id = 1` referring to different analyses.

**Always join using `(vendor, analysis_id)` together:**

```sql
-- CORRECT
select y.*, a.modelled_lob
from stg_cat_modelling_results__ylts y
join stg_cat_modelling_results__analysis_lists a
  on y.vendor = a.vendor and y.analysis_id = a.analysis_id
```

### Hive Partitioning

Raw models use DuckDB's `hive_partitioning=true` option, which automatically extracts partition keys from the directory structure:

```
/data/cat_results/date=2026-03-01/vendor=risklink/type=ylt/ylt_65e190.parquet
                   ↑                  ↑              ↑
                   date               vendor         type
```

These become columns in the result set, enabling efficient filtering.

### Column Normalisation

etlval normalises vendor column names to snake_case before writing. Examples:

| Vendor Input | Parquet Column |
|--------------|----------------|
| `yearid` | `year_id` |
| `eventid` | `event_id` |
| `YearID` | `year_id` |
| `modelId` | `model_id` |

dbt always receives clean snake_case column names.

---

## 7. See Also

- [etlval Downstream Integration](../etlval/docs/downstream-integration.md) — Full schema contract, column normalisation, and end-to-end workflows
- [etlval Architecture](../etlval/docs/architecture.md) — How etlval validates and pushes data
- [etlval Push Feature](../etlval/docs/push-feature.md) — Push command reference
