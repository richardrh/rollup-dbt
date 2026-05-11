"""Column-name enums. One StrEnum per logical frame.

StrEnum members are strings, so `pl.col(C.FOO)` and `pl.Schema({C.FOO: dt})`
both work. Use `pl.col(C.FOO)` everywhere — attribute shorthand (`pl.col.foo`)
only works for valid Python identifiers and some vendor columns have spaces.
"""

from enum import StrEnum


# ----- raw vendor YLTs (wire column names preserved) -----

class RawRisklinkYltCol(StrEnum):
    """Raw RiskLink (RMS) YLT parquet — wire column names preserved."""
    SIMULATION_SET_ID = "SimulationSetId"
    YEAR_ID           = "yearid"
    EVENT_ID          = "eventid"
    DATE              = "date"
    P_VALUE           = "p_value"
    ANLS_ID           = "anlsid"
    NAME              = "name"
    DESCRIPTION       = "description"
    RATE              = "rate"
    MEAN_LOSS         = "meanloss"
    STD_DEV           = "stddev"
    EXP_VALUE         = "expvalue"
    LOSS              = "loss"


class RawVeriskYltCol(StrEnum):
    """Raw Verisk (AIR) YLT parquet — wire column names preserved.

    Matches the parquets that ship out of AIR touchstone (CamelCase). The
    'lob' concept on this file is in `ExposureAttribute` — staging renames
    it to `lob` as part of normalization.
    """
    ANALYSIS           = "Analysis"
    EXPOSURE_ATTRIBUTE = "ExposureAttribute"
    CATALOG_TYPE_CODE  = "CatalogTypeCode"
    EVENT_ID           = "EventID"
    MODEL_CODE         = "ModelCode"
    YEAR_ID            = "YearID"
    PERILSET_CODE      = "PerilSetCode"
    GROUND_UP_LOSS     = "GroundUpLoss"
    GROSS_LOSS         = "GrossLoss"
    NET_PRE_CAT_LOSS   = "NetOfPreCatLoss"
    FILENAME           = "filename"


# ----- dimension / reference tables -----

class PerilsCol(StrEnum):
    """One row per rollup peril. Integer id is the canonical key shared across
    vendors. Replaces the per-vendor duplication of `dim_region_perils`.

    `peril_family` ("EQ", "TC", "FL", "WS", "CS", "WF") is the semantic
    category — the pipeline uses this for the flood-base-model rule, NOT the
    derived `region + family` string. If a new flood region is added, no
    code change is needed.
    """
    PERIL_ID     = "peril_id"
    NAME         = "name"           # display: "Europe Winter Storm"
    REGION       = "region"         # "US", "EU", "AU", "AP", ...
    PERIL_FAMILY = "peril_family"   # "EQ", "TC", "FL", "WS", "CS", "WF"


class AnalysesCol(StrEnum):
    """Vendor analysis label → peril_id (and lob_id for RiskLink) mapping.

    Composite key (vendor, analysis_id). Replaces the union of
    `dim_rl_analysis` + `dim_region_perils.modelled_region_peril` rows.

    `lob_id` is populated for RiskLink (one analysis maps to one (lob, peril))
    and NULL for Verisk (analysis is peril-only; lob lives on the YLT row's
    `ExposureAttribute`).
    """
    VENDOR         = "vendor"           # "verisk" | "risklink"
    ANALYSIS_ID    = "analysis_id"      # str — Verisk label, or stringified rl_analysis_id
    MODELLED_LABEL = "modelled_label"   # display label (often same as analysis_id)
    PERIL_ID       = "peril_id"         # FK into perils.csv
    LOB_ID         = "lob_id"           # FK into lobs.csv; nullable for Verisk


class BlendingWeightsCol(StrEnum):
    """Per (peril_id, return_period, vendor) blend weight — long format.

    `return_period` is the EP return period used to derive the weight.
    Common values: 0 (AAL), 200 (1-in-200 OEP), 1000 (1-in-1000 OEP).

    `sub_peril` is nullable — most perils don't need regional sub-splits.

    `peril_name` + `description` are denormalised display columns: the
    pipeline NEVER joins on them — the join is on (peril_id, return_period)
    only — but the CSV stays human-readable.
    """
    PERIL_ID      = "peril_id"
    RETURN_PERIOD = "return_period"   # 0=AAL, 200, 1000, ...
    PERIL_NAME    = "peril_name"
    DESCRIPTION   = "description"
    SUB_PERIL     = "sub_peril"
    VENDOR        = "vendor"
    BASE_MODEL    = "base_model"      # "verisk" | "risklink"
    WEIGHT        = "weight"


class RollupScopeCol(StrEnum):
    """Which (modelled_lob, vendor, analysis_id) triples are in the official rollup.

    The grain is `analysis_id` — NOT `peril_id` — because two analyses can
    share a peril_id (e.g. `UK_WSSS` and `UK_WSSS_GCAdj` are both peril 206
    but only ONE is official per LOB). Replaces the
    `applies_to_{mga,prop,fa}` flag fan-out of `dim_region_perils`.

    `modelled_lob` is the natural key from `lobs.csv` — readable without a
    join, unlike the opaque integer `lob_id`.
    """
    MODELLED_LOB = "modelled_lob"
    VENDOR       = "vendor"        # "verisk" | "risklink"
    ANALYSIS_ID  = "analysis_id"   # the modelled_label / wire label per vendor
    IN_ROLLUP    = "in_rollup"


class RefLobsCol(StrEnum):
    """january `reference.lobs` + the derived (office, class) from `lobs_with_class_office`.

    Keeping office/class on the seed itself skips the runtime `split(rollup_lob,'_')`
    view; the seed owner can keep them in sync with rollup_lob directly.
    """
    LOB_ID             = "lob_id"
    MODELLED_LOB       = "modelled_lob"
    ROLLUP_LOB         = "rollup_lob"
    LOB_TYPE           = "lob_type"
    CDS_CAT_CLASS_NAME = "cds_cat_class_name"
    OFFICE             = "office"
    CLASS              = "class"


class RefForecastFactorsCol(StrEnum):
    """Long format — one row per (class, office, forecast_date).

    january had three wide columns `f_202601`, `f_202607`, `f_202701`; long
    format makes adding future forecast dates a data-only change.
    """
    CLASS         = "class"
    OFFICE        = "office"
    OFFICE_ISO2   = "office_iso2"
    FORECAST_DATE = "forecast_date"
    FACTOR        = "factor"


class RefFxRatesCol(StrEnum):
    """Long format — one row per (currency_code, target_currency, rate_date).

    january had `Rate to USD` + `Rate to GBP` columns; long format scales to any
    currency pair without a schema change.
    """
    CURRENCY_CODE   = "currency_code"
    TARGET_CURRENCY = "target_currency"
    RATE_DATE       = "rate_date"
    RATE            = "rate"


class RefEuwsRateFactorsCol(StrEnum):
    MODEL_EVENT_ID = "model_event_id"
    OCC_YEAR       = "occ_year"
    FACTOR         = "factor"


class RefEuwsRankOverridesCol(StrEnum):
    """Per-LOB rank threshold overrides for the EUWS factor.

    When rollup_lob matches AND rnk <= max_rank, euws_factor is replaced with
    `factor` instead of the joined euws_rate_factors value. Absence = no override.
    Add a row here to override a new LOB without any code change.
    """
    ROLLUP_LOB = "rollup_lob"
    MAX_RANK   = "max_rank"
    FACTOR     = "factor"


class RefAirEventsCol(StrEnum):
    EVENT_ID = "event_id"
    MODEL_ID = "model_id"
    EVENT    = "event"
    YEAR     = "year"
    DAY      = "day"


class RefRisklinkEventsCol(StrEnum):
    EVENT_ID = "event_id"
    YEAR     = "year"
    DAY      = "day"


class RefFineartAdjCol(StrEnum):
    LOB_ID              = "lob_id"
    REGION_PERIL_ID     = "region_peril_id"
    APPLIES_TO_FA       = "applies_to_fa"
    ROLLUP_REGION_PERIL = "rollup_region_peril"
    AAL_FACTOR          = "aal_factor"
    TAIL_FACTOR         = "tail_factor"


# ----- raw EP summaries (one row per RP × ep_type × lob × region_peril) -----

class StgRisklinkEpCol(StrEnum):
    """RiskLink EP summary rows — matches january `stg_rl_ep`."""
    ID           = "id"
    RP           = "rp"
    EP_TYPE      = "ep_type"
    LOB          = "lob"
    REGION_PERIL = "region_peril"
    GL           = "gl"


class StgVeriskEpCol(StrEnum):
    """Verisk EP summary rows — matches january `stg_vk_ep`."""
    RP       = "rp"
    EP_TYPE  = "ep_type"
    ANALYSIS = "analysis"
    LOB      = "lob"
    GL       = "gl"


# ----- internal canonical frames -----

class NormalizedYltCol(StrEnum):
    """Canonical YLT — both vendors land here with identical shape.

    `office` and `lob_class` ride along from the lobs join in staging.
    `peril_name` / `region` / `peril_family` come from the perils join so
    downstream factor stages have semantic dims (the flood-base-model
    rule keys on `peril_family == "FL"`, not a derived label string).
    """
    VENDOR                = "vendor"
    LOB_ID                = "lob_id"
    MODELLED_LOB          = "modelled_lob"
    ROLLUP_LOB            = "rollup_lob"
    LOB_TYPE              = "lob_type"
    CDS_CAT_CLASS_NAME    = "cds_cat_class_name"
    OFFICE                = "office"
    LOB_CLASS             = "lob_class"
    REGION_PERIL_ID       = "region_peril_id"   # = perils.peril_id (canonical)
    MODELLED_REGION_PERIL = "modelled_region_peril"  # = analyses.modelled_label
    PERIL_NAME            = "peril_name"        # = perils.name (display)
    REGION                = "region"            # = perils.region
    PERIL_FAMILY          = "peril_family"      # = perils.peril_family
    MODEL_CODE            = "model_code"
    YEAR_ID               = "year_id"
    EVENT_ID              = "event_id"
    LOSS                  = "loss"


class EpCurveCol(StrEnum):
    """Output of `ep_curve_from_ylt`. AAL rows use rank_num=0, return_period=0."""
    VENDOR              = "vendor"
    LOB_ID              = "lob_id"
    REGION_PERIL_ID     = "region_peril_id"
    ROLLUP_LOB          = "rollup_lob"
    PERIL_NAME          = "peril_name"
    REGION              = "region"
    PERIL_FAMILY        = "peril_family"
    CDS_CAT_CLASS_NAME  = "cds_cat_class_name"
    EP_TYPE             = "ep_type"
    RANK_NUM            = "rank_num"
    RETURN_PERIOD       = "return_period"
    ANNUAL_LOSS         = "annual_loss"


class EpType(StrEnum):
    """Closed set of values that appear in `EpCurveCol.EP_TYPE`.

    These strings are also used as intermediate column names inside
    `ep_curve_from_ylt` (the per-year aggregates are aliased to AEP/OEP
    before being unpivoted into EP_TYPE rows).
    """
    AAL = "AAL"
    AEP = "AEP"
    OEP = "OEP"


class AllFactorsCol(StrEnum):
    """Wide per-event frame: dims + raw loss + blending + per-stage factors.

    The forecast factors (`f_{yyyymm}`) and their downstream year-tagged
    metrics are NOT enumerated here — they are data-driven. Tags come from
    `seeds.forecast_factors.forecast_date` at pipeline runtime. Adding a
    date to the seed = new `f_{tag}` column + new metric columns
    automatically; no code change.

    `FA_GROSS_TAIL_FACTOR` is carried for audit transparency only — it is
    NOT multiplied into any metric in the current chain. The fine-art
    AAL/tail split exists for future tail-loss adjustments; today only
    `FA_GROSS_AAL_FACTOR` is applied. If you start using tail, multiply it
    in `_compute_metrics` and add a new column suffix (`_fagrosstail`).
    """
    # dims
    VENDOR                = "vendor"
    LOB_ID                = "lob_id"
    MODELLED_LOB          = "modelled_lob"
    ROLLUP_LOB            = "rollup_lob"
    LOB_TYPE              = "lob_type"
    OFFICE                = "office"
    LOB_CLASS             = "lob_class"
    CDS_CAT_CLASS_NAME    = "cds_cat_class_name"
    REGION_PERIL_ID       = "region_peril_id"
    MODELLED_REGION_PERIL = "modelled_region_peril"
    PERIL_NAME            = "peril_name"
    REGION                = "region"
    PERIL_FAMILY          = "peril_family"
    BASE_MODEL            = "base_model"
    MODEL_CODE            = "model_code"
    MODEL_EVENT_ID        = "model_event_id"
    YEAR_ID               = "year_id"
    EVENT_ID              = "event_id"
    REQUIRED_CURRENCY     = "required_currency"
    RATE_TO_GBP           = "rate_to_gbp"
    # raw + blending
    LOSS                 = "loss"
    RL_PROPORTION        = "rl_proportion"
    VK_PROPORTION        = "vk_proportion"
    UPLIFT_FACTOR        = "uplift_factor_on_base_model"
    UPLIFT_FACTOR_CAPPED = "uplift_factor_on_base_model_capped"
    # year-invariant factor scalars
    RNK                  = "rnk"
    RP                   = "rp"
    RP_BUCKET            = "rp_bucket"
    EUWS_FACTOR          = "euws_factor"
    FA_GROSS_AAL_FACTOR  = "fa_gross_aal_factor"
    FA_GROSS_TAIL_FACTOR = "fa_gross_tail_factor"


class MetricCol(StrEnum):
    """Year-invariant derived loss metrics. Year-tagged metric column names
    are data-driven and built by the chain registry in `rollup/chain.py` —
    use `chain.col_after(stage, tag)` / `chain.main_loss_col(tag)` /
    `chain.dialsup_col(tag)` / `chain.forecast_factor_col(tag)` to look them
    up. Never hand-build the `loss_uplifted_capped_localccy_..._fagross`
    f-string — the registry IS the source of truth.
    """
    LOSS_UPLIFTED                 = "loss_uplifted"
    LOSS_UPLIFTED_CAPPED          = "loss_uplifted_capped"
    LOSS_UPLIFTED_CAPPED_LOCALCCY = "loss_uplifted_capped_localccy"


class HiscoFanoutCol(StrEnum):
    """Hisco output — matches duckdb marts.Hisco* tables exactly."""
    MODEL_EVENT_ID              = "ModelEventID"
    MODEL_YEAR                  = "ModelYear"
    CURRENCY_CODE               = "CurrencyCode"
    MODEL_YOA                   = "ModelYOA"
    MODEL_GROSS_LOSS            = "ModelGrossLoss"
    MODEL_INWARDS_REINSTATEMENT = "ModelInwardsReinstatement"
    MODEL_EVENT_DAY             = "ModelEventDay"
    LOSS_CLASS_NAME             = "LossClassName"
