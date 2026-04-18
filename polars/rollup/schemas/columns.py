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

class DimRegionPerilCol(StrEnum):
    ID                                  = "id"
    VENDOR                              = "vendor"
    MODELLED_REGION_PERIL               = "modelled_region_peril"
    CLEANED_REGION_PERIL                = "cleaned_region_peril"
    ROLLUP_REGION_PERIL                 = "rollup_region_peril"
    REGION                              = "region"
    PERIL                               = "peril"
    ADJUSTMENTS                         = "adjustments"
    EXCLUDES                            = "excludes"
    APPLIES_TO_MGA                      = "applies_to_mga"
    APPLIES_TO_PROP                     = "applies_to_prop"
    APPLIES_TO_FA                       = "applies_to_fa"
    BLENDING_FACTOR_REGION_PERIL_ID     = "blending_factor_region_peril_id"
    BLENDING_FACTOR_SUB_REGION_PERIL_ID = "blending_factor_sub_region_peril_id"


class DimRisklinkAnalysisCol(StrEnum):
    """Maps RiskLink analysis id → (lob, region_peril) strings for staging join."""
    RISKLINK_ANALYSIS_ID = "risklink_analysis_id"
    LOB                  = "lob"
    REGION_PERIL         = "region_peril"


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


class RefBlendingFactorsCol(StrEnum):
    """Wide across vendors — `air_blend` and `rms_blend` are used together in staging."""
    ID                  = "id"
    BLEND_SET_ID        = "blend_set_id"
    REGION_PERIL_ID     = "region_peril_id"
    REGION_PERIL        = "region_peril"
    SUB_REGION_PERIL_ID = "sub_region_peril_id"
    SUB_REGION_PERIL    = "sub_region_peril"
    AIR_BLEND           = "air_blend"
    RMS_BLEND           = "rms_blend"
    KAT_RISK_BLEND      = "kat_risk_blend"


class RefForecastFactorsCol(StrEnum):
    """Long format — one row per (class, office, forecast_date).

    january had three wide columns `f_202601`, `f_202607`, `f_202701`; long
    format makes adding future forecast dates a data-only change.
    """
    CLASS         = "class"
    OFFICE        = "office"
    OFFICE_ISO2   = "office_iso2"
    BASE_DATE     = "base_date"
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


class RefAirEventsCol(StrEnum):
    EVENT_ID = "event_id"
    MODEL_ID = "model_id"
    EVENT    = "event"
    YEAR     = "year"
    DAY      = "day"


class RefCdsRegionPerilCol(StrEnum):
    ID                   = "id"
    CDS_REGION_PERIL     = "cds_region_peril"
    CDS_SUB_REGION_PERIL = "cds_sub_region_peril"
    CDS_MODEL_TO_USE     = "cds_model_to_use"


class RefFineartAdjCol(StrEnum):
    LOB_ID              = "lob_id"
    REGION_PERIL_ID     = "region_peril_id"
    APPLIES_TO_FA       = "applies_to_fa"
    ROLLUP_REGION_PERIL = "rollup_region_peril"
    AAL_FACTOR          = "aal_factor"
    TAIL_FACTOR         = "tail_factor"


# ----- OPTIMAL seed structure (new, replacing dim_region_perils + dim_risklink_analysis + blending_factors) -----

class PerilsCol(StrEnum):
    """One row per rollup peril. Integer id, string labels for display.

    Replaces the label-rollup columns of january's `dim_region_perils`.
    `peril_id` values preserve january's `RegionPerilID` integers so that
    any external reference still resolves.
    """
    PERIL_ID     = "peril_id"
    NAME         = "name"
    REGION       = "region"        # "US", "EU", "AU", "AP", ...
    PERIL_FAMILY = "peril_family"  # "EQ", "TC", "FL", "WS", "CS", "WF"


class AnalysesCol(StrEnum):
    """Vendor analysis label → peril_id mapping. Composite key (vendor, analysis_id).

    Replaces january's `dim_rl_analysis` + vendor rows of `dim_region_perils`.
    `lob_id` is populated for RiskLink (analysis is 1:1 with a (lob, peril))
    and NULL for Verisk (analysis is peril-only; lob lives on the YLT row).
    """
    VENDOR         = "vendor"          # "verisk" | "risklink"
    ANALYSIS_ID    = "analysis_id"     # string — either the Verisk label or stringified anlsid
    MODELLED_LABEL = "modelled_label"  # display label (often same as analysis_id)
    PERIL_ID       = "peril_id"        # FK into perils.csv
    LOB_ID         = "lob_id"          # FK into lobs.csv; nullable for Verisk


class BlendingWeightsCol(StrEnum):
    """Per (peril_id, sub_peril, vendor) blend weight — long format.

    Replaces the wide AIRBlend/RMSBlend/KatRiskBlend columns of january's
    `blending_factors`. `sub_peril` is nullable — most perils don't need
    regional sub-splits.
    """
    PERIL_ID  = "peril_id"
    SUB_PERIL = "sub_peril"
    VENDOR    = "vendor"
    WEIGHT    = "weight"


class RollupScopeCol(StrEnum):
    """Which (lob_id, vendor, analysis_id) triples are in the official rollup.

    The grain is `analysis_id` — NOT `peril_id` — because two analyses can
    share a peril_id (e.g. `UK_WSSS` and `UK_WSSS_GCAdj` are both peril 206
    but only ONE is official per LOB). In january this was implicit in the
    `applies_to_{mga,prop,fa}` flags on each `dim_region_perils` row; the
    non-selected variant simply had `applies_to_*=0` for all LOB types.

    Replaces january's:
        CASE lob_type WHEN 'mga'  THEN applies_to_mga
                      WHEN 'prop' THEN applies_to_prop
                      WHEN 'fa'   THEN applies_to_fa
                      ELSE 0 END AS official_rollup
    """
    LOB_ID      = "lob_id"
    VENDOR      = "vendor"        # "verisk" | "risklink"
    ANALYSIS_ID = "analysis_id"   # the modelled_label / wire label per vendor
    IN_ROLLUP   = "in_rollup"


# ----- legacy / to-retire once OPTIMAL seeds have full data -----

class RefFloodRl22Col(StrEnum):
    MODEL_EVENT_PK        = "model_event_pk"
    MODEL_PROVIDER_ID     = "model_provider_id"
    MODEL_EVENT_ID        = "model_event_id"
    MODEL_OCCURRENCE_YEAR = "model_occurrence_year"
    MODEL_OCCURRENCE_DATE = "model_occurrence_date"
    REGION_PERIL_ID       = "region_peril_id"


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
    """Canonical YLT — both vendors land here with identical shape."""
    VENDOR                = "vendor"
    LOB_ID                = "lob_id"
    MODELLED_LOB          = "modelled_lob"
    ROLLUP_LOB            = "rollup_lob"
    LOB_TYPE              = "lob_type"
    CDS_CAT_CLASS_NAME    = "cds_cat_class_name"
    REGION_PERIL_ID       = "region_peril_id"
    MODELLED_REGION_PERIL = "modelled_region_peril"
    ROLLUP_REGION_PERIL   = "rollup_region_peril"
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
    ROLLUP_REGION_PERIL = "rollup_region_peril"
    CDS_CAT_CLASS_NAME  = "cds_cat_class_name"
    EP_TYPE             = "ep_type"
    RANK_NUM            = "rank_num"
    RETURN_PERIOD       = "return_period"
    ANNUAL_LOSS         = "annual_loss"


class AllFactorsCol(StrEnum):
    """Wide per-event frame: dims + raw loss + blending + per-stage factors.

    Equivalent to duckdb `mts_tbl_ylt_combined_all_factors` MINUS the 13
    derived loss metrics — those live in `MetricCol` (joined via the same
    row id). This is the node we `.cache()` before the Hisco fan-out.
    """
    # dims
    VENDOR              = "vendor"
    LOB_ID              = "lob_id"
    ROLLUP_LOB          = "rollup_lob"
    CDS_CAT_CLASS_NAME  = "cds_cat_class_name"
    REGION_PERIL_ID     = "region_peril_id"
    ROLLUP_REGION_PERIL = "rollup_region_peril"
    BASE_MODEL          = "base_model"
    MODEL_CODE          = "model_code"
    MODEL_EVENT_ID      = "model_event_id"
    YEAR_ID             = "year_id"
    EVENT_ID            = "event_id"
    REQUIRED_CURRENCY   = "required_currency"
    RATE_TO_GBP         = "rate_to_gbp"
    # raw + blending
    LOSS                 = "loss"
    RL_PROPORTION        = "rl_proportion"
    VK_PROPORTION        = "vk_proportion"
    UPLIFT_FACTOR        = "uplift_factor_on_base_model"
    UPLIFT_FACTOR_CAPPED = "uplift_factor_on_base_model_capped"
    # per-stage factor scalars
    F_202601             = "f_202601"
    F_202607             = "f_202607"
    F_202701             = "f_202701"
    EUWS_FACTOR          = "euws_factor"
    FA_GROSS_AAL_FACTOR  = "fa_gross_aal_factor"
    FA_GROSS_TAIL_FACTOR = "fa_gross_tail_factor"


class MetricCol(StrEnum):
    """13 derived loss metrics — one of these feeds Hisco.ModelGrossLoss.

    Name = chain of factors applied: uplifted → capped → localccy → year → euws → fagross.
    """
    LOSS_UPLIFTED                                  = "loss_uplifted"
    LOSS_UPLIFTED_CAPPED                           = "loss_uplifted_capped"
    LOSS_UPLIFTED_CAPPED_LOCALCCY                  = "loss_uplifted_capped_localccy"
    LOSS_UPLIFTED_CAPPED_LOCALCCY_202601           = "loss_uplifted_capped_localccy_202601"
    LOSS_UPLIFTED_CAPPED_LOCALCCY_202607           = "loss_uplifted_capped_localccy_202607"
    LOSS_UPLIFTED_CAPPED_LOCALCCY_202701           = "loss_uplifted_capped_localccy_202701"
    LOSS_UPLIFTED_CAPPED_LOCALCCY_202601_EUWS      = "loss_uplifted_capped_localccy_202601_euws"
    LOSS_UPLIFTED_CAPPED_LOCALCCY_202607_EUWS      = "loss_uplifted_capped_localccy_202607_euws"
    LOSS_UPLIFTED_CAPPED_LOCALCCY_202701_EUWS      = "loss_uplifted_capped_localccy_202701_euws"
    LOSS_UPLIFTED_CAPPED_LOCALCCY_202601_EUWS_FAGROSS = "loss_uplifted_capped_localccy_202601_euws_fagross"
    LOSS_UPLIFTED_CAPPED_LOCALCCY_202607_EUWS_FAGROSS = "loss_uplifted_capped_localccy_202607_euws_fagross"
    LOSS_UPLIFTED_CAPPED_LOCALCCY_202701_EUWS_FAGROSS = "loss_uplifted_capped_localccy_202701_euws_fagross"


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
