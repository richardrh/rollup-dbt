"""Frame schemas — one `pl.Schema` per logical frame, keyed by column enums."""

from __future__ import annotations

import polars as pl

from .columns import (
    AllFactorsCol,
    AnalysesCol,
    BlendingWeightsCol,
    DimRegionPerilCol,
    DimRisklinkAnalysisCol,
    EpCurveCol,
    HiscoFanoutCol,
    MetricCol,
    NormalizedYltCol,
    PerilsCol,
    RawRisklinkYltCol,
    RawVeriskYltCol,
    RefAirEventsCol,
    RefBlendingFactorsCol,
    RefCdsRegionPerilCol,
    RefEuwsRateFactorsCol,
    RefFineartAdjCol,
    RefFloodRl22Col,
    RefForecastFactorsCol,
    RefFxRatesCol,
    RefLobsCol,
    RollupScopeCol,
    StgRisklinkEpCol,
    StgVeriskEpCol,
)


# ----- raw -----

RAW_RISKLINK_YLT: pl.Schema = pl.Schema({
    RawRisklinkYltCol.SIMULATION_SET_ID: pl.Int64,
    RawRisklinkYltCol.YEAR_ID:           pl.Int64,
    RawRisklinkYltCol.EVENT_ID:          pl.Int64,
    RawRisklinkYltCol.DATE:              pl.String,
    RawRisklinkYltCol.P_VALUE:           pl.Float64,
    RawRisklinkYltCol.ANLS_ID:           pl.Int64,
    RawRisklinkYltCol.NAME:              pl.String,
    RawRisklinkYltCol.DESCRIPTION:       pl.String,
    RawRisklinkYltCol.RATE:              pl.Float64,
    RawRisklinkYltCol.MEAN_LOSS:         pl.Float64,
    RawRisklinkYltCol.STD_DEV:           pl.Float64,
    RawRisklinkYltCol.EXP_VALUE:         pl.Float64,
    RawRisklinkYltCol.LOSS:              pl.Float64,
})

RAW_VERISK_YLT: pl.Schema = pl.Schema({
    RawVeriskYltCol.ANALYSIS:           pl.String,
    RawVeriskYltCol.EXPOSURE_ATTRIBUTE: pl.String,
    RawVeriskYltCol.CATALOG_TYPE_CODE:  pl.String,
    RawVeriskYltCol.EVENT_ID:           pl.Int64,
    RawVeriskYltCol.MODEL_CODE:         pl.Int64,
    RawVeriskYltCol.YEAR_ID:            pl.Int64,
    RawVeriskYltCol.PERILSET_CODE:      pl.Int64,
    RawVeriskYltCol.GROUND_UP_LOSS:     pl.Float64,
    RawVeriskYltCol.GROSS_LOSS:         pl.Float64,
    RawVeriskYltCol.NET_PRE_CAT_LOSS:   pl.Float64,
    RawVeriskYltCol.FILENAME:           pl.String,
})


# ----- dimension / reference -----

DIM_REGION_PERILS: pl.Schema = pl.Schema({
    DimRegionPerilCol.ID:                                  pl.Int64,
    DimRegionPerilCol.VENDOR:                              pl.String,
    DimRegionPerilCol.MODELLED_REGION_PERIL:               pl.String,
    DimRegionPerilCol.CLEANED_REGION_PERIL:                pl.String,
    DimRegionPerilCol.ROLLUP_REGION_PERIL:                 pl.String,
    DimRegionPerilCol.REGION:                              pl.String,
    DimRegionPerilCol.PERIL:                               pl.String,
    DimRegionPerilCol.ADJUSTMENTS:                         pl.String,
    DimRegionPerilCol.EXCLUDES:                            pl.String,
    DimRegionPerilCol.APPLIES_TO_MGA:                      pl.Int64,
    DimRegionPerilCol.APPLIES_TO_PROP:                     pl.Int64,
    DimRegionPerilCol.APPLIES_TO_FA:                       pl.Int64,
    DimRegionPerilCol.BLENDING_FACTOR_REGION_PERIL_ID:     pl.Int64,
    DimRegionPerilCol.BLENDING_FACTOR_SUB_REGION_PERIL_ID: pl.String,
})

DIM_RISKLINK_ANALYSIS: pl.Schema = pl.Schema({
    DimRisklinkAnalysisCol.RISKLINK_ANALYSIS_ID: pl.Int64,
    DimRisklinkAnalysisCol.LOB:                  pl.String,
    DimRisklinkAnalysisCol.REGION_PERIL:         pl.String,
})

REF_LOBS: pl.Schema = pl.Schema({
    RefLobsCol.LOB_ID:             pl.Int64,
    RefLobsCol.MODELLED_LOB:       pl.String,
    RefLobsCol.ROLLUP_LOB:         pl.String,
    RefLobsCol.LOB_TYPE:           pl.String,
    RefLobsCol.CDS_CAT_CLASS_NAME: pl.String,
    RefLobsCol.OFFICE:             pl.String,
    RefLobsCol.CLASS:              pl.String,
})

REF_BLENDING_FACTORS: pl.Schema = pl.Schema({
    RefBlendingFactorsCol.ID:                  pl.Int64,
    RefBlendingFactorsCol.BLEND_SET_ID:        pl.Int64,
    RefBlendingFactorsCol.REGION_PERIL_ID:     pl.Int64,
    RefBlendingFactorsCol.REGION_PERIL:        pl.String,
    RefBlendingFactorsCol.SUB_REGION_PERIL_ID: pl.String,
    RefBlendingFactorsCol.SUB_REGION_PERIL:    pl.String,
    RefBlendingFactorsCol.AIR_BLEND:           pl.Float64,
    RefBlendingFactorsCol.RMS_BLEND:           pl.Float64,
    RefBlendingFactorsCol.KAT_RISK_BLEND:      pl.Float64,
})

REF_FORECAST_FACTORS: pl.Schema = pl.Schema({
    RefForecastFactorsCol.CLASS:         pl.String,
    RefForecastFactorsCol.OFFICE:        pl.String,
    RefForecastFactorsCol.OFFICE_ISO2:   pl.String,
    RefForecastFactorsCol.BASE_DATE:     pl.Date,
    RefForecastFactorsCol.FORECAST_DATE: pl.Date,
    RefForecastFactorsCol.FACTOR:        pl.Float64,
})

REF_FX_RATES: pl.Schema = pl.Schema({
    RefFxRatesCol.CURRENCY_CODE:   pl.String,
    RefFxRatesCol.TARGET_CURRENCY: pl.String,
    RefFxRatesCol.RATE_DATE:       pl.Date,
    RefFxRatesCol.RATE:            pl.Float64,
})

REF_EUWS_RATE_FACTORS: pl.Schema = pl.Schema({
    RefEuwsRateFactorsCol.MODEL_EVENT_ID: pl.Int64,
    RefEuwsRateFactorsCol.OCC_YEAR:       pl.Int64,
    RefEuwsRateFactorsCol.FACTOR:         pl.Float64,
})

REF_AIR_EVENTS: pl.Schema = pl.Schema({
    RefAirEventsCol.EVENT_ID: pl.Int64,
    RefAirEventsCol.MODEL_ID: pl.Int64,
    RefAirEventsCol.EVENT:    pl.Int64,
    RefAirEventsCol.YEAR:     pl.Int64,
    RefAirEventsCol.DAY:      pl.Int64,
})

REF_CDS_REGION_PERIL: pl.Schema = pl.Schema({
    RefCdsRegionPerilCol.ID:                   pl.Int64,
    RefCdsRegionPerilCol.CDS_REGION_PERIL:     pl.String,
    RefCdsRegionPerilCol.CDS_SUB_REGION_PERIL: pl.String,
    RefCdsRegionPerilCol.CDS_MODEL_TO_USE:     pl.String,
})

REF_FINEART_ADJ: pl.Schema = pl.Schema({
    RefFineartAdjCol.LOB_ID:              pl.Int64,
    RefFineartAdjCol.REGION_PERIL_ID:     pl.Int64,
    RefFineartAdjCol.APPLIES_TO_FA:       pl.Int64,
    RefFineartAdjCol.ROLLUP_REGION_PERIL: pl.String,
    RefFineartAdjCol.AAL_FACTOR:          pl.Float64,
    RefFineartAdjCol.TAIL_FACTOR:         pl.Float64,
})

REF_FLOOD_RL22: pl.Schema = pl.Schema({
    RefFloodRl22Col.MODEL_EVENT_PK:        pl.Int64,
    RefFloodRl22Col.MODEL_PROVIDER_ID:     pl.Int64,
    RefFloodRl22Col.MODEL_EVENT_ID:        pl.Int64,
    RefFloodRl22Col.MODEL_OCCURRENCE_YEAR: pl.Int64,
    RefFloodRl22Col.MODEL_OCCURRENCE_DATE: pl.Datetime(time_unit="us"),
    RefFloodRl22Col.REGION_PERIL_ID:       pl.Int64,
})


# ----- OPTIMAL seed structure (replaces dim_region_perils + dim_risklink_analysis + blending_factors) -----

PERILS: pl.Schema = pl.Schema({
    PerilsCol.PERIL_ID:     pl.Int64,
    PerilsCol.NAME:         pl.String,
    PerilsCol.REGION:       pl.String,
    PerilsCol.PERIL_FAMILY: pl.String,
})

ANALYSES: pl.Schema = pl.Schema({
    AnalysesCol.VENDOR:         pl.String,
    AnalysesCol.ANALYSIS_ID:    pl.String,
    AnalysesCol.MODELLED_LABEL: pl.String,
    AnalysesCol.PERIL_ID:       pl.Int64,
    AnalysesCol.LOB_ID:         pl.Int64,
})

BLENDING_WEIGHTS: pl.Schema = pl.Schema({
    BlendingWeightsCol.PERIL_ID:  pl.Int64,
    BlendingWeightsCol.SUB_PERIL: pl.String,
    BlendingWeightsCol.VENDOR:    pl.String,
    BlendingWeightsCol.WEIGHT:    pl.Float64,
})

ROLLUP_SCOPE: pl.Schema = pl.Schema({
    RollupScopeCol.LOB_ID:      pl.Int64,
    RollupScopeCol.VENDOR:      pl.String,
    RollupScopeCol.ANALYSIS_ID: pl.String,
    RollupScopeCol.IN_ROLLUP:   pl.Boolean,
})


# ----- raw EP summaries -----

STG_RISKLINK_EP: pl.Schema = pl.Schema({
    StgRisklinkEpCol.ID:           pl.Int64,
    StgRisklinkEpCol.RP:           pl.Int64,
    StgRisklinkEpCol.EP_TYPE:      pl.String,
    StgRisklinkEpCol.LOB:          pl.String,
    StgRisklinkEpCol.REGION_PERIL: pl.String,
    StgRisklinkEpCol.GL:           pl.Float64,
})

STG_VERISK_EP: pl.Schema = pl.Schema({
    StgVeriskEpCol.RP:       pl.Int64,
    StgVeriskEpCol.EP_TYPE:  pl.String,
    StgVeriskEpCol.ANALYSIS: pl.String,
    StgVeriskEpCol.LOB:      pl.String,
    StgVeriskEpCol.GL:       pl.Float64,
})


# ----- internal canonical -----

NORMALIZED_YLT: pl.Schema = pl.Schema({
    NormalizedYltCol.VENDOR:                pl.String,
    NormalizedYltCol.LOB_ID:                pl.Int64,
    NormalizedYltCol.MODELLED_LOB:          pl.String,
    NormalizedYltCol.ROLLUP_LOB:            pl.String,
    NormalizedYltCol.LOB_TYPE:              pl.String,
    NormalizedYltCol.CDS_CAT_CLASS_NAME:    pl.String,
    NormalizedYltCol.REGION_PERIL_ID:       pl.Int64,
    NormalizedYltCol.MODELLED_REGION_PERIL: pl.String,
    NormalizedYltCol.ROLLUP_REGION_PERIL:   pl.String,
    NormalizedYltCol.MODEL_CODE:            pl.Int64,
    NormalizedYltCol.YEAR_ID:               pl.Int64,
    NormalizedYltCol.EVENT_ID:              pl.Int64,
    NormalizedYltCol.LOSS:                  pl.Float64,
})

EP_CURVE: pl.Schema = pl.Schema({
    EpCurveCol.VENDOR:              pl.String,
    EpCurveCol.LOB_ID:              pl.Int64,
    EpCurveCol.REGION_PERIL_ID:     pl.Int64,
    EpCurveCol.ROLLUP_LOB:          pl.String,
    EpCurveCol.ROLLUP_REGION_PERIL: pl.String,
    EpCurveCol.CDS_CAT_CLASS_NAME:  pl.String,
    EpCurveCol.EP_TYPE:             pl.String,
    EpCurveCol.RANK_NUM:            pl.Int64,
    EpCurveCol.RETURN_PERIOD:       pl.Int64,
    EpCurveCol.ANNUAL_LOSS:         pl.Float64,
})

# ALL_FACTORS = dim + factor scalars. Derived metrics are joined in via MetricCol.
ALL_FACTORS: pl.Schema = pl.Schema({
    AllFactorsCol.VENDOR:               pl.String,
    AllFactorsCol.LOB_ID:               pl.Int64,
    AllFactorsCol.ROLLUP_LOB:           pl.String,
    AllFactorsCol.CDS_CAT_CLASS_NAME:   pl.String,
    AllFactorsCol.REGION_PERIL_ID:      pl.Int64,
    AllFactorsCol.ROLLUP_REGION_PERIL:  pl.String,
    AllFactorsCol.BASE_MODEL:           pl.String,
    AllFactorsCol.MODEL_CODE:           pl.Int64,
    AllFactorsCol.MODEL_EVENT_ID:       pl.Int64,
    AllFactorsCol.YEAR_ID:              pl.Int64,
    AllFactorsCol.EVENT_ID:             pl.Int64,
    AllFactorsCol.REQUIRED_CURRENCY:    pl.String,
    AllFactorsCol.RATE_TO_GBP:          pl.Float64,
    AllFactorsCol.LOSS:                 pl.Float64,
    AllFactorsCol.RL_PROPORTION:        pl.Float32,
    AllFactorsCol.VK_PROPORTION:        pl.Float32,
    AllFactorsCol.UPLIFT_FACTOR:        pl.Float32,
    AllFactorsCol.UPLIFT_FACTOR_CAPPED: pl.Float32,
    # FIX: We dont' want this cols here this was the wide format from january
    # We should have date variable and add validated dates which derive
    # from the forecast factors seed table
    AllFactorsCol.F_202601:             pl.Float64,
    AllFactorsCol.F_202607:             pl.Float64,
    AllFactorsCol.F_202701:             pl.Float64,
    AllFactorsCol.EUWS_FACTOR:          pl.Float64,
    AllFactorsCol.FA_GROSS_AAL_FACTOR:  pl.Float64,
    AllFactorsCol.FA_GROSS_TAIL_FACTOR: pl.Float64,
})

METRICS: pl.Schema = pl.Schema({m: pl.Float64 for m in MetricCol})

HISCO_FANOUT: pl.Schema = pl.Schema({
    HiscoFanoutCol.MODEL_EVENT_ID:              pl.Int64,
    HiscoFanoutCol.MODEL_YEAR:                  pl.Int64,
    HiscoFanoutCol.CURRENCY_CODE:               pl.String,
    HiscoFanoutCol.MODEL_YOA:                   pl.Int32,
    HiscoFanoutCol.MODEL_GROSS_LOSS:            pl.Float64,
    HiscoFanoutCol.MODEL_INWARDS_REINSTATEMENT: pl.Int32,
    HiscoFanoutCol.MODEL_EVENT_DAY:             pl.Int64,
    HiscoFanoutCol.LOSS_CLASS_NAME:             pl.String,
})
