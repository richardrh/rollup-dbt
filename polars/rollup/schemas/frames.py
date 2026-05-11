"""Frame schemas — one `pl.Schema` per logical frame, keyed by column enums."""

from __future__ import annotations

import polars as pl

from .columns import (
    AllFactorsCol,
    AnalysesCol,
    BlendingWeightsCol,
    EpCurveCol,
    HiscoFanoutCol,
    MetricCol,
    NormalizedYltCol,
    PerilsCol,
    RawRisklinkYltCol,
    RawVeriskYltCol,
    RefAirEventsCol,
    RefEuwsRankOverridesCol,
    RefEuwsRateFactorsCol,
    RefFineartAdjCol,
    RefForecastFactorsCol,
    RefFxRatesCol,
    RefLobsCol,
    RefRisklinkEventsCol,
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


# ----- dimension / reference (the OPTIMAL split — one table, one job) -----

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
    AnalysesCol.LOB_ID:         pl.Int64,   # nullable for Verisk
})

BLENDING_WEIGHTS: pl.Schema = pl.Schema({
    BlendingWeightsCol.PERIL_ID:       pl.Int64,
    BlendingWeightsCol.RETURN_PERIOD:  pl.Int64,
    BlendingWeightsCol.PERIL_NAME:     pl.String,
    BlendingWeightsCol.DESCRIPTION:    pl.String,
    BlendingWeightsCol.SUB_PERIL:      pl.String,
    BlendingWeightsCol.VENDOR:         pl.String,
    BlendingWeightsCol.BASE_MODEL:     pl.String,
    BlendingWeightsCol.WEIGHT:         pl.Float64,
})

ROLLUP_SCOPE: pl.Schema = pl.Schema({
    RollupScopeCol.MODELLED_LOB: pl.String,
    RollupScopeCol.VENDOR:       pl.String,
    RollupScopeCol.ANALYSIS_ID:  pl.String,
    RollupScopeCol.IN_ROLLUP:    pl.Boolean,
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

REF_FORECAST_FACTORS: pl.Schema = pl.Schema({
    RefForecastFactorsCol.CLASS:         pl.String,
    RefForecastFactorsCol.OFFICE:        pl.String,
    RefForecastFactorsCol.OFFICE_ISO2:   pl.String,
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

REF_EUWS_RANK_OVERRIDES: pl.Schema = pl.Schema({
    RefEuwsRankOverridesCol.ROLLUP_LOB: pl.String,
    RefEuwsRankOverridesCol.MAX_RANK:   pl.Int64,
    RefEuwsRankOverridesCol.FACTOR:     pl.Float64,
})

REF_AIR_EVENTS: pl.Schema = pl.Schema({
    RefAirEventsCol.EVENT_ID: pl.Int64,
    RefAirEventsCol.MODEL_ID: pl.Int64,
    RefAirEventsCol.EVENT:    pl.Int64,
    RefAirEventsCol.YEAR:     pl.Int64,
    RefAirEventsCol.DAY:      pl.Int64,
})

REF_RISKLINK_EVENTS: pl.Schema = pl.Schema({
    RefRisklinkEventsCol.EVENT_ID: pl.Int64,
    RefRisklinkEventsCol.YEAR:     pl.Int64,
    RefRisklinkEventsCol.DAY:      pl.Int64,
})

REF_FINEART_ADJ: pl.Schema = pl.Schema({
    RefFineartAdjCol.LOB_ID:              pl.Int64,
    RefFineartAdjCol.REGION_PERIL_ID:     pl.Int64,
    RefFineartAdjCol.APPLIES_TO_FA:       pl.Int64,
    RefFineartAdjCol.ROLLUP_REGION_PERIL: pl.String,
    RefFineartAdjCol.AAL_FACTOR:          pl.Float64,
    RefFineartAdjCol.TAIL_FACTOR:         pl.Float64,
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
    NormalizedYltCol.OFFICE:                pl.String,
    NormalizedYltCol.LOB_CLASS:             pl.String,
    NormalizedYltCol.REGION_PERIL_ID:       pl.Int64,
    NormalizedYltCol.MODELLED_REGION_PERIL: pl.String,
    NormalizedYltCol.PERIL_NAME:            pl.String,
    NormalizedYltCol.REGION:                pl.String,
    NormalizedYltCol.PERIL_FAMILY:          pl.String,
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
    EpCurveCol.PERIL_NAME:          pl.String,
    EpCurveCol.REGION:              pl.String,
    EpCurveCol.PERIL_FAMILY:        pl.String,
    EpCurveCol.CDS_CAT_CLASS_NAME:  pl.String,
    EpCurveCol.EP_TYPE:             pl.String,
    EpCurveCol.RANK_NUM:            pl.Int64,
    EpCurveCol.RETURN_PERIOD:       pl.Int64,
    EpCurveCol.ANNUAL_LOSS:         pl.Float64,
})

# ALL_FACTORS = dim + factor scalars. Derived metrics are joined in via MetricCol.
# Float64 throughout — losses run into hundreds of millions; Float32 is too thin.
ALL_FACTORS: pl.Schema = pl.Schema({
    AllFactorsCol.VENDOR:                pl.String,
    AllFactorsCol.LOB_ID:                pl.Int64,
    AllFactorsCol.MODELLED_LOB:          pl.String,
    AllFactorsCol.ROLLUP_LOB:            pl.String,
    AllFactorsCol.LOB_TYPE:              pl.String,
    AllFactorsCol.OFFICE:                pl.String,
    AllFactorsCol.LOB_CLASS:             pl.String,
    AllFactorsCol.CDS_CAT_CLASS_NAME:    pl.String,
    AllFactorsCol.REGION_PERIL_ID:       pl.Int64,
    AllFactorsCol.MODELLED_REGION_PERIL: pl.String,
    AllFactorsCol.PERIL_NAME:            pl.String,
    AllFactorsCol.REGION:                pl.String,
    AllFactorsCol.PERIL_FAMILY:          pl.String,
    AllFactorsCol.BASE_MODEL:            pl.String,
    AllFactorsCol.MODEL_CODE:            pl.Int64,
    AllFactorsCol.MODEL_EVENT_ID:        pl.Int64,
    AllFactorsCol.YEAR_ID:               pl.Int64,
    AllFactorsCol.EVENT_ID:              pl.Int64,
    AllFactorsCol.REQUIRED_CURRENCY:     pl.String,
    AllFactorsCol.RATE_TO_GBP:           pl.Float64,
    AllFactorsCol.LOSS:                  pl.Float64,
    AllFactorsCol.RL_PROPORTION:         pl.Float64,
    AllFactorsCol.VK_PROPORTION:         pl.Float64,
    AllFactorsCol.UPLIFT_FACTOR:         pl.Float64,
    AllFactorsCol.UPLIFT_FACTOR_CAPPED:  pl.Float64,
    # Year-tagged forecast factor columns (f_{yyyymm}) are data-driven from the
    # forecast_factors seed at runtime. validate_schema runs with strict=False
    # against this schema so the extras pass through.
    AllFactorsCol.RNK:                   pl.UInt32,
    AllFactorsCol.RP:                    pl.Float64,
    AllFactorsCol.RP_BUCKET:             pl.Int64,
    AllFactorsCol.EUWS_FACTOR:           pl.Float64,
    AllFactorsCol.FA_GROSS_AAL_FACTOR:   pl.Float64,
    AllFactorsCol.FA_GROSS_TAIL_FACTOR:  pl.Float64,
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
