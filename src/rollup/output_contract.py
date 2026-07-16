from __future__ import annotations

from rollup.columns import Col


MARTS_DIR = "marts"
ANALYSIS_DIR = "analysis"
DEBUG_DIR = "debug"

COMBINED_YLT_FILE = "mts_tbl_ylt_combined_all_factors.parquet"
WIDE_YLT_FILE = "mts_tbl_ylt_combined_all_factors_wide.parquet"
DIALSUP_YLT_FILE = "mts_tbl_ylt_dialsup.parquet"
EVENT_VALIDATION_FILE = "mts_event_validation.parquet"
EP_REPORT_FILE = "ep_report.csv"
DUCKDB_FILE = "rollup.duckdb"

WIDE_IDENTITY_DIMENSIONS = (
    Col.vendor,
    Col.analysis_id,
    Col.base_model,
    Col.model_code,
    Col.year_id,
    Col.event_id,
    Col.model_event_id,
    Col.event_day,
    Col.rollup_lob,
    Col.rollup_peril,
    Col.modelled_lob,
    Col.modelled_peril,
    Col.region_peril_id,
    Col.blend_subregion_peril_id,
    Col.cds_cat_class_name,
    Col.class_,
    Col.office,
    Col.currency,
    Col.target_currency,
    Col.rnk,
    Col.rp,
    Col.rp_bucket,
    Col.selection_priority,
    Col.is_dialsup,
    Col.is_euws,
)
WIDE_DIAGNOSTIC_COLUMNS = (
    Col.risklink_blended_contribution,
    Col.verisk_blended_contribution,
    Col.uplift_factor_on_base_model,
)
