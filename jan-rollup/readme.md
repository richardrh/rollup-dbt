# what is in this folder

duckdb_schema: a copy of the duckdb "data pipeline" database that was used for january's analysis
ep_summaries: excel spreadsheets which contain ep sumamries used to match "rislink/rms to verisk"
lib-scripts: some scripts that were initially used to load data into the duckdb database - this
may provide useful to understand how the initial data load happens and also to see hpw the
ep summaries are used

air_ep_vw.sql : sql query which acts as a view over the air_ylt
air_ylt_c1.parquet: air_ylt chunk 1
air_ylt_c2.parquet: air_ylt chunk 2

# How the schema works - from what i remember

the data is loaded into main initially.
reference tables are loaded
joins and initial model blend happen in int\_ layer
to prevent the query speed from slowing down i THINK that
mts_tbl_ylt_combined_all_factors is a copy of int_tbl_ylt_combined_all_factors

This may nolt be true but i think it is what we did, the script that generated it
is not included in this repo i think

# end tables

marts ends up producing some int\_ tables like :
mts_vw_ylt_combined_all_factors_long**aggd_for_cds**fanout_rl_withdayid
mts_vw_ylt_combined_all_factors_long**aggd_for_cds**fanout_air

HiscoAIR_202601_dialsup,B
HiscoAIR_202601_domestic_euws_fagross,B
HiscoAIR_202601_domestic_euws_fix,B
HiscoAIR_202601_domestic_euws_fix_fa_fix,B
HiscoAIR_202607_domestic_euws_fagross,B
HiscoAIR_202607_domestic_euws_fix,B
HiscoAIR_202607_domestic_euws_fix_fa_fix,B
HiscoAIR_202701_domestic_euws_fagross,B
HiscoAIR_202701_domestic_euws_fix,B
HiscoAIR_202701_domestic_euws_fix_fa_fix,B
HiscoAIR_202701_euws_fagross,B
HiscoRMS_202601_dialsup,B
HiscoRMS_202601_fagross,B
HiscoRMS_202601_fl,B
HiscoRMS_202601_fl_fa_fix,B
HiscoRMS_202607_fagross,B
HiscoRMS_202607_fl,B
HiscoRMS_202607_fl_fa_fix,B
HiscoRMS_202701_fagross,B
HiscoRMS_202701_fl,B
HiscoRMS_202701_fl_fa_fix,B
