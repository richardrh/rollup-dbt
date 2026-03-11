1. add numeric data type and >= 0 test to AAL, OEP_200
etc inside models/raw/_sources.yml

2. discuss if we have validated the source in raw, we dont need to
validate or test the same column of data again in staging?
This seems like a waste of time.


3. We are going to keep models/intermediate/010_ep/int_ep_combined
but we probably are not going to use it as we might use the
analysis lists as previously.

3b. we take the analysis list from stg_cat_modelling_results__analysis_lists
and join to itself.
split it into source_vendor (gonna be risklink or verisk) or instead of splitting
the table in two queries, join to itself but split by source_vendor -
the output should look like this:

risklink_aal, risklink_oep_200, risklink_oep_1000, verisk_aal, verisk_oep_200, verisk_oep_1000

joined on modelled_lob, rollup_lob, and peril


4. be clear where we are going to store the dev and prod duckdb files.
specified in profiles right now.
