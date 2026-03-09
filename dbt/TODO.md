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
split it into source_vendor (gonna be risklink or verisk)


4. be clear where we are going to store the dev and prod duckdb files.
specified in profiles right now.
