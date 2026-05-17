# data/ep_summaries

`schema.yaml` is the pipeline2 source of truth for optional long-format EP
summary review inputs.

Pipeline2 expects operators or tests to provide CSVs under the glob declared in
that manifest. The repository intentionally keeps only the YAML contract here;
legacy XLSX conversion files and commands were removed.
