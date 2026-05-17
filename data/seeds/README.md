# data/seeds

`schema.yaml` is the pipeline2 source of truth for seed-shaped inputs.

Pipeline2 expects operators or tests to provide CSVs at the paths declared in
that manifest. The repository intentionally keeps only the YAML contract here;
legacy seed loaders, enum schemas, fixtures, and generated reference files were
removed.
