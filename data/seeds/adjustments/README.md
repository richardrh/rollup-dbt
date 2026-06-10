# data/seeds/adjustments

Adjustment seed CSVs are colocated with their validnator configs. The current
adjustments input is `euws_rank_overrides.csv`, validated by the folder-level
config because this section currently has one CSV schema.

```bash
validnator validate -p data/seeds/adjustments/validnator.yml -i data/seeds/adjustments/euws_rank_overrides.csv -o validation-output/euws-rank-overrides
```
