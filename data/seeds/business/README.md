# data/seeds/business

Business seed CSVs have different schemas. Because validnator validates one CSV
input at a time, each dataset has a clearly named colocated config rather than a
single folder YAML that would incorrectly require all business columns at once.

```bash
uv run validnator validate -p data/seeds/business/validnator-lobs.yml -i data/seeds/business/lobs.csv -o validation-output/lobs
uv run validnator validate -p data/seeds/business/validnator-perils.yml -i data/seeds/business/perils.csv -o validation-output/perils
```
