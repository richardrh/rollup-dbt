# data/seeds/vor

VOR seed CSVs have different schemas. Because validnator validates one CSV input
at a time, each dataset has a clearly named colocated config rather than a
single folder YAML that would incorrectly require all VOR columns at once.

```bash
validnator validate -p data/seeds/vor/validnator-blending-factors.yml -i data/seeds/vor/blending_factors.csv -o validation-output/blending-factors
validnator validate -p data/seeds/vor/validnator-fx-rates.yml -i data/seeds/vor/fx_rates.csv -o validation-output/fx-rates
validnator validate -p data/seeds/vor/validnator-forecast-factors.yml -i data/seeds/vor/forecast_factors.csv -o validation-output/forecast-factors
validnator validate -p data/seeds/vor/validnator-euws-rate-factors.yml -i data/seeds/vor/euws_rate_factors.csv -o validation-output/euws-rate-factors
```
