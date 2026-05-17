# EP Summaries

These files are optional review inputs for vendor EP summaries.

Drop long-format CSVs here. Normal pipeline runs use the reviewed
`data/seeds/vor/blending_weights.csv` seed; EP summary CSVs do not calculate or
override model blending weights. Original xlsx files may coexist but are not
read after conversion.

For the experimental pipeline2 path, the authoritative EP summary input contract
is [`schema.yaml`](schema.yaml) in this directory so schema edits stay alongside
the EP files they describe.

---

## RiskLink — `risklink/*.long.csv`

| column | type | notes |
|--------|------|-------|
| `id` | integer | RiskLink analysis ID |
| `rp` | integer | return period; `0` = AAL row |
| `ep_type` | string | `AAL`, `OEP`, or `AEP` |
| `lob` | string | modelled LOB — must match `analyses.modelled_label` |
| `region_peril` | string | peril label — must match `analyses.modelled_label` |
| `gl` | float | gross loss |

Sample:
```
id,rp,ep_type,lob,region_peril,gl
1,0,AAL,HIC_HH_UK,GB FL HD,1806464.0
1,100,OEP,HIC_HH_UK,GB FL HD,19365339.0
1,1000,OEP,HIC_HH_UK,GB FL HD,62873626.0
1,0,AAL,HIC_HH_UK,GB WSSS,10775338.0
```

To generate from the RMS xlsx export: `rollup ep-summary-to-csv <file>.xlsx`

---

## Verisk — `verisk/*.long.csv`

| column | type | notes |
|--------|------|-------|
| `rp` | integer | return period; `0` = AAL row |
| `ep_type` | string | `AAL`, `OEP`, or `AEP` |
| `analysis` | string | Verisk analysis label — must match `analyses.modelled_label` |
| `lob` | string | modelled LOB |
| `gl` | float | gross loss |

Sample:
```
rp,ep_type,analysis,lob,gl
0,AAL,EU_WS,HIC_HH_UK,5421000.0
100,OEP,EU_WS,HIC_HH_UK,18200000.0
0,AAL,GB_FL,HIC_HH_UK,1650000.0
100,OEP,GB_FL,HIC_HH_UK,17800000.0
```

No automated converter exists yet — produce this CSV directly.

---

## Return period set

`rp` must be one of: `0, 2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 10000`

Missing return periods are silently skipped (a sparse file is fine). Only AAL
(`ep_type=AAL`, `rp=0`) is used for blending weight derivation; the full curve
is available for future diagnostics.
