# data/ep_summaries

Long-format EP summary CSV inputs live under vendor subfolders in this section.
Validate a long EP summary CSV before rollup consumes it with the colocated
validnator pipeline:

```bash
validnator validate \
  -p data/ep_summaries/validnator.yml \
  -i data/ep_summaries/verisk/verisk_ep_summary.long.csv \
  -o validation-output/ep-summary
```
