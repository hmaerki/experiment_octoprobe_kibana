```bash
find ~/experiment/experiment_octoprobe_kibana/kibana/2026-02-27_sample_data/reports/ch_hans_1_20260226-205450 -type f -not -name "*.json" -delete

find ~/experiment/experiment_octoprobe_kibana/kibana/2026-02-27_sample_data/reports/ch_hans_1_20260226-205450 -type f -name "_results.json" -delete

find ~/experiment/experiment_octoprobe_kibana/kibana/2026-02-27_sample_data/reports/ch_hans_1_20260226-205450 -type d -empty -delete
```
