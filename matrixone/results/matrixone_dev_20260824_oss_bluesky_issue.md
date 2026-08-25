# MatrixOne dev JSONBench Bluesky workload results

## Environment

- MatrixOne version: `8.0.30-MatrixOne-v4.2.0`
- Test date: `2026-08-24`
- Data source: `oss://mo-bench/bluesky`, loaded directly with MatrixOne `LOAD DATA URL`
- Table: `bluesky` with native JSON data; no JSON path index
- Query timings: seconds; three sequential attempts per query unless marked skipped
- Q2 at 100M was skipped after the first formal attempt was terminated because the query was taking more than an hour. The separate Q2 result-display query completed in about 69 minutes.

## Results

| Dataset | Loaded rows | Data size (GiB) | Q1 (s) | Q2 (s) | Q3 (s) | Q4 (s) | Q5 (s) |
|---|---:|---:|---|---|---|---|---|
| 1M | 1,000,000 | 0.21 | 0.324 / 0.312 / 0.288 | 3.663 / 3.678 / 4.227 | 0.912 / 0.912 / 0.925 | 0.595 / 0.496 / 0.523 | 0.624 / 0.550 / 0.549 |
| 10M | 9,999,997 | 2.05 | 1.614 / 1.127 / 1.063 | 211.144 / 208.714 / 209.755 | 6.646 / 7.177 / 8.159 | 2.777 / 2.336 / 3.486 | 2.721 / 3.250 / 2.575 |
| 100M | 99,999,984 | 20.02 | 20.674 / 20.230 / 13.018 | skipped | 42.100 / 31.017 / 30.699 | 14.270 / 11.586 / 12.525 | 14.735 / 16.858 / 15.913 |

## Notes

- No query errors were recorded for the completed queries.
- Data size in the table uses `1 GiB = 2^30 bytes`; the source JSON files retain the exact byte counts.
- The 10M and 100M loads are short of the nominal size by 3 and 16 rows respectively.
- The 100M Q2 formal runtime is intentionally not reported as a completed benchmark result.

Source result files:

- `matrixone/results/matrixone_dev_20260824_oss_bluesky_1m.json`
- `matrixone/results/matrixone_dev_20260824_oss_bluesky_10m.json`
- `matrixone/results/matrixone_dev_20260824_oss_bluesky_q2_skipped_bluesky_100m.json`
