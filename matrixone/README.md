# MatrixOne adapter for JSONBench

This adapter targets a MatrixOne server that is already running through the
MySQL protocol. It does not install, start, stop, or reconfigure MatrixOne.

## Connection

```bash
export MO_HOST=127.0.0.1
export MO_PORT=6001
export MO_USER=root
export MO_PASSWORD=111
export MO_MACHINE='m6i.8xlarge, 10000gib gp3'
export MO_TOPOLOGY='record the CN/TN/Log topology here'
```

`MO_INIT_COMMAND` defaults to `SET time_zone='+00:00'` so the temporal query
results are reproducible. Set it explicitly when comparing against another
JSONBench system.

## Data loading

The baseline keeps the complete NDJSON document in one native `JSON` column.
The default `MO_LOAD_MODE=direct` asks MatrixOne to read each `.json.gz` file
using `LOAD DATA INFILE` with gzip compression. If the server cannot access the
file path, use `MO_LOAD_MODE=local`; this decompresses to a temporary file and
uses `LOAD DATA LOCAL INFILE`.

The loader disables CSV escaping because JSON backslashes must be preserved.
The tab delimiter is safe for valid JSON lines, where a literal tab is not
allowed. The first run must be the smoke test below; inspect the row count and
JSON extraction before starting a performance run.

The query port casts Bluesky's 16-digit `time_us` values to `DECIMAL(20,0)`
before dividing by one million. A scale-bearing `DECIMAL(20,6)` is too narrow
for this integer epoch and would clamp the value before `FROM_UNIXTIME`.

## Smoke test

```bash
./smoke.sh /path/to/data/bluesky
# or, when server-side gzip access is unavailable:
MO_LOAD_MODE=local ./smoke.sh /path/to/data/bluesky
```

The smoke test loads the first file, checks nested JSON extraction and
fractional epoch conversion, executes all five benchmark queries once, and
drops the temporary database unless `MO_KEEP_SMOKE_DB=1` is set.

## Benchmark

```bash
./main.sh 1 /path/to/data/bluesky success.log error.log _matrixone
./main.sh 2 /path/to/data/bluesky success.log error.log _matrixone
```

The choices are 1m, 10m, 100m, 1000m, and 5 for all sizes. Each query is run
three times, with a host page-cache drop before each query as required by the
JSONBench methodology. Set `MO_DROP_CACHES=0` when the host cannot grant that
operation and record the resulting cache policy in the benchmark report.

Runtime, count, size, query-result, and physical-plan artifacts are written
next to the command output. A JSONBench-compatible result is written under
`results/` after a successful run. `mo_table_size` is used for MatrixOne's
logical table-size metric; it must not be presented as byte-for-byte equivalent
to another engine's `bytes_on_disk` field.
