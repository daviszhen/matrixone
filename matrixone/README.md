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
The default `MO_LOAD_MODE=direct` asks MatrixOne to read each local `.json.gz`
file using `LOAD DATA INFILE` with gzip compression. For the shared dev
environment, `MO_LOAD_MODE=oss` sends a `LOAD DATA URL S3OPTION` statement and
lets MatrixOne read each `.json.gz` object directly from OSS. If the server
cannot access either source, use `MO_LOAD_MODE=local`; this decompresses to a
temporary file and uses `LOAD DATA LOCAL INFILE`.

The loader disables CSV escaping because JSON backslashes must be preserved.
The tab delimiter is safe for valid JSON lines, where a literal tab is not
allowed. The first run must be the smoke test below; inspect the row count and
JSON extraction before starting a performance run.

The query port casts Bluesky's 16-digit `time_us` values to `DECIMAL(20,0)`
before dividing by one million. A scale-bearing `DECIMAL(20,6)` is too narrow
for this integer epoch and would clamp the value before `FROM_UNIXTIME`.

The benchmark queries use MatrixOne's MySQL JSON operators: `->` extracts a
JSON value and `->>` extracts an unquoted scalar. Operands use full MySQL JSON
paths, for example `data -> '$.commit' ->> '$.collection'`.

## Smoke test

```bash
./smoke.sh /path/to/data/bluesky
# or, when server-side gzip access is unavailable:
MO_LOAD_MODE=local ./smoke.sh /path/to/data/bluesky
# or, read the first object directly from OSS:
MO_LOAD_MODE=oss ./smoke.sh oss://mo-bench/bluesky
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

## MatrixOne dev environment

`dev.sh` loads the selected files directly from Aliyun OSS into a dedicated
database on the MatrixOne dev endpoint. It does not download or decompress
the data locally. MatrixOne receives one `LOAD DATA URL S3OPTION` statement
per object and reads the compressed NDJSON server-side.

The script reads OSS settings from an optional deployment-provided
`MO_OSS_CONFIG` file (simple `KEY=VALUE` lines) or from environment variables.
Environment variables take precedence. Credentials are never committed or
printed by the adapter; provide them through the mounted config file or the
`OSS_*` environment overrides.

Prerequisites:

```bash
command -v mysql
```

Example deployment config (keep the real file outside Git):

```dotenv
OSS_ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com
OSS_REGION=oss-cn-hangzhou
OSS_ACCESS_KEY_ID=<access-key-id>
OSS_SECRET_ACCESS_KEY=<access-key-secret>
```

Run with `MO_OSS_CONFIG=/run/secrets/jsonbench-oss.env`, or set
`OSS_ENDPOINT`, `OSS_REGION`, `OSS_ACCESS_KEY_ID`, and
`OSS_SECRET_ACCESS_KEY` directly in the process environment.

Run the one-million-row smoke/performance workload:

```bash
cd matrixone
MO_DEV_PASSWORD='<password-from-your-secret-store>' ./dev.sh 1
```

The first argument follows `main.sh`: `1`, `2`, `3`, `4`, or `5` means 1m,
10m, 100m, 1000m, or all sizes. The OSS source defaults to
`oss://mo-bench/bluesky`; override it with `MO_DEV_OSS_PREFIX` (or pass the
source as the second argument). The connection
defaults identify the shared dev endpoint and can be overridden with
`MO_DEV_HOST`, `MO_DEV_PORT`, and `MO_DEV_USER`. The password must be supplied
through `MO_DEV_PASSWORD`, `MO_PASSWORD`, or `MYSQL_PWD`; it is not a script
default.

The OSS region is derived from the configured endpoint when `OSS_REGION` is
omitted (`oss-cn-hangzhou` for the default Aliyun endpoint). Override
`OSS_ENDPOINT`, `OSS_REGION`, `OSS_ACCESS_KEY_ID`, or
`OSS_SECRET_ACCESS_KEY` when needed. `MO_LOAD_PARALLEL=true` is the default
and can be set to `false` for a serial load.

If one OSS object fails to load, `dev.sh` records its URI and error timing in
`error.log`, continues loading the remaining objects, and runs the queries on
the successfully loaded subset. Query failures still stop the benchmark.

Because the dev endpoint is remote, `MO_DROP_CACHES` defaults to `0`. This is
an operational dev run and is not a cold-cache JSONBench result unless the
runner explicitly enables cache dropping. Database names use the
`jsonbench_dev_bluesky` prefix and are dropped by `main.sh` after each size;
change `MO_DB_NAME_PREFIX` if an isolated tenant/database namespace is needed.
