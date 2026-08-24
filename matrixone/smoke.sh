#!/usr/bin/env bash

set -euo pipefail
DATA_DIRECTORY="${1:-${HOME}/data/bluesky}"
DB_NAME="${2:-jsonbench_matrixone_smoke}"

source "$(dirname "$0")/_common.sh"
require_tools || exit 2
check_connection || {
    echo "MatrixOne is not reachable at $MO_HOST:$MO_PORT" >&2
    exit 1
}

[[ -d "$DATA_DIRECTORY" ]] || {
    echo "data directory does not exist: $DATA_DIRECTORY" >&2
    exit 2
}

success_log="$(mktemp "${TMPDIR:-/tmp}/jsonbench-mo-smoke-success.XXXXXX")"
error_log="$(mktemp "${TMPDIR:-/tmp}/jsonbench-mo-smoke-error.XXXXXX")"
trap 'rm -f -- "$success_log" "$error_log"' EXIT

"$MATRIXONE_DIR/create_and_load.sh" "$DB_NAME" bluesky "$DATA_DIRECTORY" 1 "$success_log" "$error_log"
count="$("$MATRIXONE_DIR/count.sh" "$DB_NAME" bluesky)"
echo "loaded documents: $count"

if [[ ! "$count" =~ ^[1-9][0-9]*$ ]]; then
    echo "smoke test loaded no documents" >&2
    exit 1
fi

echo "checking nested JSON extraction"
mo_sql "SELECT data -> '$.commit' ->> '$.collection' FROM $(sql_ident "$DB_NAME").bluesky LIMIT 1;"
echo "checking fractional epoch conversion"
mo_sql "SELECT FROM_UNIXTIME(1705319696.123456), TIMESTAMPDIFF(MICROSECOND, FROM_UNIXTIME(1705319696.123456), FROM_UNIXTIME(1705319697.123456));"
"$MATRIXONE_DIR/query_results.sh" "$DB_NAME" "$MATRIXONE_DIR/smoke_query_results.txt"

if [[ "${MO_KEEP_SMOKE_DB:-0}" != "1" ]]; then
    "$MATRIXONE_DIR/drop_table.sh" "$DB_NAME"
else
    echo "keeping smoke database $DB_NAME (MO_KEEP_SMOKE_DB=1)"
fi
echo "MatrixOne JSONBench smoke test passed"
