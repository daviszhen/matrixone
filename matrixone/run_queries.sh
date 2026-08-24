#!/usr/bin/env bash

set -euo pipefail
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <DB_NAME> <RESULT_FILE_RUNTIMES> [ERROR_LOG]" >&2
    exit 2
fi

DB_NAME="$1"
RESULT_FILE="$2"
ERROR_LOG="${3:-query_errors.log}"

source "$(dirname "$0")/_common.sh"
require_tools || exit 2
check_connection || { echo "cannot connect to MatrixOne at $MO_HOST:$MO_PORT" >&2; exit 1; }

QUERY_FILE="$MATRIXONE_DIR/queries.sql"
load_benchmark_queries

mkdir -p "$(dirname "$RESULT_FILE")" "$(dirname "$ERROR_LOG")"
: >"$ERROR_LOG"

rows=()
for query_index in "${!BENCHMARK_QUERIES[@]}"; do
    query="${BENCHMARK_QUERIES[$query_index]}"
    runtimes=()
    echo "Running query Q$((query_index + 1))"
    drop_page_cache

    for attempt in 1 2 3; do
        time_file="$(mktemp "${TMPDIR:-/tmp}/jsonbench-mo-time.XXXXXX")"
        stderr_file="$(mktemp "${TMPDIR:-/tmp}/jsonbench-mo-error.XXXXXX")"
        # Bash functions cannot be passed directly to /usr/bin/time. Use
        # Bash's reserved-word time so mo_mysql remains the shared wrapper.
        TIMEFORMAT='%R'
        { time mo_mysql --database="$DB_NAME" --batch --skip-column-names --raw \
            --execute="$query" >/dev/null 2>"$stderr_file"; } 2>"$time_file"
        status=$?

        if (( status != 0 )); then
            {
                echo "Q$((query_index + 1)) attempt $attempt failed (status=$status)"
                cat "$stderr_file"
                echo "SQL: $query"
            } >>"$ERROR_LOG"
            rm -f -- "$time_file" "$stderr_file"
            exit 1
        fi

        runtime="$(cat "$time_file")"
        if [[ ! "$runtime" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
            echo "Q$((query_index + 1)) attempt $attempt produced invalid runtime: $runtime" >>"$ERROR_LOG"
            rm -f -- "$time_file" "$stderr_file"
            exit 1
        fi
        runtimes+=("$runtime")
        rm -f -- "$time_file" "$stderr_file"
    done

    row="$(IFS=,; echo "${runtimes[*]}")"
    rows+=("[$row]")
done

result="$(IFS=,; echo "${rows[*]}")"
printf '[%s]\n' "$result" >"$RESULT_FILE"
echo "Runtime results written to $RESULT_FILE"
