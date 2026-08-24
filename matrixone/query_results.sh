#!/usr/bin/env bash

set -euo pipefail
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_NAME> [OUTPUT_FILE]" >&2
    exit 2
fi

DB_NAME="$1"
source "$(dirname "$0")/_common.sh"
OUTPUT_FILE="${2:-${MATRIXONE_DIR}/query_results.txt}"
require_tools || exit 2

load_benchmark_queries
: >"$OUTPUT_FILE"
for query_index in "${!BENCHMARK_QUERIES[@]}"; do
    {
        echo "------------------------------------------------------------------------------------------------------------------------"
        echo "Result for query Q$((query_index + 1)):"
        echo
        mo_mysql --database="$DB_NAME" --batch --raw --execute="${BENCHMARK_QUERIES[$query_index]}"
    } >>"$OUTPUT_FILE"
done
echo "Query results written to $OUTPUT_FILE"
