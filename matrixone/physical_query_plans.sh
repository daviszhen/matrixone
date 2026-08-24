#!/usr/bin/env bash

set -euo pipefail
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_NAME> [OUTPUT_FILE]" >&2
    exit 2
fi

DB_NAME="$1"
source "$(dirname "$0")/_common.sh"
OUTPUT_FILE="${2:-${MATRIXONE_DIR}/physical_query_plans.txt}"
require_tools || exit 2

load_benchmark_queries
: >"$OUTPUT_FILE"
for query_index in "${!BENCHMARK_QUERIES[@]}"; do
    query="${BENCHMARK_QUERIES[$query_index]}"
    query="${query%;}"
    {
        echo "------------------------------------------------------------------------------------------------------------------------"
        echo "Physical query plan for query Q$((query_index + 1)):"
        echo
        mo_mysql --database="$DB_NAME" --batch --raw --execute="EXPLAIN $query"
    } >>"$OUTPUT_FILE" || {
        echo "EXPLAIN failed for Q$((query_index + 1))" >&2
        exit 1
    }
done
echo "Physical query plans written to $OUTPUT_FILE"
