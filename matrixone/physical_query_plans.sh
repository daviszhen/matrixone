#!/usr/bin/env bash

set -u -o pipefail
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_NAME> [OUTPUT_FILE]" >&2
    exit 2
fi

DB_NAME="$1"
OUTPUT_FILE="${2:-${MATRIXONE_DIR}/physical_query_plans.txt}"
source "$(dirname "$0")/_common.sh"
require_tools || exit 2

mapfile -t QUERIES < <(awk 'NF { print }' "$MATRIXONE_DIR/queries.sql")
: >"$OUTPUT_FILE"
for query_index in "${!QUERIES[@]}"; do
    query="${QUERIES[$query_index]}"
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
