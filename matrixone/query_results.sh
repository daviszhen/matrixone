#!/usr/bin/env bash

set -euo pipefail
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_NAME> [OUTPUT_FILE]" >&2
    exit 2
fi

DB_NAME="$1"
OUTPUT_FILE="${2:-${MATRIXONE_DIR}/query_results.txt}"
source "$(dirname "$0")/_common.sh"
require_tools || exit 2

mapfile -t QUERIES < <(awk 'NF { print }' "$MATRIXONE_DIR/queries.sql")
: >"$OUTPUT_FILE"
for query_index in "${!QUERIES[@]}"; do
    {
        echo "------------------------------------------------------------------------------------------------------------------------"
        echo "Result for query Q$((query_index + 1)):"
        echo
        mo_mysql --database="$DB_NAME" --batch --raw --execute="${QUERIES[$query_index]}"
    } >>"$OUTPUT_FILE"
done
echo "Query results written to $OUTPUT_FILE"
