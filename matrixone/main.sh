#!/usr/bin/env bash

set -euo pipefail

DEFAULT_CHOICE=ask
DEFAULT_DATA_SOURCE="${HOME}/data/bluesky"
CHOICE="${1:-$DEFAULT_CHOICE}"
DATA_SOURCE="${2:-$DEFAULT_DATA_SOURCE}"
SUCCESS_LOG="${3:-success.log}"
ERROR_LOG="${4:-error.log}"
OUTPUT_PREFIX="${5:-_m6i.8xlarge}"

source "$(dirname "$0")/_common.sh"
require_tools || exit 2
check_connection || {
    echo "MatrixOne is not reachable at $MO_HOST:$MO_PORT" >&2
    exit 1
}

if [[ "$MO_LOAD_MODE" != "oss" ]]; then
    [[ -d "$DATA_SOURCE" ]] || {
        echo "data directory does not exist: $DATA_SOURCE" >&2
        exit 2
    }
fi

if [[ "$CHOICE" == "ask" ]]; then
    echo "Select the dataset size to benchmark:"
    echo "1) 1m (default)"
    echo "2) 10m"
    echo "3) 100m"
    echo "4) 1000m"
    echo "5) all"
    read -r -p "Enter the number corresponding to your choice: " CHOICE
fi

benchmark() {
    local size="$1"
    local file_count
    if [[ "$MO_LOAD_MODE" == "oss" ]]; then
        file_count="${MO_REMOTE_FILE_COUNT:-$size}"
    else
        file_count=$(find "$DATA_SOURCE" -maxdepth 1 -type f -name '*.json.gz' | wc -l)
    fi
    if [[ ! "$file_count" =~ ^[0-9]+$ ]]; then
        echo "remote file count must be a non-negative integer: $file_count" >&2
        return 2
    fi
    if (( file_count < size )); then
        echo "not enough data files: need $size, found $file_count" >&2
        return 2
    fi

    # Keep the historical database names by default. The dev wrapper uses a
    # separate prefix because this benchmark drops the database after each
    # size and must not collide with another tenant's JSONBench run.
    local db_prefix="${MO_DB_NAME_PREFIX:-bluesky}"
    local db_name="${db_prefix}_${size}m"
    local artifact_prefix="${OUTPUT_PREFIX}_bluesky_${size}m"
    "$MATRIXONE_DIR/create_and_load.sh" "$db_name" bluesky "$DATA_SOURCE" "$size" "$SUCCESS_LOG" "$ERROR_LOG"
    "$MATRIXONE_DIR/count.sh" "$db_name" bluesky | tee "${artifact_prefix}.count"
    "$MATRIXONE_DIR/total_size.sh" "$db_name" bluesky | tee "${artifact_prefix}.total_size"
    "$MATRIXONE_DIR/data_size.sh" "$db_name" bluesky | tee "${artifact_prefix}.data_size"
    "$MATRIXONE_DIR/index_size.sh" "$db_name" bluesky | tee "${artifact_prefix}.index_size"
    "$MATRIXONE_DIR/query_results.sh" "$db_name" "${artifact_prefix}.query_results"
    "$MATRIXONE_DIR/physical_query_plans.sh" "$db_name" "${artifact_prefix}.physical_query_plans"
    "$MATRIXONE_DIR/benchmark.sh" "$db_name" "${artifact_prefix}.results_runtime" "${artifact_prefix}.query_errors"
    "$MATRIXONE_DIR/write_result.sh" "$size" "$OUTPUT_PREFIX" \
        "${artifact_prefix}.results_runtime" \
        "${artifact_prefix}.count" \
        "${artifact_prefix}.total_size" \
        "${artifact_prefix}.data_size" \
        "${artifact_prefix}.index_size"
    "$MATRIXONE_DIR/drop_table.sh" "$db_name"
}

case "$CHOICE" in
    1) benchmark 1 ;;
    2) benchmark 10 ;;
    3) benchmark 100 ;;
    4) benchmark 1000 ;;
    5)
        benchmark 1 && benchmark 10 && benchmark 100 && benchmark 1000
        ;;
    *)
        echo "choice must be 1, 2, 3, 4, or 5" >&2
        exit 2
        ;;
esac
