#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 6 ]]; then
    echo "Usage: $0 <DATA_SOURCE> <DB_NAME> <TABLE_NAME> <MAX_FILES> <SUCCESS_LOG> <ERROR_LOG>" >&2
    exit 2
fi

DATA_SOURCE="$1"
DB_NAME="$2"
TABLE_NAME="$3"
MAX_FILES="$4"
SUCCESS_LOG="$5"
ERROR_LOG="$6"

source "$(dirname "$0")/_common.sh"
require_tools || exit 2

[[ "$MAX_FILES" =~ ^[1-9][0-9]*$ ]] || { echo "MAX_FILES must be positive: $MAX_FILES" >&2; exit 2; }

if [[ "$MO_LOAD_MODE" == "oss" ]]; then
    [[ "$DATA_SOURCE" =~ ^oss://[^/]+/.+ ]] || {
        echo "OSS data source must be an oss://bucket/path URI: $DATA_SOURCE" >&2
        exit 2
    }
else
    [[ -d "$DATA_SOURCE" ]] || { echo "data directory does not exist: $DATA_SOURCE" >&2; exit 2; }
fi

if [[ "$MO_LOAD_MODE" != "oss" ]]; then
    mapfile -d '' FILES < <(find "$DATA_SOURCE" -maxdepth 1 -type f -name '*.json.gz' -print0 | sort -z)
    if (( ${#FILES[@]} < MAX_FILES )); then
        echo "not enough .json.gz files: need $MAX_FILES, found ${#FILES[@]}" >&2
        exit 2
    fi
fi

mkdir -p "$(dirname "$SUCCESS_LOG")" "$(dirname "$ERROR_LOG")"
counter=0
failed=0

while (( counter < MAX_FILES )); do
    if [[ "$MO_LOAD_MODE" == "oss" ]]; then
        counter=$((counter + 1))
        file="${DATA_SOURCE%/}/file_$(printf '%04d' "$counter").json.gz"
    else
        file="${FILES[$counter]}"
        counter=$((counter + 1))
    fi
    started=$(date +%s)
    echo "Loading [$counter/$MAX_FILES] $file"

    if load_file_sql "$DB_NAME" "$TABLE_NAME" "$file"; then
        elapsed=$(( $(date +%s) - started ))
        printf '[%s] loaded %s in %ss\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$file" "$elapsed" >>"$SUCCESS_LOG"
    else
        elapsed=$(( $(date +%s) - started ))
        printf '[%s] failed %s after %ss (MO_LOAD_MODE=%s)\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$file" "$elapsed" "$MO_LOAD_MODE" >>"$ERROR_LOG"
        failed=1
        # A bad object must not prevent the remaining OSS objects from being
        # loaded.  The caller can opt into continuing the benchmark with the
        # successfully loaded subset (the dev wrapper does this explicitly).
        continue
    fi
done

if (( failed )) && [[ "${MO_CONTINUE_ON_LOAD_ERROR:-0}" != "1" ]]; then
    exit 1
fi
exit 0
