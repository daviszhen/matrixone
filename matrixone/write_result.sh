#!/usr/bin/env bash

set -u -o pipefail
if [[ $# -lt 6 ]]; then
    echo "Usage: $0 <SIZE_MILLIONS> <OUTPUT_PREFIX> <RUNTIME_FILE> <COUNT_FILE> <TOTAL_SIZE_FILE> <DATA_SIZE_FILE> [INDEX_SIZE_FILE]" >&2
    exit 2
fi

SIZE_MILLIONS="$1"
OUTPUT_PREFIX="$2"
RUNTIME_FILE="$3"
COUNT_FILE="$4"
TOTAL_SIZE_FILE="$5"
DATA_SIZE_FILE="$6"
INDEX_SIZE_FILE="${7:-}"

source "$(dirname "$0")/_common.sh"
require_tools || exit 2

dataset_size=$((SIZE_MILLIONS * 1000000))
num_loaded_documents="$(last_field "$COUNT_FILE")"
total_size="$(last_field "$TOTAL_SIZE_FILE")"
data_size="$(last_field "$DATA_SIZE_FILE")"
index_size=0
if [[ -n "$INDEX_SIZE_FILE" && -f "$INDEX_SIZE_FILE" ]]; then
    index_size="$(last_field "$INDEX_SIZE_FILE")"
fi

for value_name in num_loaded_documents total_size data_size index_size; do
    value="${!value_name}"
    if [[ ! "$value" =~ ^[0-9]+$ ]]; then
        echo "$value_name is not an integer: '$value'" >&2
        exit 1
    fi
done

version="${MO_VERSION:-}"
if [[ -z "$version" ]]; then
    version="$(mo_sql 'SELECT VERSION()' 2>/dev/null | head -n 1 || true)"
fi
version="${version:-unknown}"
machine="${MO_MACHINE:-$(hostname)}"
topology="${MO_TOPOLOGY:-unspecified}"
result_prefix="${OUTPUT_PREFIX#_}"
output_file="$MATRIXONE_DIR/results/${result_prefix}_bluesky_${SIZE_MILLIONS}m.json"

if [[ "${PRETTY_NAME:-}" == "" && -r /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
fi
os_name="${PRETTY_NAME:-$(uname -s)}"

jq -n \
    --arg system 'MatrixOne' \
    --arg version "$version" \
    --arg os "$os_name" \
    --arg date "$(date +%F)" \
    --arg machine "$machine" \
    --arg topology "$topology" \
    --argjson dataset_size "$dataset_size" \
    --argjson num_loaded_documents "$num_loaded_documents" \
    --argjson total_size "$total_size" \
    --argjson data_size "$data_size" \
    --argjson index_size "$index_size" \
    --argjson result "$(cat "$RUNTIME_FILE")" \
    '{system:$system,version:$version,os:$os,date:$date,machine:$machine,topology:$topology,retains_structure:"yes",tags:["native_json","no_json_path_index"],dataset_size:$dataset_size,num_loaded_documents:$num_loaded_documents,total_size:$total_size,data_size:$data_size,index_size:$index_size,result:$result}' \
    >"$output_file"
echo "Benchmark result written to $output_file"
