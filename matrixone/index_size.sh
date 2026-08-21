#!/usr/bin/env bash

set -u -o pipefail
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <DB_NAME> <TABLE_NAME>" >&2
    exit 2
fi

source "$(dirname "$0")/_common.sh"
require_tools || exit 2

sizes="$(table_status_sizes "$1" "$2" || true)"
if [[ -n "$sizes" ]]; then
    read -r _ index_size <<<"$sizes"
    printf '%s\n' "$index_size"
else
    # The baseline has no user-created JSON path indexes.
    printf '0\n'
fi
