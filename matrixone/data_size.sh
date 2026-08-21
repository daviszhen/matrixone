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
    read -r data_size _ <<<"$sizes"
    printf '%s\n' "$data_size"
else
    mo_sql "SELECT mo_table_size($(sql_literal "$1"), $(sql_literal "$2"));"
fi
