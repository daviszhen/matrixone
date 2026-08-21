#!/usr/bin/env bash

set -u -o pipefail
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <DB_NAME> <TABLE_NAME>" >&2
    exit 2
fi

source "$(dirname "$0")/_common.sh"
require_tools || exit 2
mo_sql "SELECT COUNT(*) FROM $(sql_ident "$1").$(sql_ident "$2");"
