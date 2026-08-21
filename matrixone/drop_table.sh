#!/usr/bin/env bash

set -u -o pipefail
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_NAME>" >&2
    exit 2
fi

source "$(dirname "$0")/_common.sh"
require_tools || exit 2
mo_sql "DROP DATABASE IF EXISTS $(sql_ident "$1");"
