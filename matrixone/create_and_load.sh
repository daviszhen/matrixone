#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 6 ]]; then
    echo "Usage: $0 <DB_NAME> <TABLE_NAME> <DATA_SOURCE> <NUM_FILES> <SUCCESS_LOG> <ERROR_LOG>" >&2
    exit 2
fi

DB_NAME="$1"
TABLE_NAME="$2"
DATA_SOURCE="$3"
NUM_FILES="$4"
SUCCESS_LOG="$5"
ERROR_LOG="$6"

source "$(dirname "$0")/_common.sh"
require_tools || exit 2
check_connection || { echo "cannot connect to MatrixOne at $MO_HOST:$MO_PORT" >&2; exit 1; }

[[ "$DB_NAME" =~ ^[A-Za-z0-9_]+$ ]] || { echo "unsafe database name: $DB_NAME" >&2; exit 2; }
[[ "$TABLE_NAME" =~ ^[A-Za-z0-9_]+$ ]] || { echo "unsafe table name: $TABLE_NAME" >&2; exit 2; }

echo "Creating database $DB_NAME"
mo_sql "DROP DATABASE IF EXISTS $(sql_ident "$DB_NAME"); CREATE DATABASE $(sql_ident "$DB_NAME");"
echo "Creating table $DB_NAME.$TABLE_NAME"
mo_mysql --database="$DB_NAME" --batch --skip-column-names --raw <"$MATRIXONE_DIR/ddl.sql"
echo "Loading $NUM_FILES file(s) into $DB_NAME.$TABLE_NAME"
"$MATRIXONE_DIR/load_data.sh" "$DATA_SOURCE" "$DB_NAME" "$TABLE_NAME" "$NUM_FILES" "$SUCCESS_LOG" "$ERROR_LOG"
