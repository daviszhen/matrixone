#!/usr/bin/env bash

# Shared MatrixOne JSONBench settings.  This file is sourced by the adapter
# scripts and is not intended to be run directly.

set -u

MATRIXONE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MYSQL_BIN="${MYSQL_BIN:-mysql}"
MO_HOST="${MO_HOST:-127.0.0.1}"
MO_PORT="${MO_PORT:-6001}"
MO_USER="${MO_USER:-root}"
MO_PASSWORD="${MO_PASSWORD:-}"
MO_INIT_COMMAND="${MO_INIT_COMMAND:-SET time_zone='+00:00'}"
MO_LOAD_MODE="${MO_LOAD_MODE:-direct}"

if [[ -n "$MO_PASSWORD" ]]; then
    export MYSQL_PWD="$MO_PASSWORD"
fi

mo_mysql() {
    local args=(
        --protocol=tcp
        --host="$MO_HOST"
        --port="$MO_PORT"
        --user="$MO_USER"
    )
    if [[ -n "$MO_INIT_COMMAND" ]]; then
        args+=(--init-command="$MO_INIT_COMMAND")
    fi
    "$MYSQL_BIN" "${args[@]}" "$@"
}

mo_sql() {
    local sql="$1"
    mo_mysql --batch --skip-column-names --raw --execute="$sql"
}

sql_ident() {
    local value="$1"
    local escaped="${value//\`/\`\`}"
    printf '`%s`' "$escaped"
}

sql_literal() {
    local escaped="$1"
    # MatrixOne receives these values as MySQL string literals. Escape
    # backslashes before quotes so file paths containing either character do
    # not change the SQL literal sent to the server.
    escaped="${escaped//\\/\\\\}"
    escaped="${escaped//\'/\'\'}"
    escaped="${escaped//$'\n'/\\n}"
    escaped="${escaped//$'\r'/\\r}"
    printf "'%s'" "$escaped"
}

require_tools() {
    command -v "$MYSQL_BIN" >/dev/null 2>&1 || {
        echo "MatrixOne JSONBench requires mysql client: $MYSQL_BIN" >&2
        return 1
    }
    command -v jq >/dev/null 2>&1 || {
        echo "MatrixOne JSONBench requires jq" >&2
        return 1
    }
    if [[ "$MO_LOAD_MODE" == "local" ]] && ! command -v gzip >/dev/null 2>&1; then
        echo "MatrixOne JSONBench local loading requires gzip" >&2
        return 1
    fi
}

check_connection() {
    mo_sql 'SELECT 1' >/dev/null
}

load_benchmark_queries() {
    mapfile -t BENCHMARK_QUERIES < <(awk 'NF { print }' "$MATRIXONE_DIR/queries.sql")
    if (( ${#BENCHMARK_QUERIES[@]} != 5 )); then
        echo "expected exactly 5 benchmark queries, found ${#BENCHMARK_QUERIES[@]}" >&2
        return 2
    fi
}

load_file_sql() {
    local db="$1"
    local table="$2"
    local file="$3"
    local file_sql
    file_sql="$(sql_literal "$file")"
    local table_sql
    table_sql="$(sql_ident "$db").$(sql_ident "$table")"

    if [[ "$MO_LOAD_MODE" == "direct" ]]; then
        # JSONBench is newline-delimited JSON.  A literal tab cannot occur in
        # valid JSON, so it is a safe one-field delimiter.  Disabling CSV
        # escaping is essential: JSON backslashes must be preserved verbatim.
        mo_sql "LOAD DATA INFILE {'filepath'=${file_sql}, 'compression'='gzip', 'format'='csv'} INTO TABLE ${table_sql} FIELDS TERMINATED BY '\\t' ESCAPED BY '' LINES TERMINATED BY '\\n';"
        return
    fi

    if [[ "$MO_LOAD_MODE" != "local" ]]; then
        echo "MO_LOAD_MODE must be direct or local, got: $MO_LOAD_MODE" >&2
        return 2
    fi

    local temp_dir temp_file
    temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/jsonbench-mo.XXXXXX")"
    temp_file="$temp_dir/$(basename "${file%.gz}")"
    if ! gzip -dc -- "$file" >"$temp_file"; then
        rm -rf -- "$temp_dir"
        return 1
    fi

    file_sql="$(sql_literal "$temp_file")"
    # LOCAL mode is a fallback for servers that cannot read the gzip path
    # directly.  It is intentionally explicit because decompression is not a
    # database runtime measurement in JSONBench.
    mo_mysql --local-infile=1 --batch --skip-column-names --raw --execute="LOAD DATA LOCAL INFILE ${file_sql} INTO TABLE ${table_sql} FIELDS TERMINATED BY '\\t' ESCAPED BY '' LINES TERMINATED BY '\\n';"
    local status=$?
    rm -rf -- "$temp_dir"
    return "$status"
}

drop_page_cache() {
    [[ "${MO_DROP_CACHES:-1}" == "0" ]] && return 0

    if [[ -w /proc/sys/vm/drop_caches ]] &&
        printf '3\n' >/proc/sys/vm/drop_caches 2>/dev/null; then
        return 0
    fi

    if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1 &&
        printf '3\n' | sudo -n tee /proc/sys/vm/drop_caches >/dev/null 2>&1; then
        return 0
    fi

    echo "cannot drop host page cache; set MO_DROP_CACHES=0 to explicitly run without cache dropping" >&2
    return 1
}

last_field() {
    awk 'NF { value=$NF } END { if (value != "") print value }' "$1"
}

table_status_sizes() {
    local db="$1"
    local table="$2"
    local status
    local data_size
    local index_size

    for _ in 1 2 3 4 5 6 7 8 9 10; do
        status="$(mo_mysql --batch --skip-column-names --raw \
            --execute="SHOW TABLE STATUS FROM $(sql_ident "$db") LIKE $(sql_literal "$table");" \
            2>/dev/null | tail -n 1 || true)"
        data_size="$(printf '%s\n' "$status" | awk -F '\t' 'NF >= 8 { print $6 }')"
        index_size="$(printf '%s\n' "$status" | awk -F '\t' 'NF >= 8 { print $8 }')"
        if [[ "$data_size" =~ ^[0-9]+$ && "$index_size" =~ ^[0-9]+$ ]]; then
            if (( data_size > 0 || index_size > 0 )); then
                printf '%s\t%s\n' "$data_size" "$index_size"
                return 0
            fi
        fi
        sleep 1
    done

    if [[ "$data_size" =~ ^[0-9]+$ && "$index_size" =~ ^[0-9]+$ ]] &&
        (( data_size > 0 || index_size > 0 )); then
        printf '%s\t%s\n' "$data_size" "$index_size"
        return 0
    fi
    return 1
}
