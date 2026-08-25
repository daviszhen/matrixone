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
MO_OSS_CONFIG="${MO_OSS_CONFIG:-${OSS_CONFIG_FILE:-}}"
OSS_ENDPOINT="${OSS_ENDPOINT:-}"
OSS_REGION="${OSS_REGION:-}"
OSS_ACCESS_KEY_ID="${OSS_ACCESS_KEY_ID:-}"
OSS_SECRET_ACCESS_KEY="${OSS_SECRET_ACCESS_KEY:-${OSS_ACCESS_KEY_SECRET:-}}"
MO_LOAD_PARALLEL="${MO_LOAD_PARALLEL:-true}"

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

read_oss_config_value() {
    local wanted="$1"
    awk -F= -v wanted="$wanted" '
        function trim(value) {
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
            return value
        }
        /^[[:space:]]*(#|;|$)/ { next }
        {
            key = trim($1)
            sub(/^export[[:space:]]+/, "", key)
            if (key == wanted) {
                value = trim(substr($0, index($0, "=") + 1))
                if (substr(value, 1, 1) == "\"" && substr(value, length(value), 1) == "\"") {
                    value = substr(value, 2, length(value) - 2)
                } else {
                    single_quote = sprintf("%c", 39)
                    if (substr(value, 1, 1) == single_quote && substr(value, length(value), 1) == single_quote) {
                        value = substr(value, 2, length(value) - 2)
                    }
                }
                print value
                exit
            }
        }
    ' "$MO_OSS_CONFIG"
}

load_oss_config() {
    if [[ -n "$MO_OSS_CONFIG" ]]; then
        if [[ -r "$MO_OSS_CONFIG" ]]; then
            if [[ -z "$OSS_ENDPOINT" ]]; then
                OSS_ENDPOINT="$(read_oss_config_value OSS_ENDPOINT)"
            fi
            if [[ -z "$OSS_ACCESS_KEY_ID" ]]; then
                OSS_ACCESS_KEY_ID="$(read_oss_config_value OSS_ACCESS_KEY_ID)"
            fi
            if [[ -z "$OSS_SECRET_ACCESS_KEY" ]]; then
                OSS_SECRET_ACCESS_KEY="$(read_oss_config_value OSS_SECRET_ACCESS_KEY)"
            fi
            if [[ -z "$OSS_SECRET_ACCESS_KEY" ]]; then
                OSS_SECRET_ACCESS_KEY="$(read_oss_config_value OSS_ACCESS_KEY_SECRET)"
            fi
        elif [[ -z "$OSS_ENDPOINT" || -z "$OSS_ACCESS_KEY_ID" || -z "$OSS_SECRET_ACCESS_KEY" ]]; then
            echo "MO_OSS_CONFIG is not readable and OSS credentials are incomplete: $MO_OSS_CONFIG" >&2
            return 2
        fi
    fi
    if [[ -z "$OSS_REGION" && -n "$OSS_ENDPOINT" ]]; then
        local endpoint_host="${OSS_ENDPOINT#*://}"
        endpoint_host="${endpoint_host%%/*}"
        OSS_REGION="${endpoint_host%%.*}"
    fi

    [[ -n "$OSS_ENDPOINT" ]] || { echo "OSS endpoint is not configured" >&2; return 2; }
    [[ -n "$OSS_REGION" ]] || { echo "OSS region is not configured" >&2; return 2; }
    [[ -n "$OSS_ACCESS_KEY_ID" ]] || { echo "OSS access key ID is not configured" >&2; return 2; }
    [[ -n "$OSS_SECRET_ACCESS_KEY" ]] || { echo "OSS access key secret is not configured" >&2; return 2; }
    [[ "$MO_LOAD_PARALLEL" == "true" || "$MO_LOAD_PARALLEL" == "false" ]] || {
        echo "MO_LOAD_PARALLEL must be true or false, got: $MO_LOAD_PARALLEL" >&2
        return 2
    }
}

parse_oss_uri() {
    local uri="$1"
    [[ "$uri" =~ ^oss://([^/]+)/(.+)$ ]] || {
        echo "OSS source must be an oss://bucket/path URI: $uri" >&2
        return 2
    }
    OSS_BUCKET="${BASH_REMATCH[1]}"
    OSS_FILEPATH="${BASH_REMATCH[2]}"
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
    if [[ "$MO_LOAD_MODE" == "oss" ]]; then
        load_oss_config || return 2
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

    if [[ "$MO_LOAD_MODE" == "oss" ]]; then
        parse_oss_uri "$file" || return
        local endpoint_sql region_sql key_sql secret_sql bucket_sql filepath_sql parallel_sql
        endpoint_sql="$(sql_literal "$OSS_ENDPOINT")"
        region_sql="$(sql_literal "$OSS_REGION")"
        key_sql="$(sql_literal "$OSS_ACCESS_KEY_ID")"
        secret_sql="$(sql_literal "$OSS_SECRET_ACCESS_KEY")"
        bucket_sql="$(sql_literal "$OSS_BUCKET")"
        filepath_sql="$(sql_literal "$OSS_FILEPATH")"
        parallel_sql="$(sql_literal "$MO_LOAD_PARALLEL")"
        # The server reads the compressed NDJSON object directly from OSS. A
        # tab delimiter makes each JSON line one CSV field, while disabling
        # CSV escaping preserves JSON backslashes byte-for-byte.
        mo_sql "LOAD DATA URL S3OPTION {'endpoint'=${endpoint_sql}, 'region'=${region_sql}, 'access_key_id'=${key_sql}, 'secret_access_key'=${secret_sql}, 'bucket'=${bucket_sql}, 'filepath'=${filepath_sql}, 'compression'='gzip', 'format'='csv'} INTO TABLE ${table_sql} FIELDS TERMINATED BY '\\t' ESCAPED BY '' LINES TERMINATED BY '\\n' PARALLEL ${parallel_sql};"
        return
    fi

    if [[ "$MO_LOAD_MODE" == "direct" ]]; then
        # JSONBench is newline-delimited JSON.  A literal tab cannot occur in
        # valid JSON, so it is a safe one-field delimiter.  Disabling CSV
        # escaping is essential: JSON backslashes must be preserved verbatim.
        mo_sql "LOAD DATA INFILE {'filepath'=${file_sql}, 'compression'='gzip', 'format'='csv'} INTO TABLE ${table_sql} FIELDS TERMINATED BY '\\t' ESCAPED BY '' LINES TERMINATED BY '\\n';"
        return
    fi

    if [[ "$MO_LOAD_MODE" != "local" ]]; then
        echo "MO_LOAD_MODE must be direct, local, or oss, got: $MO_LOAD_MODE" >&2
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
