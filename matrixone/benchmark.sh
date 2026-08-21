#!/usr/bin/env bash

set -euo pipefail
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <DB_NAME> <RESULT_FILE_RUNTIMES> [ERROR_LOG]" >&2
    exit 2
fi

"$(dirname "$0")/run_queries.sh" "$1" "$2" "${3:-query_errors.log}"
