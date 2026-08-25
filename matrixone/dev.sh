#!/usr/bin/env bash

set -euo pipefail

MATRIXONE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CHOICE="${1:-1}"
DATA_SOURCE="${2:-${MO_DEV_OSS_PREFIX:-${OSS_PREFIX:-oss://mo-bench/bluesky}}}"
ARTIFACT_DIRECTORY="${MO_DEV_ARTIFACT_DIRECTORY:-$MATRIXONE_DIR/dev-results}"
SUCCESS_LOG="${MO_DEV_SUCCESS_LOG:-$ARTIFACT_DIRECTORY/success.log}"
ERROR_LOG="${MO_DEV_ERROR_LOG:-$ARTIFACT_DIRECTORY/error.log}"
OUTPUT_PREFIX="${MO_DEV_OUTPUT_PREFIX:-_matrixone_dev_$(date +%Y%m%d)}"

case "$CHOICE" in
    1) REQUIRED_FILES=1 ;;
    2) REQUIRED_FILES=10 ;;
    3) REQUIRED_FILES=100 ;;
    4) REQUIRED_FILES=1000 ;;
    5) REQUIRED_FILES=1000 ;;
    *)
        echo "choice must be 1, 2, 3, 4, or 5" >&2
        exit 2
        ;;
esac

[[ "$DATA_SOURCE" =~ ^oss://[^/]+/.+ ]] || {
    echo "dev data source must be an oss://bucket/path URI: $DATA_SOURCE" >&2
    exit 2
}

# The password is intentionally supplied through the environment and never
# embedded in this repository. MO_PASSWORD is also accepted for consistency
# with the regular MatrixOne adapter scripts.
if [[ -z "${MO_PASSWORD:-}" && -n "${MO_DEV_PASSWORD:-}" ]]; then
    export MO_PASSWORD="$MO_DEV_PASSWORD"
fi
if [[ -z "${MO_PASSWORD:-}" && -z "${MYSQL_PWD:-}" ]]; then
    echo "set MO_DEV_PASSWORD (or MO_PASSWORD/MYSQL_PWD) before running the dev test" >&2
    exit 2
fi

# These defaults identify the shared dev endpoint, while the secret remains
# caller-provided. They can all be overridden for another MatrixOne tenant.
export MO_HOST="${MO_DEV_HOST:-freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech}"
export MO_PORT="${MO_DEV_PORT:-6001}"
export MO_USER="${MO_DEV_USER:-019d6bb9-afc7-7984-8d08-87c686a8a9e4:admin:accountadmin}"
export MO_LOAD_MODE=oss
export MO_DB_NAME_PREFIX="${MO_DB_NAME_PREFIX:-jsonbench_dev_bluesky}"
export MO_REMOTE_FILE_COUNT="$REQUIRED_FILES"
# Continue with the successfully loaded subset when an individual OSS object
# is unavailable; load_data.sh records every failed URI in error.log.
export MO_CONTINUE_ON_LOAD_ERROR=1
# A remote dev host cannot safely drop the local machine's page cache. Set
# MO_DROP_CACHES=1 only when the runner is deliberately configured for it.
export MO_DROP_CACHES="${MO_DROP_CACHES:-0}"

mkdir -p "$ARTIFACT_DIRECTORY" "$(dirname "$SUCCESS_LOG")" "$(dirname "$ERROR_LOG")"

echo "Running MatrixOne dev JSONBench (choice=$CHOICE, files=$REQUIRED_FILES)"
echo "Endpoint: ${MO_HOST}:${MO_PORT}; database prefix: ${MO_DB_NAME_PREFIX}"
echo "OSS source: ${DATA_SOURCE}; loading directly from OSS (no local staging)"

pushd "$ARTIFACT_DIRECTORY" >/dev/null
"$MATRIXONE_DIR/main.sh" "$CHOICE" "$DATA_SOURCE" "$SUCCESS_LOG" "$ERROR_LOG" "$OUTPUT_PREFIX"
popd >/dev/null

echo "MatrixOne dev JSONBench completed"
