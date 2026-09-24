#!/usr/bin/env bash
# Regression for ODBC connections retained by session-lifetime FdwState values.
#
# One persistent PostgreSQL session runs 150 transactions in simple-query mode,
# with five empty FDW scans per transaction.  The old implementation retains
# one ODBC connection (about two file descriptors) per scan.  Production, with
# RLIMIT_NOFILE=1024, therefore fails around transaction 101.  The local test
# container has a larger limit, so this runner also samples the backend's FD
# count and rejects linear growth even when pgbench itself can finish.
set -uo pipefail

CONTAINER=${CONTAINER:-sybase-fdw-test-postgres}
TRANSACTIONS=${TRANSACTIONS:-150}
MAX_BACKEND_FDS=${MAX_BACKEND_FDS:-128}
APP_NAME=sybase_fdw_connection_lifetime
SCRIPT_PATH=/workspace/wrappers/.ci/sybase-test-env/scripts/pgbench-connection-lifetime.sql
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
RESULTS_DIR=$(dirname "$SCRIPT_DIR")/results
OUT_FILE=$RESULTS_DIR/connection_lifetime.out
ERR_FILE=$RESULTS_DIR/connection_lifetime.err

mkdir -p "$RESULTS_DIR"

if ! docker exec "$CONTAINER" pg_isready -h localhost -U postgres >/dev/null 2>&1; then
    echo "ERR Postgres is not ready. Run 'make up' first."
    exit 2
fi

log_offset=$(docker logs "$CONTAINER" 2>&1 | wc -l | tr -d ' ')
max_fds=0
samples=0

docker exec "$CONTAINER" pgbench \
    --host=localhost \
    --username=postgres \
    --no-vacuum \
    --client=1 \
    --jobs=1 \
    --transactions="$TRANSACTIONS" \
    --protocol=simple \
    --file="$SCRIPT_PATH" \
    "postgresql://localhost/postgres?application_name=$APP_NAME" \
    >"$OUT_FILE" 2>"$ERR_FILE" &
pgbench_job=$!

while kill -0 "$pgbench_job" 2>/dev/null; do
    backend_pid=$(docker exec "$CONTAINER" psql -h localhost -U postgres -XAt \
        -c "SELECT pid FROM pg_stat_activity WHERE application_name = '$APP_NAME' ORDER BY backend_start DESC LIMIT 1" \
        2>/dev/null | tr -d '[:space:]')

    case "$backend_pid" in
        ''|*[!0-9]*) ;;
        *)
            fd_count=$(docker exec "$CONTAINER" sh -c \
                "find /proc/$backend_pid/fd -mindepth 1 -maxdepth 1 -type l 2>/dev/null | wc -l" \
                | tr -d '[:space:]')
            case "$fd_count" in
                ''|*[!0-9]*) ;;
                *)
                    samples=$((samples + 1))
                    if [ "$fd_count" -gt "$max_fds" ]; then
                        max_fds=$fd_count
                    fi
                    ;;
            esac
            ;;
    esac
    sleep 0.05
done

wait "$pgbench_job"
pgbench_ec=$?

crashed=no
if docker logs "$CONTAINER" 2>&1 | awk -v off="$log_offset" 'NR>off' | \
    grep -qE 'terminated by signal 11|server process .* was terminated by signal'; then
    crashed=yes
fi

echo "pgbench exit code: $pgbench_ec"
echo "backend FD samples: $samples"
echo "backend FD peak: $max_fds (limit: $MAX_BACKEND_FDS)"
echo "backend crash detected: $crashed"

if [ "$pgbench_ec" -ne 0 ]; then
    echo "FAIL pgbench did not complete $TRANSACTIONS transactions"
    sed -n '1,20p' "$ERR_FILE"
    exit 1
fi

if [ "$samples" -eq 0 ]; then
    echo "FAIL could not sample the pgbench backend"
    exit 1
fi

if [ "$max_fds" -gt "$MAX_BACKEND_FDS" ]; then
    echo "FAIL backend FD count grew beyond the bounded threshold"
    exit 1
fi

if [ "$crashed" = yes ]; then
    echo "FAIL PostgreSQL backend crashed during the regression"
    exit 1
fi

echo "PASS connection lifetime remained bounded"
