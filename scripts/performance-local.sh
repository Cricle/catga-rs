#!/usr/bin/env bash
# Local podman performance harness for Catga storage benchmarks.
#
# Cross-platform counterpart of scripts/performance.sh (which targets Docker
# Compose on a CI runner): this script drives podman directly, so it runs on
# Linux, macOS, and Windows (WSL or Git Bash) without Docker Desktop.
#
# SQLite needs no service and always runs. Each network backend is measured only
# when its container is started; the benchmark skips backends whose URL is unset.
#
# MySQL and SQL Server are published on non-standard host ports (13306 / 11433)
# because the standard 3306 / 1433 ports are frequently blocked or held by local
# services, which silently breaks port forwarding.
#
# Usage:
#   scripts/performance-local.sh [--backends sqlite,redis,postgres,mysql,mssql|all]
#                                [--relaxed-durability] [--keep-containers] [--in-process]
#
#   --backends              Comma-separated backends to start and measure (default: all).
#   --relaxed-durability    Run MySQL/PostgreSQL with durability relaxed to isolate fsync cost.
#                           Measurements then show the fsync price, NOT production behavior.
#   --keep-containers       Leave containers running after the benchmarks finish.
#   --in-process            Also run the service-free in-process benchmarks.

set -euo pipefail

backends="all"
relaxed_durability=false
keep_containers=false
in_process=false

while (($#)); do
    case "$1" in
        --backends) backends=${2:?missing backends}; shift 2 ;;
        --relaxed-durability) relaxed_durability=true; shift ;;
        --keep-containers) keep_containers=true; shift ;;
        --in-process) in_process=true; shift ;;
        --help|-h) sed -n '2,24p' "$0"; exit 0 ;;
        *) printf 'error: unknown argument %s\n' "$1" >&2; exit 1 ;;
    esac
done

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
container_prefix="catga-perf"

command -v podman >/dev/null || { echo "error: podman must be on PATH" >&2; exit 1; }
command -v cargo >/dev/null || { echo "error: cargo must be on PATH" >&2; exit 1; }

want_backend() {
    [[ "$backends" == "all" ]] && return 0
    local candidate
    IFS=',' read -ra requested <<<"$backends"
    for candidate in "${requested[@]}"; do
        [[ "$candidate" == "$1" ]] && return 0
    done
    return 1
}

started=()

cleanup() {
    if [[ "$keep_containers" == true ]]; then
        [[ ${#started[@]} -gt 0 ]] && echo "containers left running: ${started[*]}"
        return 0
    fi
    local name
    for name in "${started[@]:-}"; do
        [[ -n "$name" ]] && podman rm -f "$name" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

wait_for_port() {
    local name=$1 port=$2 deadline=$((SECONDS + 180))
    while ((SECONDS < deadline)); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            exec 3>&- 3<&-
            return 0
        fi
        sleep 2
    done
    echo "error: service '$name' did not open port $port within 180s" >&2
    exit 1
}

wait_for_health() {
    local name=$1 port=$2; shift 2
    wait_for_port "$name" "$port"
    if (($# == 0)); then return 0; fi
    local deadline=$((SECONDS + 180))
    while ((SECONDS < deadline)); do
        if podman exec "$name" "$@" >/dev/null 2>&1; then return 0; fi
        sleep 2
    done
    echo "error: service '$name' did not become healthy within 180s" >&2
    exit 1
}

start_container() {
    local name=$1 image=$2 port=$3; shift 3
    podman rm -f "$name" >/dev/null 2>&1 || true
    podman run -d --name "$name" "$@" "$image" >/dev/null
    started+=("$name")
}

# Clear every benchmark URL first so the harness never measures a stale service.
unset CATGA_REDIS_URL CATGA_POSTGRES_URL CATGA_MYSQL_URL CATGA_MSSQL_URL || true

if want_backend redis; then
    echo "starting redis..."
    start_container "$container_prefix-redis" docker.io/library/redis:8-alpine 6379 \
        -p 127.0.0.1:6379:6379
    wait_for_health "$container_prefix-redis" 6379 redis-cli ping
    export CATGA_REDIS_URL="redis://127.0.0.1:6379/"
fi

if want_backend postgres; then
    echo "starting postgres..."
    postgres_command=()
    [[ "$relaxed_durability" == true ]] && postgres_command=(-c synchronous_commit=off)
    start_container "$container_prefix-postgres" docker.io/library/postgres:17-alpine 5432 \
        -p 127.0.0.1:5432:5432 \
        -e POSTGRES_DB=catga -e POSTGRES_USER=catga -e POSTGRES_PASSWORD=catga_e2e_password \
        "${postgres_command[@]}"
    wait_for_health "$container_prefix-postgres" 5432 pg_isready -U catga -d catga
    export CATGA_POSTGRES_URL="postgres://catga:catga_e2e_password@127.0.0.1:5432/catga"
fi

if want_backend mysql; then
    echo "starting mysql..."
    mysql_command=()
    [[ "$relaxed_durability" == true ]] && mysql_command=(--innodb-flush-log-at-trx-commit=2 --sync-binlog=0)
    start_container "$container_prefix-mysql" docker.io/library/mysql:8.4 13306 \
        -p 127.0.0.1:13306:3306 \
        -e MYSQL_DATABASE=catga -e MYSQL_USER=catga -e MYSQL_PASSWORD=catga_e2e_password \
        -e MYSQL_ROOT_PASSWORD=catga_root_e2e_password \
        "${mysql_command[@]}"
    wait_for_health "$container_prefix-mysql" 13306 \
        mysqladmin ping -h 127.0.0.1 -u root -pcatga_root_e2e_password
    export CATGA_MYSQL_URL="mysql://catga:catga_e2e_password@127.0.0.1:13306/catga"
fi

if want_backend mssql; then
    echo "starting mssql (azure-sql-edge)..."
    start_container "$container_prefix-mssql" mcr.microsoft.com/azure-sql-edge:latest 11433 \
        -p 127.0.0.1:11433:1433 \
        -e ACCEPT_EULA=1 -e MSSQL_SA_PASSWORD=Catga_e2e_password_2026! -e MSSQL_PID=Developer
    # azure-sql-edge ships no sqlcmd on PATH; a reachable port is the readiness signal.
    wait_for_health "$container_prefix-mssql" 11433
    export CATGA_MSSQL_URL="server=tcp:127.0.0.1,11433;User Id=sa;Password=Catga_e2e_password_2026!;TrustServerCertificate=true;Database=master"
fi

cd "$repository_root"

if [[ "$in_process" == true ]]; then
    echo "running in-process benchmarks (critical path, mediator, flow)..."
    cargo test --release -p catga-tests --all-features \
        --test critical_path_performance --test mediator_performance --test flow_performance \
        -- --ignored --nocapture
fi

echo "running storage benchmark (SQLite always; started services included)..."
cargo test --release -p catga-tests --all-features --test storage_performance -- --ignored --nocapture

echo "performance run complete."
if [[ "$relaxed_durability" == true ]]; then
    echo "note: MySQL/PostgreSQL ran with relaxed durability; numbers show the fsync cost, not production behavior."
fi
