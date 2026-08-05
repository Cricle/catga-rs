#!/usr/bin/env bash
# Runs the release-grade Docker E2E performance suite and writes publishable diagnostics.
# Also supports running criterion microbenchmarks for quick performance feedback.
set -euo pipefail

profile=full
output_directory=
keep_services=false
validate_only=false
health_timeout_seconds=180
repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose_file="$repository_root/testing/docker/compose.yaml"
# Functional E2E uses its own `catga-e2e` project. Benchmarks intentionally use a separate
# project so E2E migrations, writes, and background checkpoints cannot contaminate measurements.
compose_project=catga-performance
services_started=false

# Criterion benchmark options
run_criterion=false
skip_transport=false
criterion_output_dir=

usage() {
    cat <<'EOF'
Usage: scripts/performance.sh [options]

  Docker E2E Options:
  --profile core|sql|full              Docker services and functional E2E profile (default: full)
  --output-directory PATH              Publishable result directory (default: target/performance/<profile>)
  --keep-services                      Leave Docker services and volumes running
  --validate-only                      Validate the performance runner without starting services
  --health-timeout-seconds NUMBER      Per-service health timeout for the functional E2E preflight

  Criterion Benchmark Options:
  --criterion                          Run criterion microbenchmarks (no Docker required)
  --skip-transport                     Skip transport benchmarks (redis, robustmq, nats)
  --criterion-output PATH              Output directory for criterion results (default: target/criterion)

  Examples:
  ./scripts/performance.sh --criterion --skip-transport
  ./scripts/performance.sh --profile full
  ./scripts/performance.sh --criterion --criterion-output target/my-benchmarks
EOF
}

die() { printf 'error: %s\n' "$*" >&2; exit 1; }

profile_rank() {
    case "$1" in
        core) printf '0\n' ;;
        sql) printf '1\n' ;;
        full) printf '2\n' ;;
        *) die "unsupported performance profile '$1'" ;;
    esac
}

configure_profile() {
    case "$profile" in
        core) compose_profile_args=() ;;
        sql) compose_profile_args=(--profile sql) ;;
        full) compose_profile_args=(--profile sql --profile full) ;;
        *) die "unsupported performance profile '$profile'" ;;
    esac
}

container_id() {
    local service=$1 id
    id=$(docker compose --project-name "$compose_project" -f "$compose_file" \
        "${compose_profile_args[@]}" ps --all --quiet "$service" | head -n1)
    [[ -n "$id" ]] || die "Docker Compose did not create an E2E container for '$service'"
    printf '%s\n' "$id"
}

published_port() {
    local service=$1 container_port=$2 address
    address=$(docker port "$(container_id "$service")" "${container_port}/tcp" | head -n1)
    [[ "$address" =~ :([0-9]+)$ ]] || die "E2E service '$service' does not publish TCP port $container_port"
    printf '%s\n' "${BASH_REMATCH[1]}"
}

set_connection_environment() {
    export CATGA_REQUIRE_EXTERNAL_SERVICES=1
    export CATGA_NATS_URL="nats://127.0.0.1:$(published_port nats 4222)"
    export CATGA_REDIS_URL="redis://127.0.0.1:$(published_port redis 6379)"
    if (( $(profile_rank "$profile") >= $(profile_rank sql) )); then
        export CATGA_MYSQL_URL="mysql://catga:catga_e2e_password@127.0.0.1:$(published_port mysql 3306)/catga"
        export CATGA_POSTGRES_URL="postgres://catga:catga_e2e_password@127.0.0.1:$(published_port postgres 5432)/catga"
    fi
    if (( $(profile_rank "$profile") >= $(profile_rank full) )); then
        export CATGA_MSSQL_URL="server=tcp:127.0.0.1,$(published_port mssql 1433);User Id=sa;Password=Catga_e2e_password_2026!;TrustServerCertificate=true;Database=master"
    fi
}

wait_for_health() {
    local service=$1 id health deadline
    id=$(container_id "$service")
    deadline=$((SECONDS + health_timeout_seconds))
    while (( SECONDS < deadline )); do
        health=$(docker inspect --format '{{.State.Health.Status}}' "$id") ||
            die "could not inspect benchmark service '$service'"
        case "$health" in
            healthy) return 0 ;;
            unhealthy) die "benchmark service '$service' reported unhealthy" ;;
        esac
        sleep 1
    done
    die "benchmark service '$service' did not become healthy within ${health_timeout_seconds}s"
}

start_benchmark_services() {
    docker compose --project-name "$compose_project" -f "$compose_file" \
        "${compose_profile_args[@]}" up --detach
    services_started=true
    for service in nats redis; do wait_for_health "$service"; done
    if (( $(profile_rank "$profile") >= $(profile_rank sql) )); then
        for service in mysql postgres; do wait_for_health "$service"; done
    fi
    if (( $(profile_rank "$profile") >= $(profile_rank full) )); then wait_for_health mssql; fi
    set_connection_environment
}

copy_diagnostics() {
    docker compose --project-name "$compose_project" -f "$compose_file" \
        "${compose_profile_args[@]}" logs --no-color >"$output_directory/docker-compose.log" 2>&1 || true
    if [[ -d "$repository_root/target/e2e-logs" ]]; then
        rm -rf "$output_directory/e2e-logs"
        cp -R "$repository_root/target/e2e-logs" "$output_directory/e2e-logs"
    fi
}

redact_publishable_output() {
    local path
    while IFS= read -r -d '' path; do
        sed -E -i \
            -e 's#(://[^:/[:space:]]+):[^@[:space:]]+@#\1:[REDACTED]@#g' \
            -e 's/(Password|password)=[^;[:space:]]+/\1=[REDACTED]/g' \
            -e 's/catga_(root_)?e2e_password/CATGA_PASSWORD_REDACTED/g' \
            "$path"
    done < <(find "$output_directory" -type f -print0)
}

cleanup() {
    copy_diagnostics
    redact_publishable_output
    if [[ "$services_started" == true && "$keep_services" == false ]]; then
        docker compose --project-name "$compose_project" -f "$compose_file" \
            "${compose_profile_args[@]}" down --volumes --remove-orphans || true
    fi
}

while (($#)); do
    case "$1" in
        --profile) profile=${2:?missing profile}; shift 2 ;;
        --output-directory) output_directory=${2:?missing output directory}; shift 2 ;;
        --keep-services) keep_services=true; shift ;;
        --validate-only) validate_only=true; shift ;;
        --health-timeout-seconds) health_timeout_seconds=${2:?missing timeout}; shift 2 ;;
        --criterion) run_criterion=true; shift ;;
        --skip-transport) skip_transport=true; shift ;;
        --criterion-output) criterion_output_dir=${2:?missing output directory}; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) die "unknown argument '$1'" ;;
    esac
done

command -v cargo >/dev/null || die 'Cargo must be available on PATH'

# If running criterion benchmarks only, don't require Docker
if [[ "$run_criterion" == true ]]; then
    command -v jq >/dev/null || die 'jq must be available on PATH'
    criterion_output_dir=${criterion_output_dir:-"$repository_root/target/criterion"}
    criterion_output_dir=$(realpath -m "$criterion_output_dir")
    mkdir -p "$criterion_output_dir"

    echo "=== Criterion Microbenchmarks ==="
    echo "Output directory: $criterion_output_dir"

    # Run catga-core benchmarks (mediator, registry, handler, flow)
    # Note: order_checkout_benchmark requires nightly, so we run specific benchmarks
    echo ""
    echo "Running catga-core benchmarks..."
    echo "  - mediator_throughput"
    cargo bench -p catga-core --bench mediator_throughput -- --noplot 2>&1 | tee "$criterion_output_dir/mediator_throughput.log"
    echo "  - registry"
    cargo bench -p catga-core --bench registry -- --noplot 2>&1 | tee "$criterion_output_dir/registry.log"
    echo "  - handler_dispatch"
    cargo bench -p catga-core --bench handler_dispatch -- --noplot 2>&1 | tee "$criterion_output_dir/handler_dispatch.log"
    echo "  - macro_expansion"
    cargo bench -p catga-core --bench macro_expansion -- --noplot 2>&1 | tee "$criterion_output_dir/macro_expansion.log"
    echo "  - flow_throughput"
    cargo bench -p catga-core --bench flow_throughput -- --noplot 2>&1 | tee "$criterion_output_dir/flow_throughput.log"

    # Copy criterion results
    cp -r "$repository_root/target/criterion"/* "$criterion_output_dir/" 2>/dev/null || true

    # Run transport benchmarks if not skipped and services are available
    if [[ "$skip_transport" == false ]]; then
        echo ""
        echo "Checking transport benchmark availability..."

        # Check Redis availability
        if [[ -n "${CATGA_REDIS_URL:-}" ]]; then
            echo "Running Redis benchmarks (CATGA_REDIS_URL is set)..."
            cargo bench -p catga-redis -- --noplot 2>&1 | tee "$criterion_output_dir/catga-redis.log"
        else
            echo "Skipping Redis benchmarks (CATGA_REDIS_URL not set)"
        fi

        # Check NATS availability
        if [[ -n "${CATGA_NATS_URL:-}" ]]; then
            echo "Running NATS benchmarks (CATGA_NATS_URL is set)..."
            cargo bench -p catga-nats -- --noplot 2>&1 | tee "$criterion_output_dir/catga-nats.log"
        else
            echo "Skipping NATS benchmarks (CATGA_NATS_URL not set)"
        fi

        # Check RobustMQ availability
        if [[ -n "${CATGA_ROBUSTMQ_URL:-}" ]]; then
            echo "Running RobustMQ benchmarks (CATGA_ROBUSTMQ_URL is set)..."
            cargo bench -p catga-robustmq -- --noplot 2>&1 | tee "$criterion_output_dir/catga-robustmq.log"
        else
            echo "Skipping RobustMQ benchmarks (CATGA_ROBUSTMQ_URL not set)"
        fi
    else
        echo ""
        echo "Skipping all transport benchmarks (--skip-transport set)"
    fi

    # Generate summary report
    echo ""
    echo "=== Benchmark Summary ==="
    if command -v cargo-benchcmp >/dev/null 2>&1; then
        echo "cargo-benchcmp available for comparison"
    fi

    echo ""
    echo "Results saved to: $criterion_output_dir"
    echo ""
    echo "Core benchmarks completed:"
    echo "  - mediator_throughput (catga-core)"
    echo "  - registry (catga-core)"
    echo "  - handler_dispatch (catga-core)"
    echo "  - macro_expansion (catga-core)"
    echo "  - flow_throughput (catga-core)"
    [[ "$skip_transport" == false ]] && echo "  - redis_throughput (catga-redis, if CATGA_REDIS_URL set)"
    [[ "$skip_transport" == false ]] && echo "  - nats_throughput (catga-nats, if CATGA_NATS_URL set)"
    [[ "$skip_transport" == false ]] && echo "  - robustmq_throughput (catga-robustmq, if CATGA_ROBUSTMQ_URL set)"

    echo ""
    echo "To view HTML reports, run:"
    echo "  cargo bench -p catga-core"
    echo "  # Then open target/criterion/report/index.html in a browser"

    exit 0
fi

# Continue with Docker E2E benchmarks
command -v docker >/dev/null || die 'Docker must be available on PATH'
command -v jq >/dev/null || die 'jq must be available on PATH'
[[ -f "$compose_file" ]] || die "Docker Compose file does not exist: $compose_file"
docker compose version >/dev/null || die 'Docker Compose v2 must be available through docker compose'
profile_rank "$profile" >/dev/null
configure_profile
output_directory=${output_directory:-"$repository_root/target/performance/$profile"}
output_directory=$(realpath -m "$output_directory")
case "$output_directory" in
    "$repository_root"/target/*) ;;
    *) die '--output-directory must be inside the repository target directory' ;;
esac

if [[ "$validate_only" == true ]]; then
    bash "$repository_root/scripts/e2e.sh" --profile "$profile" --validate-only
    printf "Validated Docker E2E performance runner for profile '%s'.\n" "$profile"
    exit 0
fi

mkdir -p "$output_directory"
trap cleanup EXIT

set +e
bash "$repository_root/scripts/e2e.sh" --profile "$profile" \
    --health-timeout-seconds "$health_timeout_seconds" \
    --results-path "$output_directory/functional-e2e-results.json" 2>&1 | tee "$output_directory/functional-e2e.log"
functional_exit_code=${PIPESTATUS[0]}
set -e
if (( functional_exit_code != 0 )); then
    die "functional Docker E2E preflight failed with exit code $functional_exit_code"
fi

start_benchmark_services
run_benchmark() {
    local name=$1 report=$2
    shift 2
    rm -f "$output_directory/$report"
    export CATGA_PERFORMANCE_RESULTS="$output_directory/$report"
    set +e
    "$@" 2>&1 | tee "$output_directory/$name.log"
    local benchmark_exit_code=${PIPESTATUS[0]}
    set -e
    if (( benchmark_exit_code != 0 )); then
        die "$name failed with exit code $benchmark_exit_code"
    fi
    [[ -s "$output_directory/$report" ]] || die "$name did not produce $report"
}

run_benchmark memory-performance memory-performance.json \
    cargo test --release -p catga-memory --test memory_performance -- --ignored --nocapture
run_benchmark critical-performance critical-performance.json \
    cargo test --release -p catga-tests --all-features --test critical_path_performance -- --ignored --nocapture
run_benchmark mediator-performance mediator-performance.json \
    cargo test --release -p catga-tests --all-features --test mediator_performance -- --ignored --nocapture
run_benchmark mediator-pure-performance mediator-pure-performance.json \
    cargo test --release -p catga-tests --all-features --test mediator_pure_throughput -- --ignored --nocapture --test-threads=1
run_benchmark typed-mediator-performance typed-mediator-performance.json \
    cargo test --release -p catga-tests --all-features --test typed_mediator_bench -- --ignored --nocapture --test-threads=1
run_benchmark flow-performance flow-performance.json \
    cargo test --release -p catga-tests --all-features --test flow_performance -- --ignored --nocapture
run_benchmark nats-performance nats-performance.json \
    cargo test --release -p catga-tests --all-features --test nats_performance -- --ignored --nocapture
# Storage backends: SQLite, MySQL, PostgreSQL, SQL Server, and Redis.
run_benchmark storage-performance storage-performance.json \
    cargo test --release -p catga-tests --all-features --test storage_performance -- --ignored --nocapture
run_benchmark e2e-performance performance.json \
    cargo test --release -p catga-tests --all-features --test e2e_performance -- --ignored --nocapture

docker compose --project-name "$compose_project" -f "$compose_file" \
    "${compose_profile_args[@]}" ps --quiet \
    | xargs -r docker stats --no-stream --format '{{json .}}' \
    | jq -s '{ schema_version: 1, source: "Docker container statistics", containers: . }' \
    >"$output_directory/container-memory.json"

jq -r -s '
  def cell: if . == null then "—" else tostring end;
  [
    "# Catga performance total table",
    "",
    "All latency percentiles are nearest-rank. RSS is the benchmark process only; container resource data is in `container-memory.json`.",
    "",
    "| Source | Benchmark | Payload (bytes) | Operations | Throughput (ops/s) | Latency scope | p50 (ns) | p95 (ns) | p99 (ns) | RSS before (bytes) | RSS after (bytes) | RSS peak (bytes) |",
    "| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
  ][],
  (
    [ .[] | .source as $source | .results[]? | . + { source: $source } ]
    | .[]
    | [
        .source,
        .name,
        (.payload_bytes | cell),
        (.operations | cell),
        (.operations_per_second | cell),
        (.latency_scope | cell),
        (.p50_ns | cell),
        (.p95_ns | cell),
        (.p99_ns | cell),
        (.rss_before_bytes | cell),
        (.rss_after_bytes | cell),
        (.rss_peak_bytes | cell)
      ]
    | "| " + join(" | ") + " |"
  ),
  "",
  "## FlowStore lifecycle phase latency",
  "",
  "| Source | Benchmark | Phase | p50 (ns) | p95 (ns) | p99 (ns) |",
  "| --- | --- | --- | ---: | ---: | ---: |",
  (
    [ .[] | .source as $source | .results[]? | .name as $name | .phase_latencies[]? | . + { source: $source, name: $name } ]
    | .[]
    | [ .source, .name, .phase, (.p50_ns | cell), (.p95_ns | cell), (.p99_ns | cell) ]
    | "| " + join(" | ") + " |"
  ),
  "",
  "## Database-native counter deltas",
  "",
  "These counters are sampled immediately before and after the isolated storage benchmark. A negative delta indicates a counter reset.",
  "",
  "| Backend | Counter | Unit | Before | After | Delta |",
  "| --- | --- | --- | ---: | ---: | ---: |",
  (
    [ .[] | .database_metric_deltas[]? ]
    | .[]
    | [ .backend, .metric, .unit, (.before | cell), (.after | cell), (.delta | cell) ]
    | "| " + join(" | ") + " |"
  )
' "$output_directory/memory-performance.json" \
    "$output_directory/critical-performance.json" \
    "$output_directory/mediator-performance.json" \
    "$output_directory/mediator-pure-performance.json" \
    "$output_directory/typed-mediator-performance.json" \
    "$output_directory/flow-performance.json" \
    "$output_directory/nats-performance.json" \
    "$output_directory/storage-performance.json" \
    "$output_directory/performance.json" >"$output_directory/summary.md"

jq -r -s '
  "Catga performance measurements", "",
  ([.[] | .source as $source | .results[]? | "\($source) / \(.name): operations=\(.operations), elapsed_ns=\(.elapsed_nanoseconds), operations_per_second=\(.operations_per_second)"] | .[]),
  "", "FlowStore lifecycle phase latency",
  ([.[] | .source as $source | .results[]? | .name as $name | .phase_latencies[]? | "\($source) / \($name) / \(.phase): p50_ns=\(.p50_ns), p95_ns=\(.p95_ns), p99_ns=\(.p99_ns)"] | .[]),
  "", "Database-native counter deltas",
  ([.[] | .database_metric_deltas[]? | "\(.backend) / \(.metric): unit=\(.unit), before=\(.before), after=\(.after), delta=\(.delta)"] | .[])
' "$output_directory/memory-performance.json" \
    "$output_directory/critical-performance.json" \
    "$output_directory/mediator-performance.json" \
    "$output_directory/mediator-pure-performance.json" \
    "$output_directory/typed-mediator-performance.json" \
    "$output_directory/flow-performance.json" \
    "$output_directory/nats-performance.json" \
    "$output_directory/storage-performance.json" \
    "$output_directory/performance.json" >"$output_directory/summary.txt"
jq -r '
  "", "Functional Docker E2E timings", "",
  (.scenarios[] | "\(.id): succeeded=\(.succeeded), duration_ms=\(.durationMilliseconds)")
' "$output_directory/functional-e2e-results.json" >>"$output_directory/summary.txt"
printf 'Docker E2E performance suite passed; results: %s\n' "$output_directory"
