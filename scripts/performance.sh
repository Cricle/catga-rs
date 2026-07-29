#!/usr/bin/env bash
# Runs the release-grade Docker E2E performance suite and writes publishable diagnostics.
set -euo pipefail

profile=full
output_directory=
keep_services=false
validate_only=false
health_timeout_seconds=180
repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose_file="$repository_root/testing/docker/compose.yaml"
compose_project=catga-e2e
services_started=false

usage() {
    cat <<'EOF'
Usage: scripts/performance.sh [options]

  --profile core|sql|full              Docker services and functional E2E profile (default: full)
  --output-directory PATH              Publishable result directory (default: target/performance/<profile>)
  --keep-services                      Leave Docker services and volumes running
  --validate-only                      Validate the performance runner without starting services
  --health-timeout-seconds NUMBER      Per-service health timeout for the functional E2E preflight
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
        --help|-h) usage; exit 0 ;;
        *) die "unknown argument '$1'" ;;
    esac
done

command -v cargo >/dev/null || die 'Cargo must be available on PATH'
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
bash "$repository_root/scripts/e2e.sh" --profile "$profile" --keep-services \
    --health-timeout-seconds "$health_timeout_seconds" \
    --results-path "$output_directory/functional-e2e-results.json" 2>&1 | tee "$output_directory/functional-e2e.log"
functional_exit_code=${PIPESTATUS[0]}
set -e
services_started=true
if (( functional_exit_code != 0 )); then
    die "functional Docker E2E preflight failed with exit code $functional_exit_code"
fi

set_connection_environment
export CATGA_PERFORMANCE_RESULTS="$output_directory/memory-performance.json"
set +e
cargo test --release -p catga-memory --test memory_performance -- --ignored --nocapture \
    2>&1 | tee "$output_directory/memory-performance.log"
memory_exit_code=${PIPESTATUS[0]}
set -e
if (( memory_exit_code != 0 )); then
    die "memory performance test failed with exit code $memory_exit_code"
fi

set +e
cargo test --release -p catga-tests --all-features \
    --test critical_path_performance \
    --test mediator_performance \
    --test flow_performance \
    -- --ignored --nocapture \
    2>&1 | tee "$output_directory/in-process-performance.log"
in_process_exit_code=${PIPESTATUS[0]}
set -e
if (( in_process_exit_code != 0 )); then
    die "in-process performance test failed with exit code $in_process_exit_code"
fi

set +e
cargo test --release -p catga-tests --all-features --test nats_performance -- --ignored --nocapture \
    2>&1 | tee "$output_directory/nats-performance.log"
nats_exit_code=${PIPESTATUS[0]}
set -e
if (( nats_exit_code != 0 )); then
    die "NATS JetStream performance test failed with exit code $nats_exit_code"
fi

extract_throughput() {
    local log_file=$1 metric_name=$2
    local value
    value=$(grep -E "^${metric_name}:" "$log_file" | tail -n1 | sed -E 's/.*_per_second=([0-9.]+).*/\1/')
    [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "could not read $metric_name throughput from $log_file"
    printf '%s\n' "$value"
}

write_in_process_results() {
    local critical mediator flow dsl nats
    critical=$(extract_throughput "$output_directory/in-process-performance.log" critical_application_path)
    mediator=$(extract_throughput "$output_directory/in-process-performance.log" mediator_batch_scheduler_throughput)
    flow=$(extract_throughput "$output_directory/in-process-performance.log" local_flow_execution_throughput)
    dsl=$(extract_throughput "$output_directory/in-process-performance.log" local_dsl_flow_execution_throughput)
    nats=$(extract_throughput "$output_directory/nats-performance.log" nats_jetstream_publish_receive_ack)

    jq -n \
        --argjson critical "$critical" \
        --argjson mediator "$mediator" \
        --argjson flow "$flow" \
        --argjson dsl "$dsl" \
        '{ schema_version: 1, source: "in-process", results: [
            { name: "critical_application_path", operations: 4096, operations_per_second: $critical },
            { name: "mediator_batch_scheduler", operations: 4096, operations_per_second: $mediator },
            { name: "local_flow_execution", operations: 4096, operations_per_second: $flow },
            { name: "local_dsl_flow_execution", operations: 4096, operations_per_second: $dsl }
        ] }' >"$output_directory/in-process-performance.json"
    jq -n \
        --argjson nats "$nats" \
        '{ schema_version: 1, source: "NATS JetStream", results: [
            { name: "nats_jetstream_publish_receive_ack", operations: 1000, operations_per_second: $nats }
        ] }' >"$output_directory/nats-performance.json"
}

write_in_process_results

set +e
export CATGA_PERFORMANCE_RESULTS="$output_directory/performance.json"
cargo test --release -p catga-tests --all-features --test e2e_performance -- --ignored --nocapture \
    2>&1 | tee "$output_directory/e2e-performance.log"
e2e_performance_exit_code=${PIPESTATUS[0]}
set -e
if (( e2e_performance_exit_code != 0 )); then
    die "Docker E2E performance test failed with exit code $e2e_performance_exit_code"
fi

jq -r '
  "Catga Docker E2E performance summary", "",
  (.results[] | "\(.name): operations=\(.operations), elapsed_ns=\(.elapsed_nanoseconds), operations_per_second=\(.operations_per_second)")
' "$output_directory/performance.json" >"$output_directory/summary.txt"
jq -r '
  "", "Functional Docker E2E timings", "",
  (.scenarios[] | "\(.id): succeeded=\(.succeeded), duration_ms=\(.durationMilliseconds)")
' "$output_directory/functional-e2e-results.json" >>"$output_directory/summary.txt"
jq -r -s '
  def cell: if . == null then "—" else tostring end;
  [
    "# Catga performance total table",
    "",
    "| Source | Benchmark | Operations | Throughput (ops/s) | p50 (ns) | p95 (ns) | p99 (ns) | RSS before (bytes) | RSS after (bytes) | RSS peak (bytes) |",
    "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
  ],
  (
    [
      (.[0].results[]? | . + { source: "memory" }),
      (.[1].results[]? | . + { source: "in-process" }),
      (.[2].results[]? | . + { source: "NATS JetStream" }),
      (.[3].results[]? | . + { source: "Docker E2E" })
    ]
    | .[]
    | [
        .source,
        .name,
        (.operations | cell),
        (.operations_per_second | cell),
        (.p50_ns | cell),
        (.p95_ns | cell),
        (.p99_ns | cell),
        (.rss_before_bytes | cell),
        (.rss_after_bytes | cell),
        (.rss_peak_bytes | cell)
      ]
    | "| " + join(" | ") + " |"
  )
' "$output_directory/memory-performance.json" \
    "$output_directory/in-process-performance.json" \
    "$output_directory/nats-performance.json" \
    "$output_directory/performance.json" >"$output_directory/summary.md"
printf '\nIn-process and JetStream benchmark timings\n\n' >>"$output_directory/summary.txt"
grep -hE '^(critical_application_path|mediator_batch_scheduler_throughput|local_(dsl_)?flow_execution_throughput|nats_jetstream_publish_receive_ack):' \
    "$output_directory/in-process-performance.log" \
    "$output_directory/nats-performance.log" >>"$output_directory/summary.txt" || true
printf 'Docker E2E performance suite passed; results: %s\n' "$output_directory"
