#!/usr/bin/env bash
# Runs the declared Docker-backed Catga end-to-end scenario matrix.
set -euo pipefail

profile=full
coverage=false
keep_services=false
validate_only=false
required_pass_percentage=95
health_timeout_seconds=180
repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
matrix_path="$repository_root/testing/e2e-scenarios.json"
results_path="$repository_root/target/e2e-results.json"
compose_file="$repository_root/testing/docker/compose.yaml"
compose_project=catga-e2e
services_started=false

usage() {
    cat <<'EOF'
Usage: scripts/e2e.sh [options]

  --profile core|sql|full              Services and scenarios to run (default: full)
  --coverage                           Accumulate coverage with cargo llvm-cov
  --keep-services                      Leave Docker services and volumes running
  --validate-only                      Validate Docker Compose and the scenario matrix
  --required-pass-percentage NUMBER    Required scenario pass rate (default: 95)
  --health-timeout-seconds NUMBER      Per-service health timeout (default: 180)
  --matrix-path PATH                   Scenario matrix path
  --results-path PATH                  Result JSON path
EOF
}

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

profile_rank() {
    case "$1" in
        core) printf '0\n' ;;
        sql) printf '1\n' ;;
        full) printf '2\n' ;;
        *) die "unsupported E2E profile '$1'" ;;
    esac
}

compose_profile_args=()
configure_profile() {
    case "$profile" in
        core) compose_profile_args=() ;;
        sql) compose_profile_args=(--profile sql) ;;
        full) compose_profile_args=(--profile sql --profile full) ;;
        *) die "unsupported E2E profile '$profile'" ;;
    esac
}

container_id() {
    local service=$1 id
    id=$(docker compose --project-name "$compose_project" -f "$compose_file" \
        "${compose_profile_args[@]}" ps --all --quiet "$service" | head -n1)
    [[ -n "$id" ]] || die "Docker Compose did not create an E2E container for '$service'"
    printf '%s\n' "$id"
}

wait_for_health() {
    local service=$1 id health deadline
    id=$(container_id "$service")
    deadline=$((SECONDS + health_timeout_seconds))
    while (( SECONDS < deadline )); do
        health=$(docker inspect --format '{{.State.Health.Status}}' "$id") ||
            die "could not inspect E2E service '$service'"
        case "$health" in
            healthy) return 0 ;;
            unhealthy) die "E2E service '$service' reported unhealthy" ;;
        esac
        sleep 1
    done
    die "E2E service '$service' did not become healthy within ${health_timeout_seconds}s"
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

cleanup() {
    if [[ "$services_started" == true && "$keep_services" == false ]]; then
        docker compose --project-name "$compose_project" -f "$compose_file" \
            "${compose_profile_args[@]}" down --volumes --remove-orphans || true
    fi
}

while (($#)); do
    case "$1" in
        --profile) profile=${2:?missing profile}; shift 2 ;;
        --coverage) coverage=true; shift ;;
        --keep-services) keep_services=true; shift ;;
        --validate-only) validate_only=true; shift ;;
        --required-pass-percentage) required_pass_percentage=${2:?missing percentage}; shift 2 ;;
        --health-timeout-seconds) health_timeout_seconds=${2:?missing timeout}; shift 2 ;;
        --matrix-path) matrix_path=${2:?missing matrix path}; shift 2 ;;
        --results-path) results_path=${2:?missing result path}; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) die "unknown argument '$1'" ;;
    esac
done

command -v docker >/dev/null || die 'Docker must be available on PATH'
command -v cargo >/dev/null || die 'Cargo must be available on PATH'
command -v jq >/dev/null || die 'jq must be available on PATH'
[[ -f "$compose_file" ]] || die "Docker Compose file does not exist: $compose_file"
[[ -f "$matrix_path" ]] || die "E2E scenario matrix does not exist: $matrix_path"
docker compose version >/dev/null || die "Docker Compose v2 must be available through 'docker compose'"
configure_profile

jq -e '
  .schemaVersion == 1 and (.scenarios | type == "array" and length > 0) and
  all(.scenarios[]; (.id | type == "string" and length > 0) and
    (.package | type == "string" and length > 0) and
    (.target | type == "string" and length > 0) and
    (.profile == "core" or .profile == "sql" or .profile == "full") and
    (.critical | type == "boolean") and (.testArguments | type == "array")) and
  (([.scenarios[].id] | length) == ([.scenarios[].id] | unique | length))
' "$matrix_path" >/dev/null || die 'invalid E2E scenario matrix'

selected_count=$(jq --argjson maximum "$(profile_rank "$profile")" '
  [.scenarios[] | select((if .profile == "core" then 0 elif .profile == "sql" then 1 else 2 end) <= $maximum)] | length
' "$matrix_path")
(( selected_count > 0 )) || die "E2E profile '$profile' selects no scenarios"

if [[ "$validate_only" == true ]]; then
    docker compose --project-name "$compose_project" -f "$compose_file" \
        "${compose_profile_args[@]}" config --quiet
    printf "Validated %s E2E scenarios for profile '%s'.\n" "$selected_count" "$profile"
    exit 0
fi

trap cleanup EXIT
docker compose --project-name "$compose_project" -f "$compose_file" \
    "${compose_profile_args[@]}" up --detach
services_started=true
for service in nats redis; do wait_for_health "$service"; done
if (( $(profile_rank "$profile") >= $(profile_rank sql) )); then
    for service in mysql postgres; do wait_for_health "$service"; done
fi
if (( $(profile_rank "$profile") >= $(profile_rank full) )); then wait_for_health mssql; fi
set_connection_environment

log_directory="$repository_root/target/e2e-logs"
mkdir -p "$log_directory" "$(dirname "$results_path")"
result_lines=$(mktemp)
trap 'rm -f "$result_lines"; cleanup' EXIT

group_number=0
while IFS= read -r group; do
    ((group_number += 1))
    group_id=$(printf 'e2e-group-%03d' "$group_number")
    package=$(jq -r '.package' <<<"$group")
    target=$(jq -r '.target' <<<"$group")
    mapfile -t test_arguments < <(jq -r '.testArguments[]' <<<"$group")
    cargo_arguments=(test -p "$package" --all-features --test "$target")
    if [[ "$coverage" == true ]]; then cargo_arguments=(llvm-cov "${cargo_arguments[@]}" --no-clean); fi
    ((${#test_arguments[@]})) && cargo_arguments+=(-- "${test_arguments[@]}")
    log_path="$log_directory/${group_id}-${package}-${target}.log"
    started=$SECONDS
    set +e
    cargo "${cargo_arguments[@]}" 2>&1 | tee "$log_path"
    exit_code=${PIPESTATUS[0]}
    set -e
    duration="$(( (SECONDS - started) * 1000 ))"

    while IFS= read -r scenario; do
        id=$(jq -r '.id' <<<"$scenario")
        critical=$(jq -r '.critical' <<<"$scenario")
        jq -n --arg id "$id" --arg package "$package" --arg target "$target" \
            --arg group_id "$group_id" --arg log_path "$log_path" \
            --argjson critical "$critical" --argjson exit_code "$exit_code" --argjson duration "$duration" \
            '{id:$id, critical:$critical, package:$package, target:$target,
              succeeded:($exit_code == 0), exitCode:$exit_code,
              durationMilliseconds:$duration, executionGroup:$group_id, logPath:$log_path}' >>"$result_lines"
    done < <(jq -c '.scenarios[]' <<<"$group")
done < <(jq -c --argjson maximum "$(profile_rank "$profile")" '
  [.scenarios[] | select((if .profile == "core" then 0 elif .profile == "sql" then 1 else 2 end) <= $maximum)]
  | group_by([.package, .target, .testArguments])
  | .[]
  | {package: .[0].package, target: .[0].target, testArguments: .[0].testArguments, scenarios: .}
' "$matrix_path")

jq -s --arg profile "$profile" --argjson required "$required_pass_percentage" '
  {schemaVersion: 1, profile:$profile, requiredPassPercentage:$required,
   declaredScenarios:length, passedScenarios:([.[] | select(.succeeded)] | length),
   failedCriticalScenarios:([.[] | select(.critical and (.succeeded | not))] | length), scenarios:.} |
  .passPercentage = (if .declaredScenarios == 0 then 0 else ((100 * .passedScenarios / .declaredScenarios * 100 | round) / 100) end) |
  .succeeded = (.passPercentage >= .requiredPassPercentage and .failedCriticalScenarios == 0)
' "$result_lines" >"$results_path"

if [[ $(jq -r '.succeeded' "$results_path") != true ]]; then
    cat "$results_path" >&2
    die 'E2E scenario gate failed'
fi
printf 'E2E gate passed: %s/%s scenarios passed (%s%%).\n' \
    "$(jq -r '.passedScenarios' "$results_path")" \
    "$(jq -r '.declaredScenarios' "$results_path")" \
    "$(jq -r '.passPercentage' "$results_path")"
