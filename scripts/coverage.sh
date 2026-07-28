#!/usr/bin/env bash
# Accumulates workspace and Docker E2E coverage, then enforces strict thresholds.
set -euo pipefail

profile=full
keep_services=false
validate_only=false
required_line_coverage=95
required_region_coverage=95
required_e2e_pass_percentage=95
health_timeout_seconds=180
repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
matrix_path="$repository_root/testing/e2e-scenarios.json"
output_directory="$repository_root/target/coverage"

usage() {
    cat <<'EOF'
Usage: scripts/coverage.sh [options]

  --profile core|sql|full
  --keep-services
  --validate-only
  --required-line-coverage NUMBER
  --required-region-coverage NUMBER
  --required-e2e-pass-percentage NUMBER
  --health-timeout-seconds NUMBER
  --matrix-path PATH
  --output-directory PATH
EOF
}

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
run_coverage() { cargo "$@" || die "cargo $* failed"; }

while (($#)); do
    case "$1" in
        --profile) profile=${2:?missing profile}; shift 2 ;;
        --keep-services) keep_services=true; shift ;;
        --validate-only) validate_only=true; shift ;;
        --required-line-coverage) required_line_coverage=${2:?missing coverage}; shift 2 ;;
        --required-region-coverage) required_region_coverage=${2:?missing coverage}; shift 2 ;;
        --required-e2e-pass-percentage) required_e2e_pass_percentage=${2:?missing percentage}; shift 2 ;;
        --health-timeout-seconds) health_timeout_seconds=${2:?missing timeout}; shift 2 ;;
        --matrix-path) matrix_path=${2:?missing matrix path}; shift 2 ;;
        --output-directory) output_directory=${2:?missing output directory}; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) die "unknown argument '$1'" ;;
    esac
done

command -v cargo >/dev/null || die 'Cargo must be available on PATH'
command -v cargo-llvm-cov >/dev/null || die 'cargo-llvm-cov must be available on PATH'
e2e_script="$repository_root/scripts/e2e.sh"
[[ -x "$e2e_script" || -f "$e2e_script" ]] || die "E2E runner does not exist: $e2e_script"

if [[ "$validate_only" == true ]]; then
    bash "$e2e_script" --profile "$profile" --validate-only --matrix-path "$matrix_path"
    printf "Validated strict coverage runner for profile '%s'.\n" "$profile"
    exit 0
fi

mkdir -p "$output_directory"
results_path="$output_directory/e2e-results.json"
rm -f "$results_path"
run_coverage llvm-cov clean --workspace
run_coverage llvm-cov test --workspace --all-features --no-report

e2e_arguments=(--profile "$profile" --coverage --required-pass-percentage "$required_e2e_pass_percentage"
    --health-timeout-seconds "$health_timeout_seconds" --matrix-path "$matrix_path" --results-path "$results_path")
[[ "$keep_services" == true ]] && e2e_arguments+=(--keep-services)
bash "$e2e_script" "${e2e_arguments[@]}"

jq -e --argjson required "$required_e2e_pass_percentage" '
  .schemaVersion == 1 and .succeeded and .passPercentage >= $required and .failedCriticalScenarios == 0
' "$results_path" >/dev/null || die 'E2E result artifact does not satisfy the strict scenario gate'

run_coverage llvm-cov report --lcov --output-path "$output_directory/lcov.info"
run_coverage llvm-cov report --json --output-path "$output_directory/coverage.json"
run_coverage llvm-cov report --html --output-dir "$output_directory/html"
run_coverage llvm-cov report --fail-under-lines "$required_line_coverage" --fail-under-regions "$required_region_coverage"
printf 'Strict coverage gate passed; artifacts: %s\n' "$output_directory"
