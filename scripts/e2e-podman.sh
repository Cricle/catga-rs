#!/usr/bin/env bash
#
# E2E test runner using Podman Compose
#
# Usage:
#   ./e2e-podman.sh              # Run basic E2E tests (nats + redis)
#   ./e2e-podman.sh --profile sql        # Include MySQL and PostgreSQL
#   ./e2e-podman.sh --profile full       # Include all databases
#   ./e2e-podman.sh --stop        # Stop containers only
#   ./e2e-podman.sh --clean       # Stop and remove volumes
#
# Requirements:
#   - podman (v4+ recommended)
#   - podman-compose OR `podman compose` plugin
#
# Environment variables:
#   CATGA_CONTAINER_IMAGE_PREFIX - Override image prefix
#     Set to "registry.access.redhat.com/" for RHEL-based images
#     Set to "" for unqualified images (podman default search)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_DIR="$PROJECT_ROOT/testing/docker"
COMPOSE_FILE="$COMPOSE_DIR/podman-compose.yaml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check for podman
check_podman() {
    if ! command -v podman &> /dev/null; then
        log_error "podman is not installed or not in PATH"
        exit 1
    fi

    # Check podman compose availability
    if podman compose --help &> /dev/null; then
        COMPOSE_CMD="podman compose"
    elif command -v podman-compose &> /dev/null; then
        COMPOSE_CMD="podman-compose"
    else
        log_error "Neither 'podman compose' plugin nor 'podman-compose' is available"
        log_info "Install podman compose plugin: https://podman-desktop.io/"
        exit 1
    fi

    log_info "Using compose command: $COMPOSE_CMD"
}

# Start containers
start_containers() {
    local profile="${1:-}"
    local profile_arg=""

    if [[ -n "$profile" ]]; then
        profile_arg="--profile $profile"
    fi

    log_info "Starting E2E containers (profile: ${profile:-basic})..."
    cd "$COMPOSE_DIR"

    # Export image prefix if set
    if [[ -n "${CATGA_CONTAINER_IMAGE_PREFIX:-}" ]]; then
        export CATGA_CONTAINER_IMAGE_PREFIX
        log_info "Using image prefix: $CATGA_CONTAINER_IMAGE_PREFIX"
    fi

    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d $profile_arg

    log_info "Waiting for containers to be healthy..."
    wait_for_healthy
}

# Wait for containers to be healthy
wait_for_healthy() {
    local max_wait=120
    local elapsed=0
    local interval=2

    while [[ $elapsed -lt $max_wait ]]; do
        local all_healthy=true

        # Check NATS
        if ! podman inspect --format='{{.State.Health.Status}}' catga-e2e-nats-1 2>/dev/null | grep -q "healthy"; then
            all_healthy=false
        fi

        # Check Redis
        if ! podman inspect --format='{{.State.Health.Status}}' catga-e2e-redis-1 2>/dev/null | grep -q "healthy"; then
            all_healthy=false
        fi

        if $all_healthy; then
            log_info "All required containers are healthy!"
            return 0
        fi

        sleep $interval
        elapsed=$((elapsed + interval))
        echo -n "."
    done

    echo ""
    log_warn "Timeout waiting for containers, some may still be starting..."
}

# Stop containers
stop_containers() {
    log_info "Stopping E2E containers..."
    cd "$COMPOSE_DIR"
    $COMPOSE_CMD -f "$COMPOSE_FILE" down
    log_info "Containers stopped."
}

# Clean up containers and volumes
clean_containers() {
    log_info "Stopping E2E containers and removing volumes..."
    cd "$COMPOSE_DIR"
    $COMPOSE_CMD -f "$COMPOSE_FILE" down -v
    log_info "Containers and volumes cleaned."
}

# Run the actual E2E tests
run_tests() {
    log_info "Running E2E tests..."

    cd "$PROJECT_ROOT"

    # Set environment variables for tests
    export CATGA_E2E_CONTAINER_HOST="localhost"

    # Run tests with cargo
    cargo test --test '*' --features e2e 2>&1 || {
        log_error "E2E tests failed"
        return 1
    }

    log_info "E2E tests completed successfully!"
}

# Show status
show_status() {
    log_info "Container status:"
    cd "$COMPOSE_DIR"
    $COMPOSE_CMD -f "$COMPOSE_FILE" ps
}

# Show usage
usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Run E2E tests using Podman Compose.

Options:
    --profile PROFILE    Activate compose profile: 'sql' or 'full'
    --start              Start containers only
    --stop               Stop containers only
    --clean              Stop containers and remove volumes
    --status             Show container status
    --help               Show this help message

Profiles:
    (none)               Basic: NATS + Redis
    sql                  Add MySQL + PostgreSQL
    full                 Add all databases (MySQL, PostgreSQL, MSSQL)

Environment variables:
    CATGA_CONTAINER_IMAGE_PREFIX
                        Override the container image prefix
                        (default: docker.io/library/)

Examples:
    $(basename "$0")                         # Run basic E2E tests
    $(basename "$0") --profile sql           # Include SQL databases
    $(basename "$0") --profile full          # Full test suite
    $(basename "$0") --start                 # Just start containers
    $(basename "$0") --stop                  # Just stop containers
    $(basename "$0") --clean                 # Clean up everything
EOF
}

# Main entry point
main() {
    check_podman

    local profile=""
    local action="test"

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --profile)
                profile="$2"
                shift 2
                ;;
            --start)
                action="start"
                shift
                ;;
            --stop)
                action="stop"
                shift
                ;;
            --clean)
                action="clean"
                shift
                ;;
            --status)
                action="status"
                shift
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    case "$action" in
        start)
            start_containers "$profile"
            ;;
        stop)
            stop_containers
            ;;
        clean)
            clean_containers
            ;;
        status)
            show_status
            ;;
        test)
            start_containers "$profile"
            run_tests
            log_info "To stop containers: $0 --stop"
            ;;
    esac
}

main "$@"
