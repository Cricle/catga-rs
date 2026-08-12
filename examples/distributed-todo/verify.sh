#!/usr/bin/env bash
# Verifies the distributed-kv three-node Raft cluster started by compose.yaml:
# a write POSTed to a follower is forwarded to the leader and replicated to
# all three nodes. Requires Docker on Linux (compose.yaml uses host
# networking). NATS CDC consumption is intentionally out of scope; watch it
# manually with: distributed-kv --subscribe --nats-url nats://127.0.0.1:4222
set -euo pipefail

readonly example_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly compose_file="${example_directory}/compose.yaml"
readonly project_name="catga-distributed-kv-verify-${RANDOM}"
readonly ports=(9100 9101 9102)

compose() {
  docker compose --project-name "${project_name}" --file "${compose_file}" "$@"
}

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  compose logs >&2 || true
  exit 1
}

cleanup() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

compose up --build --detach

# 1. Every node answers /status.
for port in "${ports[@]}"; do
  ready=false
  for attempt in {1..90}; do
    if curl --fail --silent "http://localhost:${port}/status" >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  [[ "${ready}" == "true" ]] || fail "node on port ${port} did not answer /status"
done

# 2. Wait for a leader election and pick a follower port.
leader_port=""
for attempt in {1..90}; do
  for port in "${ports[@]}"; do
    status="$(curl --fail --silent "http://localhost:${port}/status" 2>/dev/null || true)"
    if grep --fixed-strings --quiet '"is_leader":true' <<<"${status}"; then
      leader_port="${port}"
      break
    fi
  done
  [[ -n "${leader_port}" ]] && break
  sleep 1
done
[[ -n "${leader_port}" ]] || fail "no leader was elected across ${ports[*]}"

follower_port=""
for port in "${ports[@]}"; do
  if [[ "${port}" != "${leader_port}" ]]; then
    follower_port="${port}"
    break
  fi
done
printf 'leader on port %s; writing through follower on port %s\n' "${leader_port}" "${follower_port}"

# 3. Write through the follower; ForwardToLeaderBehavior must relay it.
key="verify-key"
value="verify-value-${RANDOM}"
if ! curl --fail --silent --show-error \
  --header "content-type: application/json" \
  --data "{\"key\":\"${key}\",\"value\":\"${value}\"}" \
  "http://localhost:${follower_port}/kv" >/dev/null; then
  fail "POST /kv through follower on port ${follower_port} failed"
fi

# 4. The write must be readable from every node (Raft replication).
for port in "${ports[@]}"; do
  replicated=false
  for attempt in {1..30}; do
    body="$(curl --fail --silent "http://localhost:${port}/kv/${key}" 2>/dev/null || true)"
    if grep --fixed-strings --quiet "\"value\":\"${value}\"" <<<"${body}"; then
      replicated=true
      break
    fi
    sleep 1
  done
  [[ "${replicated}" == "true" ]] || fail "key '${key}' was not replicated to the node on port ${port}"
done

printf 'PASS: write through follower %s (leader %s) replicated to ports %s\n' \
  "${follower_port}" "${leader_port}" "${ports[*]}"

exit 0
