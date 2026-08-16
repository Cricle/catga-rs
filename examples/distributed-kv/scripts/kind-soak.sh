#!/usr/bin/env bash
# distributed-kv kind soak test (raft).
#
#   examples/distributed-kv/scripts/kind-soak.sh [duration_s=300] [interval_s=0.2]
#
# Continuously writes (1 per interval) through the headless Service with client
# retries, deletes the current leader mid-soak, then asserts that zero
# acknowledged writes were lost: every acked key is read back from a surviving
# pod, and a 1/15 sample from the other two (including the restarted one).
# applied_index from /status is recorded before and after.
#
# Result line: target/kind-battery/results-raft.txt, scenario `soak`.
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${repo_root}"

DURATION="${1:-300}"
INTERVAL="${2:-0.2}"

BACKEND_KEY=raft; NAME=kv; APP=distributed-kv
SVC=kv-headless; YAML=examples/distributed-kv/k8s/kv.yaml
POD_PF_BASE=19101; SVC_PF_PORT=19100

LOG_DIR=target/kind-battery
LOG_FILE="${LOG_DIR}/soak-${BACKEND_KEY}.log"
RESULTS_FILE="${LOG_DIR}/results-${BACKEND_KEY}.txt"
KEYS_FILE="${LOG_DIR}/soak-${BACKEND_KEY}-acked.txt"
mkdir -p "${LOG_DIR}"
: >"${LOG_FILE}"
: >"${KEYS_FILE}"

source examples/distributed-kv/scripts/kind-lib.sh

trap pf_stop_all EXIT

log "=== soak ${BACKEND_KEY}: duration=${DURATION}s interval=${INTERVAL}s ==="

if ! wait_all_ready 120; then
  result soak FAIL "pods not Ready at soak start"
  exit 1
fi
for ord in 0 1 2; do ensure_pod_pf "${ord}"; done
ensure_svc_pf

idx_before=""
for ord in 0 1 2; do
  idx_before="${idx_before} ${NAME}-${ord}:$(pod_status "${ord}" | grep -oE '"applied_index":[0-9]+' | cut -d: -f2)"
done
log "applied_index before:${idx_before}"

mid=$((DURATION / 2))
start=${SECONDS}
end=$((start + DURATION))
n=0
unacked=0
killed=0
victim=""
victim_uid=""

while [ "${SECONDS}" -lt "${end}" ]; do
  key="soak-${BACKEND_KEY}-${n}"
  ok=0
  for _ in $(seq 1 24); do
    if kv_write "${SVC_PF_PORT}" "${key}" "${n}"; then ok=1; break; fi
    ensure_svc_pf
    sleep 0.5
  done
  if [ "${ok}" = 1 ]; then
    printf '%s %s\n' "${key}" "${n}" >>"${KEYS_FILE}"
    n=$((n + 1))
  else
    unacked=$((unacked + 1))
    log "write ${key} never acknowledged (counted, not verified)"
  fi
  if [ "${killed}" = 0 ] && [ $((SECONDS - start)) -ge "${mid}" ]; then
    victim="$(leader_ord || true)"
    victim="${victim:-0}"
    log "mid-soak: deleting ${NAME}-${victim}"
    victim_uid="$(pod_uid "${victim}")"
    KCTL_TIMEOUT=60 kctl delete pod "${NAME}-${victim}" --wait=false >>"${LOG_FILE}" 2>&1
    killed=1
  fi
  sleep "${INTERVAL}"
done
actual=$((SECONDS - start))
log "soak loop done: acked=${n} unacked=${unacked} duration=${actual}s"

# Let the deleted pod come back before verifying.
if [ -n "${victim}" ]; then
  wait_pod_ready "${victim}" 300 "${victim_uid}" || log "warn: ${NAME}-${victim} slow to return"
fi
for ord in 0 1 2; do ensure_pod_pf "${ord}"; done

idx_after=""
for ord in 0 1 2; do
  idx_after="${idx_after} ${NAME}-${ord}:$(pod_status "${ord}" | grep -oE '"applied_index":[0-9]+' | cut -d: -f2)"
done
log "applied_index after:${idx_after}"

# Full read-back on the first surviving pod; 1/15 sample on the other two.
victim_ord="${victim:-0}"
full_ord=$(( (victim_ord + 1) % 3 ))
other_ord=$(( (victim_ord + 2) % 3 ))
lost=0
log "verifying all ${n} acked keys on ${NAME}-${full_ord}"
verify_all_keys "$(pod_port "${full_ord}")" 600 1 || lost=$((lost + 1))
log "verifying 1/15 sample on ${NAME}-${victim_ord} (restarted) and ${NAME}-${other_ord}"
verify_all_keys "$(pod_port "${victim_ord}")" 300 15 || lost=$((lost + 1))
verify_all_keys "$(pod_port "${other_ord}")" 300 15 || lost=$((lost + 1))

if [ "${lost}" -eq 0 ]; then
  result soak PASS "acked=${n} unacked=${unacked} duration=${actual}s victim=${NAME}-${victim_ord} lost_acked=0 applied_index_after=[${idx_after# }]"
  exit 0
fi
result soak FAIL "acked=${n} unacked=${unacked} lost_acked>0 (see log)"
exit 1
