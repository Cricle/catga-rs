#!/usr/bin/env bash
# distributed-kv kind scenario battery (raft).
#
#   examples/distributed-kv/scripts/kind-battery.sh
#
# Deploys the raft manifest to the kind cluster `catga-kv` from a clean
# slate (old StatefulSet/PVCs removed first), then runs:
#   (a) all pods Ready + leader elected (via /status)
#   (b) write via a follower pod, replicated to all three pods
#   (c) delete the leader pod, measure failover until a service write succeeds
#   (d) restarted pod catches up from its PVC and serves every recorded key
#   (e) rolling restart of all three pods, data intact throughout
#   (f) --bench-writes latency sample against the headless service
#
# Results: target/kind-battery/results-raft.txt (one `ts|be|scenario|PASS|detail`
# line per scenario); full log: target/kind-battery/battery-raft.log.
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${repo_root}"

if [ -n "${1:-}" ]; then
  echo "usage: $0 (raft is the only consensus backend; no arguments)" >&2
  exit 2
fi

BACKEND_KEY=raft; NAME=kv; APP=distributed-kv
SVC=kv-headless; YAML=examples/distributed-kv/k8s/kv.yaml
POD_PF_BASE=19101; SVC_PF_PORT=19100

LOG_DIR=target/kind-battery
LOG_FILE="${LOG_DIR}/battery-${BACKEND_KEY}.log"
RESULTS_FILE="${LOG_DIR}/results-${BACKEND_KEY}.txt"
KEYS_FILE="${LOG_DIR}/keys-${BACKEND_KEY}.txt"
mkdir -p "${LOG_DIR}"
: >"${LOG_FILE}"
: >"${KEYS_FILE}"

source examples/distributed-kv/scripts/kind-lib.sh

FAILURES=0
pass() { result "$1" PASS "$2"; log "PASS $1: $2"; }
fail() { result "$1" FAIL "$2"; log "FAIL $1: $2"; FAILURES=$((FAILURES + 1)); }

trap pf_stop_all EXIT

# --- deploy ------------------------------------------------------------------
log "=== deploy ${BACKEND_KEY} (${YAML}) from clean slate ==="
KCTL_TIMEOUT=180 kctl delete statefulset "${NAME}" --ignore-not-found --wait=true >>"${LOG_FILE}" 2>&1
KCTL_TIMEOUT=60  kctl delete pdb "${NAME}" --ignore-not-found --wait=false >>"${LOG_FILE}" 2>&1
KCTL_TIMEOUT=60  kctl delete svc "${SVC}" --ignore-not-found --wait=false >>"${LOG_FILE}" 2>&1
KCTL_TIMEOUT=120 kctl delete pvc "raft-data-${NAME}-0" "raft-data-${NAME}-1" "raft-data-${NAME}-2" \
  --ignore-not-found --wait=true >>"${LOG_FILE}" 2>&1
kctl apply -f "${YAML}" >>"${LOG_FILE}" 2>&1 || { fail deploy "kubectl apply failed"; exit 1; }
if ! wait_all_ready 300; then
  fail deploy "pods not Ready within 300s"
  exit 1
fi
pass deploy "3/3 Ready"
sleep 5 # let consensus settle before probing leadership

# --- (a) readiness + leadership ----------------------------------------------
log "=== (a) readiness + leadership ==="
LEADER=""
deadline=$((SECONDS + 90))
while [ "${SECONDS}" -lt "${deadline}" ] && [ -z "${LEADER}" ]; do
  for ord in 0 1 2; do ensure_pod_pf "${ord}"; done
  LEADER="$(leader_ord || true)"
  [ -n "${LEADER}" ] || sleep 1
done
if [ -n "${LEADER}" ]; then
  pass a "leader=${NAME}-${LEADER}"
else
  fail a "no leader elected within 90s"
fi
[ "${FAILURES}" -eq 0 ] || { log "aborting battery: (a) failed"; exit 1; }

# --- (b) follower write replicated everywhere --------------------------------
log "=== (b) follower write replicated ==="
WRITER=""
for ord in 0 1 2; do [ "${ord}" != "${LEADER}" ] && { WRITER="${ord}"; break; }; done
bval="b$(date +%s)"
wrote=0
for _ in $(seq 1 20); do
  if kv_write "$(pod_port "${WRITER}")" "battery-b" "${bval}"; then wrote=1; break; fi
  sleep 0.5
done
if [ "${wrote}" = 1 ]; then
  record_key "battery-b" "${bval}"
  rep_ok=1
  for ord in 0 1 2; do
    deadline=$((SECONDS + 30))
    while ! kv_read_check "$(pod_port "${ord}")" "battery-b" "${bval}"; do
      [ "${SECONDS}" -lt "${deadline}" ] || { rep_ok=0; break; }
      sleep 0.5
    done
  done
  [ "${rep_ok}" = 1 ] && pass b "write via ${NAME}-${WRITER} read back from all 3 pods" \
                      || fail b "replication incomplete"
else
  fail b "write via follower ${NAME}-${WRITER} failed"
fi
[ "${FAILURES}" -eq 0 ] || { log "aborting battery: (b) failed"; exit 1; }

# --- (c) leader kill -> failover ---------------------------------------------
log "=== (c) leader kill -> failover ==="
VICTIM="${LEADER}"
VICTIM_UID="$(pod_uid "${VICTIM}")"
ensure_svc_pf
t0="$(now_s)"
KCTL_TIMEOUT=60 kctl delete pod "${NAME}-${VICTIM}" --wait=false >>"${LOG_FILE}" 2>&1
fkey="failover-c"; fval="f$(date +%s)"
fok=0
deadline=$((SECONDS + 90))
while [ "${SECONDS}" -lt "${deadline}" ]; do
  ensure_svc_pf
  if kv_write "${SVC_PF_PORT}" "${fkey}" "${fval}"; then fok=1; break; fi
  sleep 0.5
done
t1="$(now_s)"
if [ "${fok}" = 1 ]; then
  record_key "${fkey}" "${fval}"
  detail="failover=$(elapsed_s "${t0}" "${t1}")s victim=${NAME}-${VICTIM}"
  new_leader=""
  d2=$((SECONDS + 60))
  while [ "${SECONDS}" -lt "${d2}" ] && [ -z "${new_leader}" ]; do
    for ord in 0 1 2; do [ "${ord}" != "${VICTIM}" ] && ensure_pod_pf "${ord}"; done
    new_leader="$(leader_ord || true)"
    [ -n "${new_leader}" ] || sleep 1
  done
  detail="${detail} new_leader=${NAME}-${new_leader:-unknown}"
  pass c "${detail}"
else
  fail c "no successful write within 90s of killing ${NAME}-${VICTIM}"
fi

# --- (d) restarted pod catches up (PVC) --------------------------------------
log "=== (d) restarted pod catch-up ==="
if wait_pod_ready "${VICTIM}" 300 "${VICTIM_UID}" && ensure_pod_pf "${VICTIM}"; then
  dkey="post-restart-d"; dval="d$(date +%s)"
  survivor=$(( (VICTIM + 1) % 3 ))
  ensure_pod_pf "${survivor}"
  if kv_write "$(pod_port "${survivor}")" "${dkey}" "${dval}"; then
    record_key "${dkey}" "${dval}"
  else
    log "warn: post-restart write via ${NAME}-${survivor} failed (verified keys only)"
  fi
  if verify_all_keys "$(pod_port "${VICTIM}")" 120; then
    pass d "${NAME}-${VICTIM} serves all $(count_keys) keys after restart"
  else
    fail d "${NAME}-${VICTIM} missing keys after restart"
  fi
else
  fail d "${NAME}-${VICTIM} did not become Ready again"
fi

# --- (e) rolling restart, data intact ----------------------------------------
log "=== (e) rolling restart ==="
e_ok=1
for ord in 0 1 2; do
  log "rolling restart: deleting ${NAME}-${ord}"
  old_uid="$(pod_uid "${ord}")"
  KCTL_TIMEOUT=60 kctl delete pod "${NAME}-${ord}" --wait=false >>"${LOG_FILE}" 2>&1
  if ! wait_pod_ready "${ord}" 300 "${old_uid}"; then e_ok=0; log "rolling: ${NAME}-${ord} not Ready"; break; fi
  ensure_pod_pf "${ord}" || { e_ok=0; break; }
  if ! verify_all_keys "$(pod_port "${ord}")" 120; then e_ok=0; log "rolling: ${NAME}-${ord} lost keys"; break; fi
  sleep 3 # let the cluster re-settle before the next deletion
done
if [ "${e_ok}" = 1 ]; then
  pass e "all 3 pods restarted one-by-one; $(count_keys) keys intact after each"
else
  fail e "rolling restart lost data or readiness"
fi

# --- (f) bench against the service -------------------------------------------
log "=== (f) bench-writes ==="
bench_out="$(MSYS_NO_PATHCONV=1 KCTL_TIMEOUT=420 kctl exec "${NAME}-0" -- \
  /usr/local/bin/distributed-kv --bench-writes 300 --bench-addr "${SVC}:9100" 2>&1)"
log "bench: ${bench_out}"
if printf '%s' "${bench_out}" | grep -q 'p50='; then
  p50="$(printf '%s' "${bench_out}" | grep -oE 'p50=[^ ]+' | head -1)"
  p99="$(printf '%s' "${bench_out}" | grep -oE 'p99=[^ ]+' | head -1)"
  mean="$(printf '%s' "${bench_out}" | grep -oE 'mean=[^ ]+' | head -1)"
  thr="$(printf '%s' "${bench_out}" | grep -oE 'throughput=[^ ]+' | head -1)"
  pass f "writes=300 ${mean} ${p50} ${p99} ${thr} (via ${SVC}:9100 from ${NAME}-0)"
else
  fail f "bench failed: ${bench_out}"
fi

# --- summary -----------------------------------------------------------------
log "=== battery ${BACKEND_KEY} done: FAILURES=${FAILURES} ==="
exit "${FAILURES}"
