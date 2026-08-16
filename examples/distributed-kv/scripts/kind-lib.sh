#!/usr/bin/env bash
# Shared helpers for the distributed-kv kind battery scripts (Git Bash on
# Windows + podman-kind). Sourced by kind-battery.sh and kind-soak.sh, which
# set: BACKEND_KEY NAME APP SVC YAML POD_PF_BASE SVC_PF_PORT LOG_DIR LOG_FILE
# RESULTS_FILE KEYS_FILE.
#
# Every kubectl call goes through kctl() (hard `timeout`), every curl carries
# --max-time, and every podman call in the driver wraps `timeout` with retries.
# The only long-lived processes are `kubectl port-forward` tunnels; they are
# tracked by PID and torn down by the EXIT trap (see pf_start for why they
# cannot sit under a `timeout` wrapper).

KUBECTL=(kubectl --context kind-catga-kv)
KCTL_TIMEOUT=30

kctl() { timeout "${KCTL_TIMEOUT}" "${KUBECTL[@]}" "$@"; }

ts() { date +%H:%M:%S; }
log() { printf '%s %s\n' "$(ts)" "$*" | tee -a "${LOG_FILE}"; }

# result <scenario> <PASS|FAIL|ABORT> <detail>
result() {
  printf '%s|%s|%s|%s|%s\n' "$(date +%FT%T)" "${BACKEND_KEY}" "$1" "$2" "$3" | tee -a "${RESULTS_FILE}"
}

now_s() { date +%s.%N; }
elapsed_s() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", b - a}'; }

# --- port-forward management -------------------------------------------------

pf_pidfile() { printf '%s/pf-%s.pid' "${LOG_DIR}" "$1"; }

pf_stop() { # <port>
  local f pid
  f="$(pf_pidfile "$1")"
  if [ -f "$f" ]; then
    pid="$(cat "$f")"
    kill "${pid}" 2>/dev/null || true
    rm -f "$f"
  fi
}

pf_start() { # <target: pod/x | svc/y> <local-port> [budget_s]
  local target="$1" port="$2" budget="${3:-8}" deadline pid
  pf_stop "${port}"
  : >"${LOG_DIR}/pf-${port}.log"
  deadline=$((SECONDS + budget))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    # Respawn the tunnel whenever it is not running: kubectl port-forward
    # exits immediately when the target pod is not Running yet, so a single
    # launch is not enough right after a pod restart.
    pid=""
    [ -f "$(pf_pidfile "${port}")" ] && pid="$(cat "$(pf_pidfile "${port}")")"
    if [ -z "${pid}" ] || ! kill -0 "${pid}" 2>/dev/null; then
      # Deliberately NOT wrapped in `timeout`: killing the timeout wrapper
      # would orphan the kubectl child and leak the bound port. Tunnels are
      # torn down by the EXIT trap (pf_stop_all).
      ( "${KUBECTL[@]}" port-forward "${target}" "${port}:9100" \
          >>"${LOG_DIR}/pf-${port}.log" 2>&1 &
        echo $! >"$(pf_pidfile "${port}")" )
    fi
    # Any HTTP answer (even 404/503) proves the tunnel; curl exit code is what matters.
    if curl --silent --max-time 2 "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.3
  done
  log "pf_start ${target} :${port} FAILED within ${budget}s"
  return 1
}

pf_stop_all() {
  local f
  for f in "${LOG_DIR}"/pf-*.pid; do
    [ -f "${f}" ] || continue
    kill "$(cat "${f}")" 2>/dev/null || true
    rm -f "${f}"
  done
}

pod_port() { echo $((POD_PF_BASE + $1)); }

ensure_pod_pf() { # <ordinal>
  local port
  port="$(pod_port "$1")"
  if curl --silent --max-time 2 "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then
    return 0
  fi
  pf_start "pod/${NAME}-$1" "${port}" 10
}

ensure_svc_pf() {
  if curl --silent --max-time 2 "http://127.0.0.1:${SVC_PF_PORT}/healthz" >/dev/null 2>&1; then
    return 0
  fi
  pf_start "svc/${SVC}" "${SVC_PF_PORT}" 10
}

# --- KV API ------------------------------------------------------------------

kv_write() { # <port> <key> <value> -> 0 on any 2xx
  curl --silent --max-time 5 -o /dev/null -w '%{http_code}' \
    -X POST "http://127.0.0.1:$1/kv" -H 'content-type: application/json' \
    --data "{\"key\":\"$2\",\"value\":\"$3\"}" 2>/dev/null | grep -q '^2'
}

kv_read() { # <port> <key> -> body on stdout
  curl --silent --max-time 5 "http://127.0.0.1:$1/kv/$2" 2>/dev/null
}

kv_read_check() { # <port> <key> <expected-value>
  kv_read "$1" "$2" | grep -qF "\"value\":\"$3\""
}

pod_status() { # <ordinal> -> /status body
  curl --silent --max-time 5 "http://127.0.0.1:$(pod_port "$1")/status" 2>/dev/null
}

# --- cluster state -----------------------------------------------------------

pod_uid() { # <ordinal> -> current pod uid (empty when absent)
  kctl get pod "${NAME}-$1" -o jsonpath='{.metadata.uid}' 2>/dev/null || true
}

wait_pod_ready() { # <ordinal> <budget_s> [old_uid]
  local ord="$1" budget="$2" old_uid="${3:-}"
  local deadline=$((SECONDS + budget))
  local out del phase ready uid
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    out="$(kctl get pod "${NAME}-${ord}" \
      -o jsonpath='{.metadata.deletionTimestamp}|{.status.phase}|{.status.conditions[?(@.type=="Ready")].status}|{.metadata.uid}' \
      2>/dev/null || true)"
    IFS='|' read -r del phase ready uid <<<"${out}"
    if [ -z "${del}" ] && [ "${phase}" = "Running" ] && [ "${ready}" = "True" ] \
      && { [ -z "${old_uid}" ] || [ "${uid}" != "${old_uid}" ]; }; then
      return 0
    fi
    sleep 2
  done
  log "wait_pod_ready ${NAME}-${ord} FAILED within ${budget}s"
  return 1
}

wait_all_ready() { # <budget_s>
  local ord
  for ord in 0 1 2; do
    wait_pod_ready "${ord}" "$1" || return 1
  done
}

# Echo the ordinal whose /status reports is_leader, or nothing.
leader_ord() {
  local ord body
  for ord in 0 1 2; do
    body="$(pod_status "${ord}")"
    if printf '%s' "${body}" | grep -qF '"is_leader":true'; then
      echo "${ord}"
      return 0
    fi
  done
  return 1
}

record_key() { # <key> <value>
  printf '%s %s\n' "$1" "$2" >>"${KEYS_FILE}"
}

# verify_all_keys <port> <budget_s> [stride] -> 0 when every recorded key reads back
verify_all_keys() { # <port> <budget_s> [stride]
  local port="$1" budget="$2" stride="${3:-1}"
  local deadline=$((SECONDS + budget))
  local missing k v n
  while :; do
    missing=0
    n=0
    while read -r k v; do
      [ -n "${k}" ] || continue
      n=$((n + 1))
      [ $((n % stride)) -eq 0 ] || [ "${stride}" -eq 1 ] || continue
      kv_read_check "${port}" "${k}" "${v}" || missing=$((missing + 1))
    done <"${KEYS_FILE}"
    if [ "${missing}" -eq 0 ]; then
      return 0
    fi
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      log "verify_all_keys: ${missing} key(s) missing on :${port} after ${budget}s"
      return 1
    fi
    sleep 1
  done
}

count_keys() { wc -l <"${KEYS_FILE}" | tr -d ' '; }
