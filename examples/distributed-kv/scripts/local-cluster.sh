#!/usr/bin/env bash
# Local N-node catga-raft KV cluster test.
#
#   examples/distributed-kv/scripts/local-cluster.sh [nodes] [base_port] [bench_writes]
#
# Starts `nodes` distributed-kv processes (raft gRPC ports base+i*100, HTTP
# API ports in the band raft+nodes*100, KV gRPC fast path in the band
# raft+2*nodes*100), waits for a leader, writes through the leader,
# verifies every node serves the key, runs the write bench against the
# leader, then shuts the cluster down.
set -u

nodes="${1:-3}"
base_port="${2:-9100}"
bench_writes="${3:-200}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
bin="$repo_root/target/release/distributed-kv"
[ -x "$bin" ] || [ -f "$bin.exe" ] || bin="$repo_root/target/debug/distributed-kv"
[ -f "$bin" ] || [ -f "$bin.exe" ] || { echo "binary not found; run: cargo build --release -p distributed-kv"; exit 1; }

api_port() { echo $((base_port + $1 * 100 + nodes * 100)); }

pids=()
cleanup() {
    for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null; done
    wait 2>/dev/null
}
trap cleanup EXIT

echo "== starting $nodes nodes (base raft port $base_port) =="
for i in $(seq 0 $((nodes - 1))); do
    "$bin" --node "$i" --nodes "$nodes" --base-port "$base_port" \
        > "/tmp/kv-local-$i.log" 2>&1 &
    pids+=($!)
done

echo "== waiting for a leader =="
leader_api=""
for _ in $(seq 1 120); do
    for i in $(seq 0 $((nodes - 1))); do
        ep=$(curl -s --max-time 1 "http://127.0.0.1:$(api_port "$i")/status" 2>/dev/null \
            | sed -n 's/.*"leader_endpoint":"[^"]*:\([0-9][0-9]*\)".*/\1/p')
        if [ -n "$ep" ]; then
            idx=$(( (ep - base_port) / 100 ))
            if [ "$idx" -ge 0 ] && [ "$idx" -lt "$nodes" ]; then
                leader_api=$(api_port "$idx")
                break 2
            fi
        fi
    done
    sleep 0.5
done
if [ -z "$leader_api" ]; then
    echo "FAIL: no leader elected in 60s"
    tail -5 /tmp/kv-local-0.log
    exit 1
fi
echo "leader HTTP API: 127.0.0.1:$leader_api"

echo "== replicated write via leader =="
write=""
for _ in $(seq 1 20); do
    write=$(curl -s -X POST "http://127.0.0.1:$leader_api/kv" \
        -H 'Content-Type: application/json' -d '{"key":"cluster-test","value":"ok"}')
    echo "$write" | grep -q '"key":"cluster-test"' && break
    sleep 0.5
done
echo "$write"
echo "$write" | grep -q '"key":"cluster-test"' || { echo "FAIL: write rejected"; exit 1; }

echo "== verifying all $nodes nodes serve the key =="
sleep 1
fail=0
for i in $(seq 0 $((nodes - 1))); do
    api=$(api_port "$i")
    # Only the leader serves reads; followers must answer 503.
    code=$(curl -s -o /tmp/kv-read.json -w "%{http_code}" "http://127.0.0.1:$api/kv/cluster-test")
    if [ "$api" = "$leader_api" ]; then
        grep -q '"value":"ok"' /tmp/kv-read.json || { echo "FAIL: leader $api missing key"; fail=1; }
    else
        [ "$code" = "503" ] || { echo "FAIL: follower $api answered $code (expected 503)"; fail=1; }
    fi
done
[ "$fail" = "0" ] || exit 1
echo "PASS: leader read OK, followers redirect-consistent (503)"

echo "== bench: $bench_writes sequential writes via leader =="
"$bin" --bench-writes "$bench_writes" --bench-addr "127.0.0.1:$leader_api"

echo "== shutting down =="
exit 0
