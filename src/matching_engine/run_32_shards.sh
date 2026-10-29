#!/bin/sh
# Run from any directory: sh src/matching_engine/run_32_shards.sh
# Foreground supervisor; Ctrl-C stops only its own children.
set -eu
cluster_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$cluster_root"
cluster_output=build/cluster_32x1000
mkdir -p "$cluster_output"
cluster_pids=
cleanup() {
  trap '' INT TERM
  for cluster_pid in $cluster_pids; do kill "$cluster_pid" 2>/dev/null || true; done
  for cluster_pid in $cluster_pids; do wait "$cluster_pid" 2>/dev/null || true; done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
[ -x build/market_gateway ] && [ -x build/market_broadcast ] && [ -x build/market_matching_engine ] || {
  printf '%s\n' 'Build first: cmake -S src -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j 4' >&2
  exit 1
}
cluster_shard=1
while [ "$cluster_shard" -le 32 ]; do
  ./build/market_matching_engine --config src/gateway/scale_32x1000.conf \
    --shard "$cluster_shard" --market-port "$((9200 + cluster_shard))" \
    --stats "$cluster_output/shard-${cluster_shard}.json" \
    > "$cluster_output/shard-${cluster_shard}.log" 2>&1 &
  cluster_pids="$cluster_pids $!"
  cluster_shard=$((cluster_shard + 1))
done
./build/market_gateway gateway --config src/gateway/scale_32x1000.conf \
  --stats "$cluster_output/gateway.json" > "$cluster_output/gateway.log" 2>&1 &
cluster_pids="$cluster_pids $!"
./build/market_broadcast publish --config src/market_publisher/scale_32x1000.conf \
  > "$cluster_output/publisher.log" 2>&1 &
cluster_pids="$cluster_pids $!"
printf '%s\n' 'Started 32 shards + gateway + publisher; gateway TCP 7000, orders 7201..7232, market sources 9201..9232.'
printf '%s\n' 'Logs: build/cluster_32x1000/. Readiness: successful gateway login and publisher query (see deployment guide).'
printf '%s\n' 'Keep this terminal open. Ctrl-C stops this cluster. Do not run another cluster on the same ports.'
while :; do
  for cluster_pid in $cluster_pids; do
    if ! kill -0 "$cluster_pid" 2>/dev/null; then
      printf 'Child %s exited; inspect %s/*.log\n' "$cluster_pid" "$cluster_output" >&2
      exit 1
    fi
  done
  sleep 1
done
