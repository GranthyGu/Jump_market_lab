#!/bin/sh
# Launch the configured shards as separate processes and own their lifetimes.
set -eu
launcher_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
config="$launcher_root/src/config/exchange.conf"
market_config="$launcher_root/src/config/publisher.conf"
build_dir="$launcher_root/build/bin"
log_root=
selected_shard=
trace=false
data_dir=
checkpoint_ms=600000
pending_mib=64
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'HELP'
Usage: run_matching_engine.sh [options]
  --config PATH         Exchange configuration (default: src/config/exchange.conf)
  --market-config PATH  Matching publisher config (default: src/config/publisher.conf)
  --shard ID            Run only this shard (default: all configured shards)
  --build-dir PATH      Folder containing market_matching_engine (default: build/bin/)
  --log-dir PATH        Parent log folder (default: build/logs/matching_engine/)
  --data-dir PATH       Persistent data root; default beside exchange config
  --checkpoint-ms N     Snapshot interval (default: 600000)
  --pending-mib N       Per-shard pending result budget (default: 64)
  --trace               Enable internal message tracing
  --help                Show this help
Ports and symbol ownership come from the two configs, not a fixed shard formula.
Keep this terminal open. Ctrl-C stops only the shards started by this script.
HELP
}
while [ "$#" -gt 0 ]; do
  case "$1" in
    --help) usage; exit 0 ;;
    --trace) trace=true; shift ;;
    --config|--market-config|--shard|--build-dir|--log-dir|--data-dir|--checkpoint-ms|--pending-mib)
      [ "$#" -ge 2 ] || fail "Missing value for $1"
      case "$1" in
        --config) config=$2 ;;
        --market-config) market_config=$2 ;;
        --shard) selected_shard=$2 ;;
        --build-dir) build_dir=$2 ;;
        --log-dir) log_root=$2 ;;
        --data-dir) data_dir=$2 ;;
        --checkpoint-ms) checkpoint_ms=$2 ;;
        --pending-mib) pending_mib=$2 ;;
      esac
      shift 2 ;;
    *) fail "Unknown option: $1 (see --help)" ;;
  esac
done
[ -r "$config" ] || fail "Cannot read configuration: $config"
[ -r "$market_config" ] || fail "Cannot read configuration: $market_config"
[ -x "$build_dir/market_matching_engine" ] || fail "Missing market_matching_engine; run $launcher_root/src/scripts/build.sh or select --build-dir"
if [ -n "$selected_shard" ]; then
  awk -v s="$selected_shard" 'BEGIN { exit !(s ~ /^[0-9]+$/ && s >= 1 && s <= 65535) }' || fail "Shard must be in 1..65535"
fi
umask 077
[ -n "$log_root" ] || log_root="$launcher_root/build/logs/matching_engine"
mkdir -p "$log_root"
run_dir=$(mktemp -d "$log_root/run.XXXXXX")
# Join the global symbol order to publisher symbol IDs to find each shard source.
# Only deployment metadata is inspected here; engines retain full config validation.
awk -v selected="$selected_shard" '
  function trim(s) { gsub(/^[ \t\r]+|[ \t\r]+$/, "", s); return s }
  function invalid(message) { print "ERROR: " message > "/dev/stderr"; bad=1; exit 1 }
  {
    line=$0; sub(/#.*/, "", line); line=trim(line); if (line == "") next
    eq=index(line, "="); if (!eq) invalid("Expected key=value")
    key=trim(substr(line, 1, eq-1)); value=trim(substr(line, eq+1))
    n=split(value, f, ","); for (i=1; i<=n; i++) f[i]=trim(f[i])
    if (FNR == NR) {
      if (key == "shard") {
        id=f[1]+0; if (n != 3 || id <= 0 || (id in hosts)) invalid("Invalid/duplicate order shard")
        ids[++count]=id; hosts[id]=f[2]; order_ports[id]=f[3]
      } else if (key == "symbol") {
        names[++symbols]=f[1]; owners[symbols]=f[3]+0
      }
    } else if (key == "symbols") {
      for (i=1; i<=n; i++) {
        split(f[i], pair, ":"); id=pair[1]+0
        if (id <= 0 || (id in market_names)) invalid("Duplicate market symbol ID")
        market_names[id]=pair[2]; market_symbols++
      }
    } else if (key == "shard") {
      if (n < 6 || (f[2] != "tcp" && f[2] != "shm")) invalid("Invalid market source")
      source=++sources; source_host[source]=f[3]; source_port[source]=f[4]
      source_mode[source]=f[2]; source_path[source]=f[5]
      if (index(f[5], "|") || f[5] == "") invalid("Unsupported shared-view path")
      for (i=6; i<=n; i++) {
        id=f[i]+0; if (id <= 0 || (id in source_for)) invalid("Duplicate source symbol ID")
        source_for[id]=source
      }
    }
  }
  END {
    if (bad) exit 1
    if (!count || !symbols || market_symbols != symbols) invalid("Incomplete exchange/market configuration")
    for (id=1; id<=symbols; id++) {
      if (market_names[id] != names[id] || !(id in source_for)) invalid("Exchange/market symbol mapping mismatch")
      shard=owners[id]; source=source_for[id]
      if (!(shard in hosts)) invalid("Unknown order shard")
      if ((shard in shard_source) && shard_source[shard] != source) invalid("One order shard has multiple market sources")
      if ((source in source_shard) && source_shard[source] != shard) invalid("One market source spans order shards")
      if (source_host[source] != hosts[shard]) invalid("Order and market source hosts must match")
      shard_source[shard]=source; source_shard[source]=shard
    }
    for (j=1; j<=count; j++) {
      shard=ids[j]; if (!(shard in shard_source)) invalid("Order shard has no market source")
      if (selected != "" && shard != selected+0) continue
      source=shard_source[shard]
      print shard "|" hosts[shard] "|" order_ports[shard] "|" source_port[source] "|" source_mode[source] "|" source_path[source]
      emitted++
    }
    if (!emitted) invalid("Requested shard is not configured")
  }
' "$config" "$market_config" > "$run_dir/launch-plan.txt" || fail "Cannot create shard launch plan"
shard_pids=
follower_pid=
cleanup() {
  trap '' INT TERM
  for shard_pid in $shard_pids; do kill "$shard_pid" 2>/dev/null || true; done
  for shard_pid in $shard_pids; do wait "$shard_pid" 2>/dev/null || true; done
  if [ -n "$follower_pid" ]; then kill "$follower_pid" 2>/dev/null || true; wait "$follower_pid" 2>/dev/null || true; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'CONFIG=%s\nMARKET_CONFIG=%s\nLOG_DIR=%s\n' "$config" "$market_config" "$run_dir"
printf 'shard\tpid\torder_endpoint\tmarket_endpoint\n' > "$run_dir/processes.tsv"
count=0
while IFS='|' read -r shard_id host order_port market_port transport shared_path; do
  set -- "$build_dir/market_matching_engine" --config "$config" --shard "$shard_id" \
    --market-port "$market_port" --stats "$run_dir/shard-${shard_id}.json" \
    --checkpoint-ms "$checkpoint_ms" --pending-mib "$pending_mib"
  if [ -n "$data_dir" ]; then set -- "$@" --data-dir "$data_dir"; fi
  if [ "$transport" = shm ]; then set -- "$@" --market-shm "$shared_path"; fi
  if [ "$trace" = true ]; then set -- "$@" --trace; fi
  "$@" > "$run_dir/shard-${shard_id}.log" 2>&1 &
  shard_pid=$!
  shard_pids="$shard_pids $shard_pid"
  printf '%s\t%s\t%s:%s\t%s:%s\n' "$shard_id" "$shard_pid" "$host" "$order_port" "$host" "$market_port" >> "$run_dir/processes.tsv"
  count=$((count + 1))
done < "$run_dir/launch-plan.txt"
tail -n +1 -f "$run_dir"/shard-*.log &
follower_pid=$!
sleep 0.2
while :; do
  for shard_pid in $shard_pids; do
    if ! kill -0 "$shard_pid" 2>/dev/null; then
      fail "Shard process $shard_pid exited; stopping this group. Inspect $run_dir/shard-*.log"
    fi
  done
  if [ "$count" -gt 0 ]; then
    printf 'PROCESSES_RUNNING component=matching_engine shards=%s pid_table=%s/processes.tsv\n' "$count" "$run_dir"
    printf '%s\n' 'This reports process liveness. Successful gateway login confirms order-link readiness. Ctrl-C stops this group.'
    count=0
  fi
  sleep 1
done
