#!/bin/sh
# Foreground owner of one gateway process. Configuration overrides stay in build/.
set -eu
launcher_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
config="$launcher_root/src/config/exchange.conf"
build_dir="$launcher_root/build/bin"
log_root=
port_override=
trace=false
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'HELP'
Usage: run_gateway.sh [options]
  --config PATH      Exchange configuration (default: src/config/exchange.conf)
  --port PORT        Optional override in a runtime config copy (default: 7001)
  --build-dir PATH   Folder containing market_gateway (default: build/bin/)
  --log-dir PATH     Parent log folder (default: build/logs/gateway/)
  --trace            Enable internal message tracing
  --help             Show this help
Keep this terminal open. Ctrl-C stops only this gateway.
HELP
}
while [ "$#" -gt 0 ]; do
  case "$1" in
    --help) usage; exit 0 ;;
    --trace) trace=true; shift ;;
    --config|--port|--build-dir|--log-dir)
      [ "$#" -ge 2 ] || fail "Missing value for $1"
      case "$1" in
        --config) config=$2 ;;
        --port) port_override=$2 ;;
        --build-dir) build_dir=$2 ;;
        --log-dir) log_root=$2 ;;
      esac
      shift 2 ;;
    *) fail "Unknown option: $1 (see --help)" ;;
  esac
done
[ -r "$config" ] || fail "Cannot read configuration: $config"
[ -x "$build_dir/market_gateway" ] || fail "Missing market_gateway; run $launcher_root/src/scripts/build.sh or select --build-dir"
if [ -n "$port_override" ]; then
  awk -v p="$port_override" 'BEGIN { exit !(p ~ /^[0-9]+$/ && p >= 1 && p <= 65535) }' || fail "Port must be in 1..65535"
fi
umask 077
[ -n "$log_root" ] || log_root="$launcher_root/build/logs/gateway"
mkdir -p "$log_root"
run_dir=$(mktemp -d "$log_root/run.XXXXXX")
if [ -n "$port_override" ]; then
  awk -v port="$port_override" '
    /^[ \t]*gateway_listen[ \t]*=/ {
      split($0, a, "="); split(a[2], b, ","); gsub(/^[ \t]+|[ \t]+$/, "", b[1]);
      print "gateway_listen=" b[1] "," port; replaced=1; next
    }
    { print }
    END { if (!replaced) print "gateway_listen=127.0.0.1," port }
  ' "$config" > "$run_dir/exchange.conf"
  config="$run_dir/exchange.conf"
fi
endpoint=$(awk '
  BEGIN { value="127.0.0.1,7001" }
  { sub(/#.*/, "") }
  /^[ \t]*gateway_listen[ \t]*=/ { sub(/^[^=]*=/, ""); gsub(/[ \t\r]/, ""); value=$0 }
  END { print value }
' "$config")
child_pid=
follower_pid=
cleanup() {
  trap '' INT TERM
  if [ -n "$child_pid" ]; then
    kill "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  if [ -n "$follower_pid" ]; then kill "$follower_pid" 2>/dev/null || true; wait "$follower_pid" 2>/dev/null || true; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
set -- "$build_dir/market_gateway" gateway --config "$config" --stats "$run_dir/stats.json"
if [ "$trace" = true ]; then set -- "$@" --trace; fi
"$@" > "$run_dir/service.log" 2>&1 &
child_pid=$!
printf '%s\n' "$child_pid" > "$run_dir/service.pid"
sleep 0.2
if ! kill -0 "$child_pid" 2>/dev/null; then
  wait "$child_pid" 2>/dev/null || true
  child_pid=
  cat "$run_dir/service.log" >&2
  fail "Gateway exited during startup. Log: $run_dir/service.log"
fi
printf 'PROCESS_RUNNING component=gateway pid=%s configured_endpoint=%s\n' "$child_pid" "$endpoint"
printf 'CONFIG=%s\nLOG_DIR=%s\n' "$config" "$run_dir"
printf '%s\n' 'Successful client login confirms all backend links are ready. Ctrl-C stops this gateway.'
tail -n +1 -f "$run_dir/service.log" &
follower_pid=$!
set +e
wait "$child_pid"
result=$?
set -e
child_pid=
if [ "$result" -ne 0 ]; then printf 'ERROR: Gateway exited (%s). Log: %s/service.log\n' "$result" "$run_dir" >&2; fi
exit "$result"
