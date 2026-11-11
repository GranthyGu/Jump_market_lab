#!/bin/sh
# Foreground owner of one publisher; matching engines and gateway are independent.
set -eu
launcher_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
config="$launcher_root/src/config/publisher.conf"
build_dir="$launcher_root/build/bin"
log_root=
source_override=
multicast_interface=

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'HELP'
Usage: run_publisher.sh [options]
  --config PATH      Publisher configuration (default: src/config/publisher.conf)
  --build-dir PATH   Folder containing market_broadcast (default: build/bin/)
  --log-dir PATH     Parent log folder (default: build/logs/publisher/)
  --source tcp|shm   Override all source transports
  --interface IPv4  Local multicast sending interface (default from config)
  --help             Show this help
Keep this terminal open. Ctrl-C stops only this publisher.
Source outages show one red warning; retries continue silently until recovery.
HELP
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help)
      usage
      exit 0
      ;;
    --config|--build-dir|--log-dir|--source|--interface)
      [ "$#" -ge 2 ] || fail "Missing value for $1"
      case "$1" in
        --config)
          config=$2
          ;;
        --build-dir)
          build_dir=$2
          ;;
        --log-dir)
          log_root=$2
          ;;
        --source)
          source_override=$2
          ;;
        --interface)
          multicast_interface=$2
          ;;
      esac
      shift 2
      ;;
    *)
      fail "Unknown option: $1 (see --help)"
      ;;
  esac
done
[ -r "$config" ] || fail "Cannot read configuration: $config"
[ -x "$build_dir/market_broadcast" ] || fail "Missing market_broadcast; run $launcher_root/src/scripts/build.sh or select --build-dir"
case "$source_override" in
  ''|tcp|shm)
    ;;
  *)
    fail "Source must be tcp or shm"
    ;;
esac
umask 077
[ -n "$log_root" ] || log_root="$launcher_root/build/logs/publisher"
mkdir -p "$log_root"
run_dir=$(mktemp -d "$log_root/run.XXXXXX")
child_pid=
follower_pid=
cleanup() {
  trap '' INT TERM
  if [ -n "$child_pid" ]; then
    kill "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  if [ -n "$follower_pid" ]; then
    kill "$follower_pid" 2>/dev/null || true
    wait "$follower_pid" 2>/dev/null || true
  fi
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
set -- "$build_dir/market_broadcast" publish --config "$config"
if [ -n "$source_override" ]; then
  set -- "$@" --source "$source_override"
fi
if [ -n "$multicast_interface" ]; then
  set -- "$@" --interface "$multicast_interface"
fi
"$@" > "$run_dir/service.log" 2>&1 &
child_pid=$!
printf '%s\n' "$child_pid" > "$run_dir/service.pid"
sleep 0.2
if ! kill -0 "$child_pid" 2>/dev/null; then
  wait "$child_pid" 2>/dev/null || true
  child_pid=
  cat "$run_dir/service.log" >&2
  fail "Publisher exited during startup. Log: $run_dir/service.log"
fi
printf 'PROCESS_RUNNING component=publisher pid=%s\nCONFIG=%s\nLOG_DIR=%s\n' "$child_pid" "$config" "$run_dir"
printf '%s\n' 'Published index=... in service.log confirms a complete market-data batch. Ctrl-C stops this publisher.'
tail -n +1 -f "$run_dir/service.log" &
follower_pid=$!
set +e
wait "$child_pid"
result=$?
set -e
child_pid=
if [ "$result" -ne 0 ]; then
  printf 'ERROR: Publisher exited (%s). Log: %s/service.log\n' "$result" "$run_dir" >&2
fi
exit "$result"
