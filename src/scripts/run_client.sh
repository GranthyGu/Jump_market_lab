#!/bin/sh
# The C++ console reads credentials from the terminal, then owns the TCP session.
set -eu
launcher_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
build_dir="$launcher_root/build/bin"
host=127.0.0.1
port=7001
user=
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'HELP'
Usage: run_client.sh [options]
  --host IPv4        Gateway address (default: 127.0.0.1)
  --port PORT        Gateway TCP port (default: 7001)
  --user NAME        Prefill username; password is still prompted without echo
  --build-dir PATH   Folder containing gateway_client (default: build/bin/)
  --help             Show this help
Opens a terminal login screen, then the buy/sell/cancel trading console.
Type help after READY for commands; quit disconnects without canceling orders.
HELP
}
while [ "$#" -gt 0 ]; do
  case "$1" in
    --help) usage; exit 0 ;;
    --host|--port|--user|--build-dir)
      [ "$#" -ge 2 ] || fail "Missing value for $1"
      case "$1" in
        --host) host=$2 ;;
        --port) port=$2 ;;
        --user) user=$2 ;;
        --build-dir) build_dir=$2 ;;
      esac
      shift 2 ;;
    *) fail "Unknown option: $1 (see --help)" ;;
  esac
done
[ -x "$build_dir/gateway_client" ] || fail "Missing gateway_client; run $launcher_root/src/scripts/build.sh or select --build-dir"
set -- "$build_dir/gateway_client" --host "$host" --port "$port"
if [ -n "$user" ]; then set -- "$@" --user "$user"; fi
exec "$@"
