#!/bin/sh
# Request one complete market-data snapshot over the publisher's TCP interface.
set -eu
launcher_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
config="$launcher_root/src/config/publisher.conf"
build_dir="$launcher_root/build/bin"
symbol=1
host=
port=
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'HELP'
Usage: run_snapshot_client.sh [options]
  --symbol ID        Symbol ID in 1..65535 (default: 1, ITEM0001)
  --host IPv4        Publisher TCP address (default: query_host from config)
  --port PORT        Publisher TCP port (default: query_port from config, 9100)
  --config PATH      Market configuration (default: src/config/publisher.conf)
  --build-dir PATH   Folder containing market_broadcast (default: build/bin/)
  --help             Show this help
Prints the latest committed full snapshot and exits; no trading login is needed.
Use --host for the publisher's address, not the multicast group or local interface.
HELP
}
while [ "$#" -gt 0 ]; do
  case "$1" in
    --help) usage; exit 0 ;;
    --symbol|--host|--port|--config|--build-dir)
      [ "$#" -ge 2 ] && [ -n "$2" ] || fail "Missing value for $1"
      case "$1" in
        --symbol) symbol=$2 ;;
        --host) host=$2 ;;
        --port) port=$2 ;;
        --config) config=$2 ;;
        --build-dir) build_dir=$2 ;;
      esac
      shift 2 ;;
    *) fail "Unknown option: $1 (see --help)" ;;
  esac
done
awk -v n="$symbol" 'BEGIN { exit !(n ~ /^[0-9]+$/ && n >= 1 && n <= 65535) }' || fail "Symbol ID must be in 1..65535"
if [ -n "$port" ]; then
  awk -v n="$port" 'BEGIN { exit !(n ~ /^[0-9]+$/ && n >= 1 && n <= 65535) }' || fail "Port must be in 1..65535"
fi
[ -r "$config" ] || fail "Cannot read configuration: $config"
[ -x "$build_dir/market_broadcast" ] || fail "Missing market_broadcast; run $launcher_root/src/scripts/build.sh or select --build-dir"
set -- "$build_dir/market_broadcast" query --config "$config" --symbol "$symbol"
if [ -n "$host" ]; then set -- "$@" --host "$host"; fi
if [ -n "$port" ]; then set -- "$@" --port "$port"; fi
exec "$@"
