#!/bin/sh
# Join the configured market-data feed and preserve the observer's output/signals.
set -eu
launcher_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
config="$launcher_root/src/config/publisher.conf"
build_dir="$launcher_root/build/bin"
multicast_interface=
count=
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'HELP'
Usage: run_market_client.sh [options]
  --config PATH      Market configuration (default: src/config/publisher.conf)
  --interface IPv4   Local receiving interface (default from config)
  --count N          Exit after N datagrams; 0 means unlimited (default)
  --build-dir PATH   Folder containing market_broadcast (default: build/bin/)
  --help             Show this help
Displays the same snapshots, deltas and price levels as market_broadcast observe.
Multiple terminals can join the same group. Ctrl-C stops only this receiver.
HELP
}
while [ "$#" -gt 0 ]; do
  case "$1" in
    --help) usage; exit 0 ;;
    --config|--interface|--count|--build-dir)
      [ "$#" -ge 2 ] || fail "Missing value for $1"
      case "$1" in
        --config) config=$2 ;;
        --interface) multicast_interface=$2 ;;
        --count) count=$2 ;;
        --build-dir) build_dir=$2 ;;
      esac
      shift 2 ;;
    *) fail "Unknown option: $1 (see --help)" ;;
  esac
done
[ -r "$config" ] || fail "Cannot read configuration: $config"
[ -x "$build_dir/market_broadcast" ] || fail "Missing market_broadcast; run $launcher_root/src/scripts/build.sh or select --build-dir"
if [ -n "$count" ]; then
  case "$count" in *[!0-9]*) fail "Count must be a nonnegative integer" ;; esac
fi
set -- "$build_dir/market_broadcast" observe --config "$config"
if [ -n "$multicast_interface" ]; then set -- "$@" --interface "$multicast_interface"; fi
if [ -n "$count" ]; then set -- "$@" --count "$count"; fi
exec "$@"
