#!/bin/sh
# Interactive TCP snapshot console, with an optional single-query mode.
set -eu
launcher_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
config="$launcher_root/src/config/publisher.conf"
build_dir="$launcher_root/build/bin"
symbol=
host=
port=

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'HELP'
Usage: run_snapshot_client.sh [options]
  --symbol ID        Query once and exit; omit to open the interactive console
  --host IPv4        Publisher TCP address (default: query_host from config)
  --port PORT        Publisher TCP port (default: query_port from config, 9100)
  --config PATH      Market configuration (default: src/config/publisher.conf)
  --build-dir PATH   Folder containing market_broadcast (default: build/bin/)
  --help             Show this help
By default, enter query ID or just ID to request a snapshot; quit exits.
Each query prints the latest committed full snapshot; no trading login is needed.
Use --host for the publisher's address, not the multicast group or local interface.
HELP
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help)
      usage
      exit 0
      ;;
    --symbol|--host|--port|--config|--build-dir)
      [ "$#" -ge 2 ] && [ -n "$2" ] || fail "Missing value for $1"
      case "$1" in
        --symbol)
          symbol=$2
          ;;
        --host)
          host=$2
          ;;
        --port)
          port=$2
          ;;
        --config)
          config=$2
          ;;
        --build-dir)
          build_dir=$2
          ;;
      esac
      shift 2
      ;;
    *)
      fail "Unknown option: $1 (see --help)"
      ;;
  esac
done

valid_id() {
  awk -v n="$1" 'BEGIN { exit !(n ~ /^[0-9]+$/ && n >= 1 && n <= 65535) }'
}

if [ -n "$symbol" ]; then
  valid_id "$symbol" || fail "Symbol ID must be in 1..65535"
fi
if [ -n "$port" ]; then
  awk -v n="$port" 'BEGIN { exit !(n ~ /^[0-9]+$/ && n >= 1 && n <= 65535) }' || fail "Port must be in 1..65535"
fi
[ -r "$config" ] || fail "Cannot read configuration: $config"
[ -x "$build_dir/market_broadcast" ] || fail "Missing market_broadcast; run $launcher_root/src/scripts/build.sh or select --build-dir"

query_once() {
  set -- "$build_dir/market_broadcast" query --config "$config" --symbol "$1"
  if [ -n "$host" ]; then
    set -- "$@" --host "$host"
  fi
  if [ -n "$port" ]; then
    set -- "$@" --port "$port"
  fi
  "$@"
}

if [ -n "$symbol" ]; then
  query_once "$symbol"
  exit 0
fi

commands() {
  printf '%s\n' 'Commands: query ID | ID | help | quit (symbol ID: 1..65535)'
}

trap 'exit 130' INT
trap 'exit 143' TERM
printf '\n%s\n' '========== MarketLab TCP Snapshot Query =========='
commands
while printf 'snapshot> '; read -r action value extra; do
  case "$action" in
    '')
      continue
      ;;
    quit|exit)
      if [ -z "$value$extra" ]; then
        exit 0
      fi
      ;;
    help)
      if [ -z "$value$extra" ]; then
        commands
        continue
      fi
      ;;
    query)
      if [ -n "$value" ] && [ -z "$extra" ]; then
        if valid_id "$value"; then
          if ! query_once "$value"; then
            printf '%s\n' 'Query failed. Check the publisher endpoint and symbol ID, then try again.' >&2
          fi
          continue
        fi
      fi
      ;;
    *)
      if [ -z "$value$extra" ] && valid_id "$action"; then
        if ! query_once "$action"; then
          printf '%s\n' 'Query failed. Check the publisher endpoint and symbol ID, then try again.' >&2
        fi
        continue
      fi
      ;;
  esac
  printf '%s\n' 'INPUT_ERROR: enter query ID or ID (1..65535), help or quit.' >&2
done
printf '\n'
