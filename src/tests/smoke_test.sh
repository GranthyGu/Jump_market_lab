#!/bin/sh
# Verify the unified CLI with separate producer and publisher processes.
set -eu
binary=$1
template=$2
temp_dir=$(mktemp -d /tmp/marketlab-demo.XXXXXX)
base=$((20000 + $$ % 20000))
mock_pid=
publisher_pid=
cleanup() {
  if [ -n "$publisher_pid" ]; then kill "$publisher_pid" 2>/dev/null || true; wait "$publisher_pid" 2>/dev/null || true; fi
  if [ -n "$mock_pid" ]; then kill "$mock_pid" 2>/dev/null || true; wait "$mock_pid" 2>/dev/null || true; fi
  if [ -f "$temp_dir/tcp.log" ]; then cat "$temp_dir/tcp.log"; fi
  if [ -f "$temp_dir/shm.log" ]; then cat "$temp_dir/shm.log"; fi
  rm -f "$temp_dir/config" "$temp_dir/mock.log" "$temp_dir/tcp.log" "$temp_dir/shm.log" \
    "$temp_dir/query.log" "$temp_dir/observe.log" "$temp_dir/a.mmap" "$temp_dir/a.mmap.owner" \
    "$temp_dir/b.mmap" "$temp_dir/b.mmap.owner"
  rmdir "$temp_dir"
}
trap cleanup EXIT INT TERM
sed -e "s/9201/$((base + 1))/g" -e "s/9202/$((base + 2))/g" \
    -e "s/port=9100/port=$((base + 3))/" -e "s/9000/$((base + 4))/g" \
    -e "s|/tmp/marketlab-shard-a.mmap|$temp_dir/a.mmap|" \
    -e "s|/tmp/marketlab-shard-b.mmap|$temp_dir/b.mmap|" "$template" > "$temp_dir/config"
"$binary" mock --config "$temp_dir/config" > "$temp_dir/mock.log" 2>&1 &
mock_pid=$!
for mode in tcp shm; do
  "$binary" publish --config "$temp_dir/config" --source "$mode" > "$temp_dir/$mode.log" 2>&1 &
  publisher_pid=$!
  ready=0
  attempt=0
  while [ "$attempt" -lt 40 ]; do
    if "$binary" query --config "$temp_dir/config" --symbol 1 > "$temp_dir/query.log" 2>/dev/null; then ready=1; break; fi
    attempt=$((attempt + 1))
    sleep 0.1
  done
  [ "$ready" -eq 1 ]
  grep -q 'SNAPSHOT symbol=1' "$temp_dir/query.log"
  "$binary" query --config "$temp_dir/config" --symbol 3 > "$temp_dir/query.log"
  grep -q 'entries=0' "$temp_dir/query.log"
  "$binary" observe --config "$temp_dir/config" --count 3 > "$temp_dir/observe.log"
  grep -q 'DELTA' "$temp_dir/observe.log"
  kill "$publisher_pid"
  wait "$publisher_pid"
  publisher_pid=
done
echo "Separate-process TCP and shared-memory demos passed."
