#!/bin/bash
# Runtime-only build. Keep CMake internals away from binaries and session logs.
set -euo pipefail
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
build_root="$repo_root/build"
clean=false
jobs=4
while [ "$#" -gt 0 ]; do
  case "$1" in
    --clean) clean=true; shift ;;
    --jobs)
      [ "$#" -ge 2 ] || { echo 'Missing --jobs value' >&2; exit 1; }
      jobs=$2; shift 2 ;;
    --help)
      echo 'Usage: src/build.sh [--clean] [--jobs N]'
      echo '--clean removes build/, including old logs; stop all project services first.'
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done
case "$jobs" in ''|*[!0-9]*) echo 'Jobs must be a positive integer' >&2; exit 1 ;; esac
[ "$jobs" -gt 0 ] || { echo 'Jobs must be positive' >&2; exit 1; }
if [ "$clean" = true ]; then
  [ "$build_root" = "$repo_root/build" ] && [ -n "$repo_root" ] || exit 1
  rm -rf -- "$build_root"
fi
umask 077
mkdir -p "$build_root/bin" "$build_root/lib" "$build_root/logs/build"
cmake -S "$repo_root/src" -B "$build_root/cmake" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DMARKETLAB_BUILD_TESTS=OFF \
  -DCMAKE_RUNTIME_OUTPUT_DIRECTORY="$build_root/bin" \
  -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY="$build_root/lib" \
  -DCMAKE_LIBRARY_OUTPUT_DIRECTORY="$build_root/lib" \
  2>&1 | tee "$build_root/logs/build/configure.log"
cmake --build "$build_root/cmake" --parallel "$jobs" \
  --target market_gateway market_matching_engine market_broadcast gateway_client \
  2>&1 | tee "$build_root/logs/build/compile.log"
ln -sfn cmake/compile_commands.json "$build_root/compile_commands.json"
echo 'BUILD_COMPLETE: runtime binaries only (legacy tests OFF)'
printf '%s\n' "$build_root/bin/market_gateway" "$build_root/bin/market_matching_engine" \
  "$build_root/bin/market_broadcast" "$build_root/bin/gateway_client"
