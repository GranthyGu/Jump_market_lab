#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
build="$root/build/demo-tests"
if [ "${1:-}" = --help ]; then
  echo 'Usage: tests/build.sh [BUILD_DIRECTORY] (Release build of services, presentation, mailbox and recovery tests)'
  exit 0
fi
[ "$#" -le 1 ] || { echo 'Expected at most one build directory' >&2; exit 2; }
[ "$#" -eq 0 ] || build=$1
cmake -S "$root/src" -B "$build" -DCMAKE_BUILD_TYPE=Release -DMARKETLAB_BUILD_TESTS=ON
cmake --build "$build" --parallel 4 --target demo_tests
