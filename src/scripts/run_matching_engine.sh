#!/bin/sh
# The matching-engine module owns its router and all shard process lifetimes.
set -eu
launcher_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
exec python3 -B "$launcher_root/src/matching_engine/cluster/run.py" "$@"
