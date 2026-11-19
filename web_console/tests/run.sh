#!/bin/sh
set -eu
test_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PYTHONDONTWRITEBYTECODE=1
python3 -B -m unittest discover -s "$test_root/tests" -p 'test_*.py' -v
if command -v node >/dev/null 2>&1; then
    node --test "$test_root/tests/history_index.test.cjs"
fi
exec python3 -B "$test_root/tests/integration.py" "$@"
