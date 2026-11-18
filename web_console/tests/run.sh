#!/bin/sh
set -eu
test_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PYTHONDONTWRITEBYTECODE=1
python3 -B -m unittest discover -s "$test_root/tests" -p 'test_*.py' -v
exec python3 -B "$test_root/tests/integration.py" "$@"
