#!/bin/sh
# All generated files stay in web_console/runtime unless explicitly overridden.
set -eu
console_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
export PYTHONDONTWRITEBYTECODE=1
exec python3 -B "$console_root/server.py" "$@"
