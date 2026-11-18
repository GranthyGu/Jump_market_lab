"""Keep configuration identities and process cleanup local to this console."""
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as out:
        json.dump(value, out, indent=2)
        out.write("\n")
        out.flush()
        os.fsync(out.fileno())
    temporary.replace(path)


def symbol_identity(path):
    from bridge import entries
    rows = list(entries(path))
    settings = dict(rows)
    return dict(version=1, symbols=[values for key, values in rows if key == "symbol"],
                spreads=settings.get("spreads", ["disabled"]),
                spread_symbols=settings.get("spread_symbols", ["A", "B", "C", "SP_AB", "SP_AC", "BF_ABC"]))


def data_directory(config, runtime, requested, journal):
    """Never load persisted symbol IDs under a different universe."""
    identity = symbol_identity(config.exchange)
    base = requested or runtime / "data"
    manifest = base / "console-symbols.json"
    previous = json.loads(manifest.read_text()) if manifest.exists() else None
    if previous is None and (base / "cluster-state.json").exists() and requested is None:
        # Older console versions left exact generated Router configurations in their logs.
        known = {json.dumps(symbol_identity(p), sort_keys=True)
                 for p in (runtime / "logs/engines").glob("*/router.conf")}
        if len(known) != 1:
            raise ValueError(f"Cannot identify the symbol universe of {base}. Choose a verified compatible --data-dir")
        previous = json.loads(known.pop())
        atomic_json(manifest, previous)
        journal.add("console", f"DATA IDENTIFIED Existing symbol mapping retained at {base}")
    if previous is not None and previous != identity:
        if requested is not None:
            raise ValueError(f"The selected data directory belongs to a different symbol configuration: {base}")
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:12]
        selected = runtime / f"data-{digest}"
        journal.add("console", f"CONFIGURATION CHANGED Using {selected}; previous state preserved at {base}", "warning")
        base, manifest = selected, selected / "console-symbols.json"
        if manifest.exists() and json.loads(manifest.read_text()) != identity:
            raise ValueError(f"Data identity mismatch at {base}")
    atomic_json(manifest, identity)
    return base


def process_rows():
    result = subprocess.run(["ps", "-axo", "pid=,ppid=,pgid=,comm="],
                            capture_output=True, text=True, timeout=5, check=True)
    rows = {}
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 3)
        if len(fields) == 4 and all(v.isdecimal() for v in fields[:3]):
            pid, parent, group = map(int, fields[:3])
            rows[pid] = dict(pid=pid, parent=parent, group=group, executable=fields[3])
    return rows


def command_for(pid):
    result = subprocess.run(["ps", "-ww", "-p", str(pid), "-o", "lstart=,command="],
                            capture_output=True, text=True, timeout=3)
    return result.stdout.strip() if result.returncode == 0 else ""


def owned_orphans(config, runtime):
    """Only adopt abandoned native binaries built inside this exact runtime."""
    binaries = runtime / "build/bin"
    roles = {str(binaries / name): role for name, role in (
        ("market_gateway", "gateway"), ("market_broadcast", "publisher"),
        ("market_matching_engine", "engine"), ("market_engine_router", "router"),
        ("market_group_coordinator", "coordinator"))}
    found = []
    for row in process_rows().values():
        role = roles.get(row["executable"])
        if row["parent"] != 1 or role is None:
            continue
        command = command_for(row["pid"])
        if role in ("gateway", "publisher"):
            path = config.exchange if role == "gateway" else config.publisher
            owned = bool(re.search(r"--config\s+" + re.escape(str(path)) + r"(?:\s|$)", command))
        else:
            prefix = str(runtime / "logs/engines") + "/"
            owned = bool(re.search(r"--config\s+" + re.escape(prefix) + r"[^\n]*?\.conf(?:\s|$)", command))
        if owned:
            found.append(dict(row, role=role, identity=command))
    return found


def cleanup_orphans(config, runtime, journal):
    abandoned = owned_orphans(config, runtime)
    if not abandoned:
        return
    atomic_json(runtime / "logs" / f"orphan-cleanup-{time.time_ns()}.json", abandoned)
    journal.add("console", f"ORPHAN CLEANUP Found {len(abandoned)} abandoned processes from this console", "warning")
    # Stop external admission first. Native persistence files and recovery flags stay intact.
    order = {"gateway": 0, "publisher": 1, "router": 2, "engine": 3, "coordinator": 4}
    signaled = []
    for row in sorted(abandoned, key=lambda r: order[r["role"]]):
        current = process_rows().get(row["pid"])
        if not current or current != {k: row[k] for k in ("pid", "parent", "group", "executable")}:
            continue
        if command_for(row["pid"]) != row["identity"]:
            continue
        try:
            os.kill(row["pid"], signal.SIGCONT)
            os.kill(row["pid"], signal.SIGTERM)
            signaled.append(row)
            journal.add("console", f"ORPHAN STOP role={row['role']} pid={row['pid']}", "warning")
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        alive = process_rows()
        remaining = [r for r in signaled if r["pid"] in alive and command_for(r["pid"]) == r["identity"]]
        if not remaining:
            journal.add("console", "ORPHAN CLEANUP COMPLETE Ports released; previous data retained", "success")
            return
        time.sleep(.1)
    raise RuntimeError("Abandoned processes have not exited after SIGTERM: " + ", ".join(str(r["pid"]) for r in remaining))
