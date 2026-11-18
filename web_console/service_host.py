#!/usr/bin/env python3
"""A lifeline guardian: shut down the native service if its console disappears."""
import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

from deployment import atomic_json, process_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--record", required=True, type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("Missing service command")
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, lambda *_: stopped.set())
    def lifeline():
        try:
            while os.read(0, 1):
                pass
        except OSError:
            pass
        stopped.set()
    threading.Thread(target=lifeline, daemon=True).start()
    # Native stdout goes directly to the log file, never to a dying console's pipe.
    child = subprocess.Popen(command, stdin=subprocess.DEVNULL)
    record = dict(name=args.name, host_pid=os.getpid(), console_pid=os.getppid(),
                  child_pid=child.pid, command=command, started=time.time(), status="running")
    atomic_json(args.record, record)
    print(f"HOST START component={args.name} host={os.getpid()} child={child.pid}", flush=True)
    sent = False
    try:
        while child.poll() is None:
            if stopped.is_set() and not sent:
                print(f"HOST STOP component={args.name} reason=console_closed_or_stop_requested", flush=True)
                # Resume stopped workers so the supervisor can drain and close them.
                os.killpg(os.getpgrp(), signal.SIGCONT)
                child.terminate()
                sent = True
            time.sleep(.1)
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait()
        # If the native supervisor itself died, terminate its remaining descendants.
        members = [r for r in process_rows().values()
                   if r["group"] == os.getpgrp() and r["pid"] != os.getpid()]
        if members:
            os.killpg(os.getpgrp(), signal.SIGCONT)
            os.killpg(os.getpgrp(), signal.SIGTERM)
        record.update(status="exited", exit_code=child.returncode, stopped=time.time())
        atomic_json(args.record, record)
        print(f"HOST EXIT component={args.name} code={child.returncode}", flush=True)
    return child.returncode if child.returncode >= 0 else 128 - child.returncode


if __name__ == "__main__":
    sys.exit(main())
