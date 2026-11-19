#!/usr/bin/env python3
"""Run the same paced demo as the browser's Load demo button."""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description="Load real demonstration orders into a running empty console")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--duration", type=int, choices=range(10,16), default=12)
    parser.add_argument("--cancel", action="store_true", help="Stop loading; leave engines and accepted orders running")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be in 1..65535")
    url = f"http://127.0.0.1:{args.port}"
    def state():
        with urllib.request.urlopen(url+"/api/state",timeout=5) as response:
            return json.load(response)
    current = state()
    request = urllib.request.Request(url + ("/api/demo/cancel" if args.cancel else "/api/demo/start"),
        json.dumps(dict(duration=args.duration)).encode(),
        {"Content-Type":"application/json", "X-Console-Token":current["csrf"]})
    with urllib.request.urlopen(request,timeout=5) as response:
        print(json.load(response)["message"],flush=True)
    deadline = time.monotonic() + 90
    previous = None
    while time.monotonic() < deadline:
        current = state()
        demo = current["demo"]
        line = f"{demo['phase']}: {demo['accepted']}/{demo['total']} orders · {demo['elapsed']}s"
        if line != previous:
            print(line,flush=True)
            previous = line
        if not demo["running"]:
            if demo["error"]:
                raise RuntimeError(demo["error"])
            print(f"{demo['accepted']} accepted · {demo['filled_orders']} fully filled · {demo['active_orders']} resting orders · {demo['history_records']} history records. Manual trading is ready.")
            return
        time.sleep(.75)
    raise TimeoutError("Replay is still active. Inspect the browser or use demo.sh --cancel")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as exc:
        print("ERROR:",json.loads(exc.read()).get("error",str(exc)),file=sys.stderr)
        sys.exit(1)
    except (OSError, ValueError, RuntimeError) as exc:
        print("ERROR:",exc,file=sys.stderr)
        sys.exit(1)
