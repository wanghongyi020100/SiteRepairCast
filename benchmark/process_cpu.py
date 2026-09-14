#!/usr/bin/env python3
"""Read Linux process CPU ticks, optionally watching until the process exits."""

import argparse
import json
import os
from pathlib import Path
import time


def read_sample(pid):
    raw = Path("/proc/%d/stat" % pid).read_text(encoding="utf-8")
    closing = raw.rfind(")")
    if closing < 0:
        raise RuntimeError("invalid /proc/%d/stat" % pid)
    fields = raw[closing + 2:].split()
    if len(fields) < 13:
        raise RuntimeError("short /proc/%d/stat" % pid)
    return {
        "pid": pid,
        "utime_ticks": int(fields[11]),
        "stime_ticks": int(fields[12]),
        "total_ticks": int(fields[11]) + int(fields[12]),
        "clk_tck": int(os.sysconf("SC_CLK_TCK")),
        "monotonic_seconds": time.monotonic(),
    }


def write_json(path, value):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(destination)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.pid <= 0 or args.interval <= 0:
        parser.error("pid and interval must be positive")
    if args.watch and not args.output:
        parser.error("--watch requires --output")

    if not args.watch:
        print(json.dumps(read_sample(args.pid), sort_keys=True))
        return

    first = None
    latest = None
    while True:
        try:
            latest = read_sample(args.pid)
        except FileNotFoundError:
            break
        if first is None:
            first = latest
        write_json(args.output, {"first": first, "latest": latest, "process_exited": False})
        time.sleep(args.interval)

    if latest is None:
        raise SystemExit("process disappeared before the first CPU sample")
    write_json(args.output, {"first": first, "latest": latest, "process_exited": True})


if __name__ == "__main__":
    main()
