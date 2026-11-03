#!/usr/bin/env python3
"""Sample Linux process and host resources for lab benchmark runs."""

import argparse
import csv
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import time

try:
    from common import directory_size, utc_now
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import directory_size, utc_now


STOP = False


FIELDS = [
    "timestamp", "host", "role", "receiver_index", "pid", "rss_kb", "pss_kb",
    "cpu_percent", "cpu_seconds", "threads", "fd_count", "mem_total_kb",
    "mem_available_kb", "swap_total_kb", "swap_free_kb", "load_1", "load_5",
    "load_15", "disk_free_bytes", "test_directory_bytes",
]


def handle_signal(_signum, _frame):
    global STOP
    STOP = True


def read_proc(pid):
    stat_raw = Path("/proc/%d/stat" % pid).read_text(encoding="utf-8")
    closing = stat_raw.rfind(")")
    if closing < 0:
        raise ValueError("invalid /proc/%d/stat: missing comm terminator" % pid)
    fields = stat_raw[closing + 2:].split()
    if len(fields) <= 21:
        raise ValueError("short /proc/%d/stat: %d fields after comm" % (pid, len(fields)))
    clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    total_ticks = int(fields[11]) + int(fields[12])
    rss_pages = int(fields[21])
    page_kb = os.sysconf("SC_PAGE_SIZE") // 1024
    status = {}
    for line in Path("/proc/%d/status" % pid).read_text(encoding="utf-8").splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            parts = value.strip().split()
            if parts:
                status[key] = parts[0]
    pss_kb = ""
    rollup = Path("/proc/%d/smaps_rollup" % pid)
    if rollup.exists():
        for line in rollup.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("Pss:"):
                parts = line.split()
                if len(parts) > 1:
                    pss_kb = parts[1]
                break
    try:
        fd_count = len(list(Path("/proc/%d/fd" % pid).iterdir()))
    except OSError:
        fd_count = ""
    return {
        "rss_kb": int(status.get("VmRSS", rss_pages * page_kb)),
        "pss_kb": pss_kb,
        "cpu_seconds": total_ticks / float(clk_tck),
        "threads": int(status.get("Threads", 0)),
        "fd_count": fd_count,
    }


def read_meminfo():
    values = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        key, value = line.split(":", 1)
        values[key] = int(value.strip().split()[0])
    return values


def read_loadavg():
    parts = Path("/proc/loadavg").read_text(encoding="utf-8").split()
    return [float(parts[0]), float(parts[1]), float(parts[2])]


def parse_process(value):
    parts = value.split(",", 2)
    if len(parts) != 3:
        raise argparse.ArgumentTypeError("--process must be role,index,pid")
    return {"role": parts[0], "receiver_index": parts[1], "pid": int(parts[2])}


def process_rows(processes):
    rows = [{"role": "host", "receiver_index": "", "pid": 0}]
    rows.extend(processes)
    return rows


def write_warning(message):
    print("resource_monitor warning: %s" % message, file=sys.stderr, flush=True)


def run_self_test():
    import tempfile
    with tempfile.TemporaryDirectory(prefix="srcast-resource-monitor-") as tmp:
        tmp_path = Path(tmp)
        sleeper = subprocess.Popen(["sleep", "1"])
        output = tmp_path / "resources.csv"
        log = tmp_path / "resource-monitor.log"
        command = [
            sys.executable, str(Path(__file__).resolve()),
            "--host-label", "local-self-test",
            "--output", str(output),
            "--interval", "0.2",
            "--test-dir", str(tmp_path),
            "--process", "test,,%d" % sleeper.pid,
            "--process", "gone,,99999999",
        ]
        with log.open("w", encoding="utf-8") as handle:
            monitor = subprocess.Popen(command, stdout=handle, stderr=subprocess.STDOUT)
            time.sleep(1.2)
            monitor.terminate()
            try:
                monitor.wait(timeout=3)
            except subprocess.TimeoutExpired:
                monitor.kill()
        sleeper.wait(timeout=3)
        text = output.read_text(encoding="utf-8")
        lines = text.splitlines()
        log_text = log.read_text(encoding="utf-8", errors="replace")
        if len(lines) <= 1:
            raise SystemExit("self-test failed: resources.csv has no samples")
        if "Traceback" in log_text or "IndexError" in log_text:
            raise SystemExit("self-test failed: monitor tracebacked\n%s" % log_text)
        reader = csv.DictReader(lines)
        process_samples = [row for row in reader if row.get("role") == "test" and row.get("rss_kb")]
        if not process_samples:
            raise SystemExit("self-test failed: no process sample with rss_kb")
        required = ["pid", "rss_kb", "cpu_seconds", "threads", "fd_count"]
        missing = [key for key in required if not process_samples[0].get(key)]
        if missing:
            raise SystemExit("self-test failed: missing %s" % ",".join(missing))
        print("resource_monitor self-test PASS samples=%d" % (len(lines) - 1))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-label")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--test-dir", type=Path)
    parser.add_argument("--process", action="append", type=parse_process, default=[])
    parser.add_argument("--pid", action="append", type=int, default=[],
                        help="shortcut for --process process,,PID; may be repeated")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        run_self_test()
        return
    if not args.host_label:
        parser.error("--host-label is required unless --self-test is used")
    if not args.output:
        parser.error("--output is required unless --self-test is used")
    if not args.test_dir:
        parser.error("--test-dir is required unless --self-test is used")
    if args.interval <= 0:
        parser.error("--interval must be positive")
    for pid in args.pid:
        args.process.append({"role": "process", "receiver_index": "", "pid": pid})

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    exists = args.output.exists()
    previous = {}
    with args.output.open("a", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        while not STOP:
            timestamp = utc_now()
            monotonic = time.monotonic()
            mem = read_meminfo()
            load_1, load_5, load_15 = read_loadavg()
            disk_free = shutil.disk_usage(args.test_dir).free
            test_bytes = directory_size(args.test_dir)
            for process in process_rows(args.process):
                pid = process["pid"]
                row = {
                    "timestamp": timestamp,
                    "host": args.host_label,
                    "role": process["role"],
                    "receiver_index": process["receiver_index"],
                    "pid": pid,
                    "rss_kb": "",
                    "pss_kb": "",
                    "cpu_percent": "",
                    "cpu_seconds": "",
                    "threads": "",
                    "fd_count": "",
                    "mem_total_kb": mem.get("MemTotal", 0),
                    "mem_available_kb": mem.get("MemAvailable", 0),
                    "swap_total_kb": mem.get("SwapTotal", 0),
                    "swap_free_kb": mem.get("SwapFree", 0),
                    "load_1": load_1,
                    "load_5": load_5,
                    "load_15": load_15,
                    "disk_free_bytes": disk_free,
                    "test_directory_bytes": test_bytes,
                }
                if pid:
                    try:
                        current = read_proc(pid)
                        prev = previous.get(pid)
                        cpu_percent = ""
                        if prev:
                            delta_cpu = current["cpu_seconds"] - prev["cpu_seconds"]
                            delta_time = monotonic - prev["monotonic"]
                            if delta_time > 0:
                                cpu_percent = round(delta_cpu / delta_time * 100.0, 6)
                        previous[pid] = {"cpu_seconds": current["cpu_seconds"],
                                         "monotonic": monotonic}
                        row.update(current)
                        row["cpu_percent"] = cpu_percent
                    except (FileNotFoundError, ProcessLookupError, PermissionError,
                            ValueError, IndexError, OSError) as error:
                        previous.pop(pid, None)
                        write_warning("pid=%d sample skipped: %s" % (pid, error))
                        continue
                writer.writerow(row)
            output.flush()
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
