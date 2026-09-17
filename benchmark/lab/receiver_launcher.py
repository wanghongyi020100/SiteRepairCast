#!/usr/bin/env python3
"""Start and supervise multiple receiver_agent processes on one receiver host."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

try:
    from common import shell_join, utc_now, write_json
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import shell_join, utc_now, write_json


STOP = False


def handle_signal(_signum, _frame):
    global STOP
    STOP = True


def stop_process(process):
    if not process or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 3
    while process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.05)
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def parse_indexes(text):
    result = []
    for item in text.split(","):
        item = item.strip()
        if item:
            result.append(int(item))
    return result


def completed_logs(processes):
    completed = 0
    for item in processes:
        try:
            item["handle"].flush()
            text = Path(item["log"]).read_text(encoding="utf-8", errors="replace")
        except OSError:
            text = ""
        if "COMPLETED output=" in text:
            completed += 1
    return completed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--proxy-ip", required=True)
    parser.add_argument("--interface-ip", required=True)
    parser.add_argument("--multicast-ip", required=True)
    parser.add_argument("--udp-port", required=True, type=int)
    parser.add_argument("--control-port", required=True, type=int)
    parser.add_argument("--receiver-indexes", required=True)
    parser.add_argument("--drop-blocks-json", required=True, type=Path)
    parser.add_argument("--monitor-interval", required=True, type=float)
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    indexes = parse_indexes(args.receiver_indexes)
    run_dir = args.run_dir
    log_dir = run_dir / "logs"
    receiver_dir = run_dir / "receivers"
    log_dir.mkdir(parents=True, exist_ok=True)
    receiver_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "ACTIVE").touch()
    (run_dir / "launcher.pid").write_text(str(os.getpid()) + "\n", encoding="utf-8")

    drops = json.loads(args.drop_blocks_json.read_text(encoding="utf-8"))
    binary = args.repo / "build" / "receiver_agent"
    if not binary.exists():
        raise SystemExit("receiver_agent not found: %s" % binary)

    processes = []
    monitor = None
    monitor_log = None
    transfer_status = "RUNNING"
    metric_collection_error = ""
    try:
        for receiver_index in indexes:
            output_dir = receiver_dir / ("receiver-%03d" % receiver_index)
            output_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / ("receiver-%03d.log" % receiver_index)
            command = [
                str(binary),
                "--multicast-ip", args.multicast_ip,
                "--udp-port", str(args.udp_port),
                "--interface-ip", args.interface_ip,
                "--proxy-ip", args.proxy_ip,
                "--control-port", str(args.control_port),
                "--output-dir", str(output_dir),
            ]
            env = os.environ.copy()
            env["SRCAST_DROP_INITIAL_BLOCKS"] = ",".join(
                str(value) for value in drops.get(str(receiver_index), []))
            handle = log_path.open("w", encoding="utf-8")
            process = subprocess.Popen(command, stdout=handle, stderr=subprocess.STDOUT,
                                       cwd=args.repo, env=env, text=True,
                                       start_new_session=True)
            processes.append({
                "index": receiver_index,
                "pid": process.pid,
                "command": command,
                "log": str(log_path),
                "output_dir": str(output_dir),
                "process": process,
                "handle": handle,
            })

        monitor_args = [
            "python3", str(args.repo / "benchmark" / "lab" / "resource_monitor.py"),
            "--host-label", args.interface_ip,
            "--output", str(run_dir / "resources.csv"),
            "--interval", str(args.monitor_interval),
            "--test-dir", str(args.repo / "test-runs" / "lab"),
        ]
        for item in processes:
            monitor_args.extend(["--process",
                                 "receiver,%03d,%d" % (item["index"], item["pid"])])
        monitor_log = (log_dir / "resource-monitor.log").open("w", encoding="utf-8")
        monitor = subprocess.Popen(monitor_args, stdout=monitor_log, stderr=subprocess.STDOUT,
                                   cwd=args.repo, text=True, start_new_session=True)
        write_json(run_dir / "process.json", {
            "time": utc_now(),
            "launcher_pid": os.getpid(),
            "monitor_pid": monitor.pid,
            "receivers": [
                {key: value for key, value in item.items()
                 if key not in ("process", "handle")}
                for item in processes
            ],
        })
        print("receiver_launcher started=%d indexes=%s" %
              (len(processes), ",".join(str(value) for value in indexes)), flush=True)

        while not STOP:
            if any(item["process"].poll() not in (None, 0) for item in processes):
                transfer_status = "FAILED"
                break
            time.sleep(0.5)
    finally:
        completed = completed_logs(processes)
        if transfer_status != "FAILED":
            transfer_status = "PASS" if completed == len(processes) else "STOPPED"
        metrics_status = "PASS"
        if monitor and monitor.poll() not in (None, 0):
            metrics_status = "FAILED"
            metric_collection_error = "resource_monitor exited with status %d" % monitor.returncode
        for item in reversed(processes):
            stop_process(item["process"])
            item["handle"].close()
        stop_process(monitor)
        try:
            monitor_log.close()
        except Exception:
            pass
        (run_dir / "ACTIVE").unlink(missing_ok=True)
        marker = "DONE" if transfer_status == "PASS" else "FAILED"
        (run_dir / marker).touch()
        write_json(run_dir / "launcher-result.json", {
            "time": utc_now(),
            "transfer_status": transfer_status,
            "completed_receivers": completed,
            "receiver_count": len(processes),
            "metrics_status": metrics_status,
            "metrics_valid": metrics_status == "PASS",
            "metric_collection_error": metric_collection_error,
        })
        print("receiver_launcher stopped", flush=True)


if __name__ == "__main__":
    main()
