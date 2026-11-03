#!/usr/bin/env python3
"""Local Central workload runner for lab benchmark runs."""

import argparse
import json
from pathlib import Path
import re
import socket
import subprocess
import sys
import time

try:
    from common import (
        DATA_DIR, HISTORY_DIR, RUNS_DIR, append_jsonl, deterministic_file,
        load_json, safe_run_dir, sha256_file, shell_join, utc_now,
        validate_experiment, validate_hosts, write_json,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import (
        DATA_DIR, HISTORY_DIR, RUNS_DIR, append_jsonl, deterministic_file,
        load_json, safe_run_dir, sha256_file, shell_join, utc_now,
        validate_experiment, validate_hosts, write_json,
    )


def wait_port(host, port, timeout=30):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, int(port)), timeout=2):
                return
        except OSError as error:
            last_error = error
            time.sleep(0.25)
    raise TimeoutError("proxy central port is not connectable: %s:%s (%s)" %
                       (host, port, last_error))


def prepared_files(experiment):
    files = [Path(value) for value in experiment.get("files", [])]
    generated = []
    for index, spec in enumerate(experiment.get("generated_files", []), 1):
        size = int(spec["size_bytes"])
        count = int(spec.get("count", 1))
        for item in range(1, count + 1):
            path = DATA_DIR / ("%s-generated-%02d-%02d.bin" %
                               (experiment["name"], index, item))
            deterministic_file(path, size)
            generated.append(path)
    all_files = files + generated
    for path in all_files:
        if not path.exists():
            raise FileNotFoundError(path)
    return all_files


def parse_proxy_metrics(text):
    totals = {
        "proxy_repair_payload_bytes": 0,
        "proxy_repair_pread_blocks": 0,
        "peer_blocks": 0,
    }
    for line in text.splitlines():
        match = re.search(r"\bREPAIR_METRIC\s+(.*)$", line)
        if not match:
            continue
        values = dict(re.findall(r"([a-z_]+)=([a-z]+|[0-9]+)", match.group(1)))
        for key in totals:
            totals[key] += int(values.get(key, 0))
    return totals


def summarize_resources(path):
    import csv
    proxy_peak = 0.0
    proxy_cpu_values = []
    proxy_cpu_seconds = None
    min_available = None
    process_samples = 0
    if not Path(path).exists():
        return {
            "metrics_valid": False,
            "metric_collection_error": "resources.csv not found",
            "proxy_peak_rss_mb": None,
            "proxy_avg_cpu_percent": None,
            "proxy_cpu_seconds": None,
            "minimum_mem_available_mb": None,
        }
    with Path(path).open(encoding="utf-8") as source:
        for row in csv.DictReader(source):
            if row.get("role") == "proxy":
                process_samples += 1
                if row.get("rss_kb"):
                    proxy_peak = max(proxy_peak, int(row["rss_kb"]) / 1024.0)
                if row.get("cpu_percent"):
                    proxy_cpu_values.append(float(row["cpu_percent"]))
                if row.get("cpu_seconds"):
                    value = float(row["cpu_seconds"])
                    proxy_cpu_seconds = value if proxy_cpu_seconds is None else max(proxy_cpu_seconds, value)
            if row.get("mem_available_kb"):
                value = int(row["mem_available_kb"]) / 1024.0
                min_available = value if min_available is None else min(min_available, value)
    metrics_valid = process_samples > 0 and proxy_peak > 0 and proxy_cpu_seconds is not None
    return {
        "metrics_valid": metrics_valid,
        "metric_collection_error": "" if metrics_valid else "no proxy process samples",
        "proxy_peak_rss_mb": round(proxy_peak, 6) if metrics_valid else None,
        "proxy_avg_cpu_percent": round(
            sum(proxy_cpu_values) / len(proxy_cpu_values), 6) if proxy_cpu_values else None,
        "proxy_cpu_seconds": round(proxy_cpu_seconds, 6) if proxy_cpu_seconds is not None else None,
        "minimum_mem_available_mb": round(min_available, 6) if min_available is not None else None,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hosts", default=Path("benchmark/lab_configs/hosts.json"), type=Path)
    parser.add_argument("--experiment", default=Path("benchmark/lab_configs/experiment.json"),
                        type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()

    hosts = load_json(args.hosts)
    experiment = load_json(args.experiment)
    validate_hosts(hosts)
    validate_experiment(experiment)

    current_path = RUNS_DIR.parent / "current.json"
    run_id = args.run_id
    if not run_id and current_path.exists():
        run_id = load_json(current_path).get("run_id")
    if not run_id:
        run_id = "%s-%03d-r001" % (experiment["mode"], int(experiment["receiver_count"]))
    run_dir = safe_run_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    files = prepared_files(experiment)
    file_shas = {str(path): sha256_file(path) for path in files}
    total_bytes = sum(path.stat().st_size for path in files)
    proxy = hosts["proxy"]
    connect_ip = proxy.get("central_connect_ip", proxy["interface_ip"])
    binary = Path("build") / "central_sender"
    if not binary.exists():
        raise SystemExit("central_sender not found: %s" % binary)

    append_jsonl(HISTORY_DIR / "events.jsonl",
                 {"time": utc_now(), "run_id": run_id, "event": "CENTRAL_START"})
    status = "FAIL"
    failure_reason = ""
    started = time.monotonic()
    central_return = None
    try:
        wait_port(connect_ip, int(experiment["central_port"]))
        for repeat in range(1, int(experiment["repetitions"]) + 1):
            log_path = run_dir / ("central-r%03d.log" % repeat)
            command = [
                str(binary),
                "--proxy", connect_ip, str(experiment["central_port"]),
                "--pace-us", str(experiment["central_pace_us"]),
                "--section-blocks", str(experiment["section_blocks"]),
            ]
            for path in files:
                command.extend(["--file", str(path)])
            with log_path.open("w", encoding="utf-8") as output:
                output.write("command=%s\n" % shell_join(command))
                output.flush()
                process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                           text=True)
                central_return = process.wait(timeout=args.timeout)
            if central_return != 0:
                raise RuntimeError("central_sender exited with status %d" % central_return)
        status = "PASS"
    except subprocess.TimeoutExpired:
        status = "TIMEOUT"
        failure_reason = "central_sender timed out"
    except KeyboardInterrupt:
        status = "INTERRUPTED"
        failure_reason = "central_workload interrupted"
        raise
    except Exception as error:
        failure_reason = str(error)
        if experiment.get("continue_on_failure", False):
            print(failure_reason, file=sys.stderr)
        else:
            raise
    finally:
        elapsed = time.monotonic() - started
        proxy_log = run_dir / "proxy.log"
        metrics = parse_proxy_metrics(proxy_log.read_text(
            encoding="utf-8", errors="replace") if proxy_log.exists() else "")
        resources = summarize_resources(run_dir / "resources.csv")
        row = {
            "run_id": run_id,
            "mode": experiment["mode"],
            "receiver_count": experiment["receiver_count"],
            "file_count": len(files),
            "total_file_bytes": total_bytes,
            "transfer_time_seconds": round(elapsed, 6),
            "proxy_avg_cpu_percent": resources["proxy_avg_cpu_percent"],
            "proxy_cpu_seconds": resources["proxy_cpu_seconds"],
            "proxy_peak_rss_mb": resources["proxy_peak_rss_mb"],
            "receiver_total_peak_rss_mb": "",
            "receiver_max_process_rss_mb": "",
            "proxy_repair_payload_bytes": metrics["proxy_repair_payload_bytes"],
            "proxy_repair_pread_blocks": metrics["proxy_repair_pread_blocks"],
            "peer_blocks": metrics["peer_blocks"],
            "sha256_success": status == "PASS",
            "status": status,
            "failure_reason": failure_reason,
        }
        write_json(run_dir / "result.json", {
            "time": utc_now(),
            "run_id": run_id,
            "status": status,
            "failure_reason": failure_reason,
            "transfer_time_seconds": round(elapsed, 6),
            "central_return": central_return,
            "files": [{"path": str(path), "sha256": file_shas[str(path)]} for path in files],
            "metrics": metrics,
            "resources": resources,
        })
        append_jsonl(HISTORY_DIR / "events.jsonl",
                     {"time": utc_now(), "run_id": run_id,
                      "event": "CENTRAL_DONE", "status": status})
        write_json(run_dir / "central-result.json", row)
        print("run_id=%s status=%s result=%s" % (run_id, status, run_dir / "result.json"))
        if status not in ("PASS",) and not experiment.get("continue_on_failure", False):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
