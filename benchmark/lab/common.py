#!/usr/bin/env python3
"""Shared helpers for the SiteRepairCast lab benchmark controller."""

import csv
import hashlib
import json
import math
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]
LAB_ROOT = ROOT / "test-runs" / "lab"
HISTORY_DIR = LAB_ROOT / "history"
RUNS_DIR = LAB_ROOT / "runs"
DATA_DIR = LAB_ROOT / "data"


RUN_FIELDS = [
    "run_id", "mode", "receiver_count", "file_count", "total_file_bytes",
    "transfer_time_seconds", "proxy_avg_cpu_percent", "proxy_cpu_seconds",
    "proxy_peak_rss_mb", "receiver_total_peak_rss_mb",
    "receiver_max_process_rss_mb", "proxy_repair_payload_bytes",
    "proxy_repair_pread_blocks", "peer_blocks", "sha256_success", "status",
    "failure_reason",
]

SUMMARY_FIELDS = [
    "mode", "receiver_count", "runs", "success_runs", "success_rate",
    "transfer_time_median", "transfer_time_p95", "proxy_cpu_median",
    "proxy_peak_rss_median", "proxy_repair_payload_bytes_median",
    "proxy_repair_pread_blocks_median", "peer_blocks_median",
]


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def shell_join(values):
    return " ".join(shlex.quote(str(value)) for value in values)


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def atomic_write(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def write_json(path, value):
    atomic_write(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def append_jsonl(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(value, sort_keys=True) + "\n")


def run_command(argv, timeout=None, check=True, cwd=None):
    result = subprocess.run([str(value) for value in argv], text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=timeout, check=False, cwd=cwd)
    if check and result.returncode != 0:
        raise RuntimeError("command failed: %s\n%s" %
                           (shell_join(argv), result.stderr.strip()))
    return result


def ssh_command(host, script, ssh_options=None):
    options = ssh_options or ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
    return ["ssh", *options, host, "--", "bash", "-lc", shlex.quote(script)]


def ssh_run(host, script, ssh_options=None, timeout=30, check=True):
    return run_command(ssh_command(host, script, ssh_options), timeout=timeout, check=check)


def rsync_repo(host, repo, excludes):
    destination = "%s:%s/" % (host, repo)
    argv = ["rsync", "-az"]
    for item in excludes:
        argv.extend(["--exclude", item])
    argv.extend([str(ROOT) + "/", destination])
    return run_command(argv)


def split_receiver_indexes(count, host_count):
    groups = []
    first = 1
    for host_index in range(host_count):
        size = count // host_count + (1 if host_index < count % host_count else 0)
        groups.append(list(range(first, first + size)))
        first += size
    return groups


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def deterministic_file(path, size):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size == size:
        return
    pattern = bytearray()
    counter = 0
    while len(pattern) < 1024 * 1024:
        pattern.extend(hashlib.sha256(("srcast-lab:%d" % counter).encode()).digest())
        counter += 1
    pattern = bytes(pattern[:1024 * 1024])
    remaining = int(size)
    with path.open("wb") as output:
        while remaining:
            chunk = pattern[:min(remaining, len(pattern))]
            output.write(chunk)
            remaining -= len(chunk)


def loss_blocks(receiver_index, receiver_count, count, section_blocks):
    if count <= 0:
        return []
    if receiver_count * count > section_blocks - 17:
        raise ValueError("section_blocks is too small for disjoint sparse-loss sets")
    first = 17 + (receiver_index - 1) * count
    return list(range(first, first + count))


def validate_hosts(hosts):
    proxy = hosts.get("proxy")
    if not proxy or not all(key in proxy for key in ("host", "repo", "interface_ip")):
        raise ValueError("hosts.proxy requires host, repo, and interface_ip")
    receivers = hosts.get("receiver_hosts", [])
    if not receivers:
        raise ValueError("hosts.receiver_hosts must not be empty")
    for index, item in enumerate(receivers, 1):
        if not all(key in item for key in ("host", "repo", "interface_ip")):
            raise ValueError("receiver_hosts[%d] requires host, repo, and interface_ip" % index)


def validate_experiment(exp):
    required = [
        "name", "receiver_count", "repetitions", "mode", "files", "generated_files",
        "multicast_ip", "udp_port", "control_port", "central_port", "section_blocks",
        "udp_pace_us", "central_pace_us", "max_repair_rounds", "loss", "monitor",
        "retention", "continue_on_failure",
    ]
    for key in required:
        if key not in exp:
            raise ValueError("experiment.%s is required" % key)
    if exp["mode"] not in ("baseline", "peer"):
        raise ValueError("experiment.mode must be baseline or peer")
    for key in ("receiver_count", "repetitions", "udp_port", "control_port",
                "central_port", "section_blocks", "max_repair_rounds"):
        if int(exp[key]) <= 0:
            raise ValueError("experiment.%s must be positive" % key)
    if float(exp["monitor"].get("interval_seconds", 0)) <= 0:
        raise ValueError("monitor.interval_seconds must be positive")
    retention = exp["retention"]
    for key in ("max_test_directory_bytes", "min_free_disk_bytes", "keep_latest_runs"):
        if int(retention.get(key, -1)) < 0:
            raise ValueError("retention.%s must be non-negative" % key)


def safe_run_dir(run_id, root=RUNS_DIR):
    if not run_id or "/" in run_id or "\\" in run_id or run_id in {".", ".."}:
        raise ValueError("unsafe run_id: %s" % run_id)
    path = Path(root) / run_id
    resolved_root = Path(root).resolve()
    resolved = path.resolve()
    if resolved_root not in resolved.parents and resolved != resolved_root:
        raise ValueError("unsafe run directory: %s" % path)
    return path


def directory_size(path):
    total = 0
    path = Path(path)
    if not path.exists():
        return 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except OSError:
                pass
    return total


def cleanup_old_runs(retention, runs_dir=RUNS_DIR):
    runs_dir = Path(runs_dir)
    runs_dir.mkdir(parents=True, exist_ok=True)
    max_bytes = int(retention.get("max_test_directory_bytes", 0))
    min_free = int(retention.get("min_free_disk_bytes", 0))
    keep_latest = int(retention.get("keep_latest_runs", 0))
    keep_failed = bool(retention.get("keep_failed_runs", True))

    usage = shutil.disk_usage(runs_dir)
    if directory_size(runs_dir) <= max_bytes and usage.free >= min_free:
        return []

    run_dirs = [item for item in runs_dir.iterdir() if item.is_dir()]
    run_dirs.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    protected = set(run_dirs[:keep_latest])
    removed = []
    for candidate in reversed(run_dirs):
        if candidate in protected:
            continue
        if (candidate / "ACTIVE").exists():
            continue
        if keep_failed and (candidate / "FAILED").exists():
            continue
        resolved = candidate.resolve()
        root_resolved = runs_dir.resolve()
        if root_resolved not in resolved.parents:
            continue
        shutil.rmtree(candidate)
        removed.append(str(candidate))
        usage = shutil.disk_usage(runs_dir)
        if directory_size(runs_dir) <= max_bytes and usage.free >= min_free:
            break
    return removed


def percentile(values, fraction):
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def upsert_runs_csv(row):
    path = HISTORY_DIR / "runs.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    if path.exists():
        rows = list(csv.DictReader(path.open(encoding="utf-8")))
    latest_by_run = {}
    order = []
    for existing in rows:
        run_id = existing.get("run_id")
        if run_id and run_id not in latest_by_run:
            order.append(run_id)
        if run_id:
            latest_by_run[run_id] = existing
    run_id = row.get("run_id")
    if run_id and run_id not in latest_by_run:
        order.append(run_id)
    if run_id:
        latest_by_run[run_id] = {key: row.get(key, "") for key in RUN_FIELDS}
    rows = [latest_by_run[run_id] for run_id in order if run_id in latest_by_run]
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=RUN_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def append_runs_csv(row):
    upsert_runs_csv(row)


def rewrite_summary_csv():
    runs_path = HISTORY_DIR / "runs.csv"
    summary_path = HISTORY_DIR / "summary.csv"
    if not runs_path.exists():
        return
    rows = list(csv.DictReader(runs_path.open(encoding="utf-8")))
    groups = {}
    for row in rows:
        key = (row["mode"], row["receiver_count"])
        groups.setdefault(key, []).append(row)
    with summary_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        for (mode, receiver_count), selected in sorted(groups.items()):
            successes = [row for row in selected if row.get("status") == "PASS"]
            def floats(field):
                values = []
                for row in successes:
                    value = row.get(field)
                    if value in (None, ""):
                        continue
                    try:
                        values.append(float(value))
                    except ValueError:
                        continue
                return values
            def median(field):
                values = floats(field)
                return round(percentile(values, 0.5), 6) if values else ""
            def p95(field):
                values = floats(field)
                return round(percentile(values, 0.95), 6) if values else ""
            writer.writerow({
                "mode": mode,
                "receiver_count": receiver_count,
                "runs": len(selected),
                "success_runs": len(successes),
                "success_rate": round(len(successes) / len(selected), 6) if selected else 0,
                "transfer_time_median": median("transfer_time_seconds"),
                "transfer_time_p95": p95("transfer_time_seconds"),
                "proxy_cpu_median": median("proxy_avg_cpu_percent"),
                "proxy_peak_rss_median": median("proxy_peak_rss_mb"),
                "proxy_repair_payload_bytes_median": median("proxy_repair_payload_bytes"),
                "proxy_repair_pread_blocks_median": median("proxy_repair_pread_blocks"),
                "peer_blocks_median": median("peer_blocks"),
            })
