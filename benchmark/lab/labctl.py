#!/usr/bin/env python3
"""Control entry point for split SiteRepairCast lab benchmarks."""

import argparse
import json
from pathlib import Path
import csv
import re
import sys
import time

try:
    from common import (
        DATA_DIR, HISTORY_DIR, LAB_ROOT, RUNS_DIR, append_jsonl, append_runs_csv,
        deterministic_file, load_json, rewrite_summary_csv, rsync_repo, safe_run_dir,
        sha256_file, shell_join, ssh_run, utc_now, validate_experiment, validate_hosts,
        write_json,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import (
        DATA_DIR, HISTORY_DIR, LAB_ROOT, RUNS_DIR, append_jsonl, append_runs_csv,
        deterministic_file, load_json, rewrite_summary_csv, rsync_repo, safe_run_dir,
        sha256_file, shell_join, ssh_run, utc_now, validate_experiment, validate_hosts,
        write_json,
    )


DEFAULT_HOSTS = Path("benchmark/lab_configs/hosts.json")
DEFAULT_EXPERIMENT = Path("benchmark/lab_configs/experiment.json")
EXCLUDES = ["/build/", "/proxy-cache/", "/received/", "/artifacts/", "/config/"]


def load_configs(args):
    hosts = load_json(args.hosts)
    experiment = load_json(args.experiment)
    validate_hosts(hosts)
    validate_experiment(experiment)
    return hosts, experiment


def next_run_id(experiment):
    base = "%s-%03d-r" % (experiment["mode"], int(experiment["receiver_count"]))
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    for repeat in range(1, 1000):
        run_id = "%s%03d" % (base, repeat)
        if not (RUNS_DIR / run_id).exists():
            return run_id
    raise RuntimeError("could not allocate run_id")


def command_deploy(args):
    hosts, _ = load_configs(args)
    targets = [hosts["proxy"]] + hosts["receiver_hosts"]
    for target in targets:
        print("[%s] deploy %s:%s" % (utc_now(), target["host"], target["repo"]), flush=True)
        rsync_repo(target["host"], target["repo"], EXCLUDES)
    print("deploy complete")


def command_start(args):
    hosts, experiment = load_configs(args)
    run_id = args.run_id or next_run_id(experiment)
    proxy = hosts["proxy"]
    local_run_dir = safe_run_dir(run_id)
    local_run_dir.mkdir(parents=True, exist_ok=True)
    write_json(LAB_ROOT / "current.json", {
        "time": utc_now(),
        "run_id": run_id,
        "proxy": proxy,
        "experiment": str(args.experiment),
        "hosts": str(args.hosts),
    })
    remote_run_dir = Path(proxy["repo"]) / "test-runs" / "lab" / "runs" / run_id
    remote_log = remote_run_dir / "controller.log"
    command = [
        "python3", str(Path(proxy["repo"]) / "benchmark" / "lab" / "proxy_controller.py"),
        "--hosts", str(Path(proxy["repo"]) / args.hosts),
        "--experiment", str(Path(proxy["repo"]) / args.experiment),
        "--run-id", run_id,
        "--repo", proxy["repo"],
    ]
    script = (
        "mkdir -p {run_dir}; "
        "setsid {cmd} >{log} 2>&1 < /dev/null & echo $! > {pidfile}; cat {pidfile}"
    ).format(
        run_dir=shell_join([str(remote_run_dir)]),
        cmd=shell_join(command),
        log=shell_join([str(remote_log)]),
        pidfile=shell_join([str(remote_run_dir / "controller.pid")]),
    )
    result = ssh_run(proxy["host"], script, timeout=30)
    pid = int(result.stdout.strip().splitlines()[-1])
    write_json(local_run_dir / "process.json", {"run_id": run_id, "proxy_controller_pid": pid})
    append_jsonl(HISTORY_DIR / "events.jsonl",
                 {"time": utc_now(), "run_id": run_id, "event": "LABCTL_START",
                  "proxy_controller_pid": pid})
    print("run_id=%s proxy_controller_pid=%d" % (run_id, pid))
    print("status: python3 benchmark/lab/labctl.py status")


def current_run(args):
    if args.run_id:
        return args.run_id
    current = LAB_ROOT / "current.json"
    if not current.exists():
        raise RuntimeError("no current lab run; pass --run-id")
    return load_json(current)["run_id"]


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


def summarize_proxy_resources(path):
    if not Path(path).exists():
        return {
            "metrics_valid": False,
            "metric_collection_error": "proxy resources.csv missing",
            "proxy_peak_rss_mb": None,
            "proxy_avg_cpu_percent": None,
            "proxy_cpu_seconds": None,
        }
    process_samples = 0
    proxy_peak = 0.0
    cpu_values = []
    cpu_seconds = None
    with Path(path).open(encoding="utf-8") as source:
        for row in csv.DictReader(source):
            if row.get("role") != "proxy":
                continue
            process_samples += 1
            if row.get("rss_kb"):
                proxy_peak = max(proxy_peak, int(row["rss_kb"]) / 1024.0)
            if row.get("cpu_percent"):
                cpu_values.append(float(row["cpu_percent"]))
            if row.get("cpu_seconds"):
                value = float(row["cpu_seconds"])
                cpu_seconds = value if cpu_seconds is None else max(cpu_seconds, value)
    valid = process_samples > 0 and proxy_peak > 0 and cpu_seconds is not None
    return {
        "metrics_valid": valid,
        "metric_collection_error": "" if valid else "no proxy process samples",
        "proxy_peak_rss_mb": round(proxy_peak, 6) if valid else None,
        "proxy_avg_cpu_percent": round(sum(cpu_values) / len(cpu_values), 6) if cpu_values else None,
        "proxy_cpu_seconds": round(cpu_seconds, 6) if cpu_seconds is not None else None,
    }


def command_status(args):
    hosts, _ = load_configs(args)
    run_id = current_run(args)
    proxy = hosts["proxy"]
    remote_run_dir = Path(proxy["repo"]) / "test-runs" / "lab" / "runs" / run_id
    local_result = safe_run_dir(run_id) / "result.json"
    local_lifecycle = ""
    if local_result.exists():
        try:
            result = json.loads(local_result.read_text(encoding="utf-8"))
            local_lifecycle = "local_lifecycle=COLLECTED local_status=%s" % result.get("status")
        except json.JSONDecodeError:
            local_lifecycle = "local_lifecycle=COLLECTED local_status=unreadable"
    script = (
        "echo run_id={run_id}; "
        "if test -f {pidfile}; then "
        "pid=$(cat {pidfile}); echo controller_pid=$pid; "
        "kill -0 $pid 2>/dev/null && echo controller_alive=true || echo controller_alive=false; "
        "else echo controller_alive=false; fi; "
        "if test -f {process}; then "
        "python3 - <<'PY'\n"
        "import json, os\n"
        "p=json.load(open({process_py!r}))\n"
        "for key in ('proxy_pid','proxy_monitor_pid'):\n"
        "    pid=p.get(key)\n"
        "    if pid:\n"
        "        alive=True\n"
        "        try: os.kill(int(pid),0)\n"
        "        except OSError: alive=False\n"
        "        print('%s=%s alive=%s' % (key,pid,str(alive).lower()))\n"
        "for item in p.get('receiver_launchers',[]):\n"
        "    print('receiver_launcher host=%s pid=%s indexes=%s' % (item.get('host'), item.get('pid'), item.get('indexes')))\n"
        "PY\n"
        "fi; "
        "test -f {status} && cat {status} || true; "
        "test -f {done} && echo lifecycle=COMPLETED || true; "
        "test -f {failed} && echo lifecycle=FAILED || true; "
        "test -f {active} && test -f {status} && echo lifecycle=READY_FOR_CENTRAL || true; "
        "test -f {active} || test -f {done} || test -f {failed} || echo lifecycle=STOPPED; "
        "test -f {log} && tail -n 20 {log} || true"
    ).format(
        run_id=run_id,
        pidfile=shell_join([str(remote_run_dir / "controller.pid")]),
        process=shell_join([str(remote_run_dir / "process.json")]),
        process_py=str(remote_run_dir / "process.json"),
        status=shell_join([str(remote_run_dir / "status.json")]),
        active=shell_join([str(remote_run_dir / "ACTIVE")]),
        done=shell_join([str(remote_run_dir / "DONE")]),
        failed=shell_join([str(remote_run_dir / "FAILED")]),
        log=shell_join([str(remote_run_dir / "controller.log")]),
    )
    result = ssh_run(proxy["host"], script, timeout=30, check=False)
    if local_lifecycle:
        print(local_lifecycle)
    print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)
    for host_index, receiver in enumerate(hosts["receiver_hosts"], 1):
        remote_run_dir = Path(receiver["repo"]) / "test-runs" / "lab" / "runs" / run_id
        script = (
            "echo receiver_host_{host_index}={host}; "
            "if test -f {launcher}; then "
            "pid=$(cat {launcher}); "
            "kill -0 $pid 2>/dev/null && echo launcher_pid=$pid alive=true || echo launcher_pid=$pid alive=false; "
            "fi; "
            "if test -f {process}; then "
            "python3 - <<'PY'\n"
            "import json, os\n"
            "p=json.load(open({process_py!r}))\n"
            "for item in p.get('receivers',[]):\n"
            "    pid=item.get('pid')\n"
            "    alive=True\n"
            "    try: os.kill(int(pid),0)\n"
            "    except OSError: alive=False\n"
            "    print('receiver_agent index=%03d pid=%s alive=%s' % (item.get('index'), pid, str(alive).lower()))\n"
            "print('receiver_monitor_pid=%s' % p.get('monitor_pid'))\n"
            "PY\n"
            "fi; "
            "test -f {result} && cat {result} || true; "
            "test -f {done} && echo receiver_lifecycle=DONE || true; "
            "test -f {failed} && echo receiver_lifecycle=FAILED || true; "
            "test -f {active} && echo receiver_lifecycle=ACTIVE || true"
        ).format(
            host_index=host_index,
            host=receiver["host"],
            launcher=shell_join([str(remote_run_dir / "launcher.pid")]),
            process=shell_join([str(remote_run_dir / "process.json")]),
            process_py=str(remote_run_dir / "process.json"),
            result=shell_join([str(remote_run_dir / "launcher-result.json")]),
            done=shell_join([str(remote_run_dir / "DONE")]),
            failed=shell_join([str(remote_run_dir / "FAILED")]),
            active=shell_join([str(remote_run_dir / "ACTIVE")]),
        )
        receiver_status = ssh_run(receiver["host"], script, timeout=30, check=False)
        print(receiver_status.stdout.rstrip())
        if receiver_status.stderr:
            print(receiver_status.stderr.rstrip(), file=sys.stderr)


def command_stop(args):
    hosts, _ = load_configs(args)
    run_id = current_run(args)

    proxy = hosts["proxy"]
    receivers = hosts["receiver_hosts"]

    remote_run_dir = (
        Path(proxy["repo"]) /
        "test-runs" / "lab" / "runs" / run_id
    )

    # First ask proxy_controller to stop normally.
    #
    # proxy_controller's finally block may need to stop:
    #   - multiple remote receiver launchers
    #   - proxy resource monitor
    #   - site_proxy_sender
    #
    # The old 6 second timeout was too short and could SIGKILL the
    # controller before its finally block finished, leaving an orphan
    # site_proxy_sender listening on ports 6000/7000.
    script = (
        "if test -f {pidfile}; then "
        "pid=$(cat {pidfile}); "
        "kill -TERM -- -$pid 2>/dev/null || "
        "kill -TERM $pid 2>/dev/null || true; "
        "for i in $(seq 1 300); do "
        "kill -0 $pid 2>/dev/null || exit 0; "
        "sleep 0.1; "
        "done; "
        "kill -KILL -- -$pid 2>/dev/null || "
        "kill -KILL $pid 2>/dev/null || true; "
        "fi"
    ).format(
        pidfile=shell_join(
            [str(remote_run_dir / "controller.pid")]
        )
    )

    ssh_run(
        proxy["host"],
        script,
        timeout=40,
        check=False,
    )

    # Defensive fallback:
    # if the controller itself was killed/stuck, remove only the
    # site_proxy_sender belonging to this run.
    proxy_pattern = (
        "[s]ite_proxy_sender.*"
        + str(remote_run_dir / "proxy-cache")
    )

    proxy_cleanup = (
        "pkill -TERM -f -- {pattern} 2>/dev/null || true; "
        "sleep 2; "
        "pkill -KILL -f -- {pattern} 2>/dev/null || true"
    ).format(
        pattern=shell_join([proxy_pattern])
    )

    ssh_run(
        proxy["host"],
        proxy_cleanup,
        timeout=15,
        check=False,
    )

    # Defensive fallback for receiver side as well.
    # Match this run only, never all receiver_agent processes globally.
    for receiver in receivers:
        receiver_run_dir = (
            Path(receiver["repo"]) /
            "test-runs" / "lab" / "runs" / run_id
        )

        launcher_pattern = (
            "[r]eceiver_launcher.py.*--run-dir.*"
            + str(receiver_run_dir)
        )

        agent_pattern = (
            "/build/[r]eceiver_agent.*--output-dir.*"
            + str(receiver_run_dir / "receivers")
        )

        receiver_cleanup = (
            "pkill -TERM -f -- {launcher} 2>/dev/null || true; "
            "pkill -TERM -f -- {agent} 2>/dev/null || true; "
            "sleep 2; "
            "pkill -KILL -f -- {launcher} 2>/dev/null || true; "
            "pkill -KILL -f -- {agent} 2>/dev/null || true"
        ).format(
            launcher=shell_join([launcher_pattern]),
            agent=shell_join([agent_pattern]),
        )

        ssh_run(
            receiver["host"],
            receiver_cleanup,
            timeout=15,
            check=False,
        )

    append_jsonl(
        HISTORY_DIR / "events.jsonl",
        {
            "time": utc_now(),
            "run_id": run_id,
            "event": "LABCTL_STOP",
        },
    )

    print("stop completed for run_id=%s" % run_id)


def command_run_central(args):
    command = [
        "python3", "benchmark/lab/central_workload.py",
        "--hosts", str(args.hosts),
        "--experiment", str(args.experiment),
    ]
    if args.run_id:
        command.extend(["--run-id", args.run_id])
    import subprocess
    result = subprocess.run(command, text=True)
    raise SystemExit(result.returncode)


def command_collect(args):
    hosts, experiment = load_configs(args)
    run_id = current_run(args)
    local_run_dir = safe_run_dir(run_id)
    local_run_dir.mkdir(parents=True, exist_ok=True)
    targets = [("proxy", hosts["proxy"])] + [
        ("receiver-host-%d" % (index + 1), item)
        for index, item in enumerate(hosts["receiver_hosts"])
    ]
    import subprocess
    for label, target in targets:
        source = "%s:%s/" % (
            target["host"], Path(target["repo"]) / "test-runs" / "lab" / "runs" / run_id)
        destination = local_run_dir / label
        destination.mkdir(parents=True, exist_ok=True)
        command = ["rsync", "-az", source, str(destination) + "/"]
        print("[%s] collect %s" % (utc_now(), label), flush=True)
        subprocess.run(command, check=False)
    files = [Path(value) for value in experiment.get("files", [])]
    for spec_index, spec in enumerate(experiment.get("generated_files", []), 1):
        for item in range(1, int(spec.get("count", 1)) + 1):
            path = DATA_DIR / ("%s-generated-%02d-%02d.bin" %
                               (experiment["name"], spec_index, item))
            deterministic_file(path, int(spec["size_bytes"]))
            files.append(path)
    source_shas = {path.name: sha256_file(path) for path in files if path.exists()}
    completed = 0
    sha_success = True
    receiver_peaks = {}
    min_available = None
    metrics_valid = True
    metric_errors = []
    for host_index, receiver in enumerate(hosts["receiver_hosts"], 1):
        base = local_run_dir / ("receiver-host-%d" % host_index)
        for log_path in (base / "logs").glob("receiver-*.log"):
            text = log_path.read_text(encoding="utf-8", errors="replace")
            if "COMPLETED output=" in text:
                completed += 1
        for receiver_dir in (base / "receivers").glob("receiver-*"):
            for name, expected in source_shas.items():
                output = receiver_dir / name
                if not output.exists() or sha256_file(output) != expected:
                    sha_success = False
        resources = base / "resources.csv"
        if resources.exists():
            data_rows = 0
            with resources.open(encoding="utf-8") as source:
                for row in csv.DictReader(source):
                    data_rows += 1
                    if row.get("rss_kb"):
                        rss = int(row["rss_kb"]) / 1024.0
                        key = (host_index, row.get("pid"), row.get("receiver_index"))
                        receiver_peaks[key] = max(receiver_peaks.get(key, 0.0), rss)
                    if row.get("mem_available_kb"):
                        available = int(row["mem_available_kb"]) / 1024.0
                        min_available = available if min_available is None else min(min_available, available)
            if data_rows == 0:
                metrics_valid = False
                metric_errors.append("receiver-host-%d resources.csv has no samples" % host_index)
        else:
            metrics_valid = False
            metric_errors.append("receiver-host-%d resources.csv missing" % host_index)
        launcher_result = base / "launcher-result.json"
        if launcher_result.exists():
            launcher = json.loads(launcher_result.read_text(encoding="utf-8"))
            if launcher.get("metrics_valid") is False:
                metrics_valid = False
                metric_errors.append("receiver-host-%d: %s" %
                                     (host_index, launcher.get("metric_collection_error", "metrics invalid")))
    proxy_resources = local_run_dir / "proxy" / "resources.csv"
    proxy_metrics = summarize_proxy_resources(proxy_resources)
    if not proxy_metrics["metrics_valid"]:
        metrics_valid = False
        metric_errors.append(proxy_metrics["metric_collection_error"])
    if not receiver_peaks:
        metrics_valid = False
        metric_errors.append("no receiver process samples")
    proxy_log = local_run_dir / "proxy" / "proxy.log"
    metrics = parse_proxy_metrics(proxy_log.read_text(
        encoding="utf-8", errors="replace") if proxy_log.exists() else "")
    transfer_status = "PASS" if completed >= int(experiment["receiver_count"]) and sha_success else "FAIL"
    benchmark_status = "PASS" if transfer_status == "PASS" and metrics_valid else (
        "INVALID" if transfer_status == "PASS" else "FAIL")
    result_path = local_run_dir / "result.json"
    result = {}
    if result_path.exists():
        result = json.loads(result_path.read_text(encoding="utf-8"))
    central_result_path = local_run_dir / "central-result.json"
    if central_result_path.exists():
        central_result = json.loads(central_result_path.read_text(encoding="utf-8"))
        if "transfer_time_seconds" not in result or result.get("transfer_time_seconds") in ("", None):
            result["transfer_time_seconds"] = central_result.get("transfer_time_seconds", "")
    result.update({
        "time": utc_now(),
        "run_id": run_id,
        "status": benchmark_status,
        "transfer_status": transfer_status,
        "benchmark_status": benchmark_status,
        "metrics_valid": metrics_valid,
        "metric_collection_error": "; ".join(metric_errors),
        "sha256_success": sha_success,
        "completed_receivers": completed,
        "receiver_count": experiment["receiver_count"],
        "transfer_time_seconds": result.get("transfer_time_seconds", ""),
        "receiver_total_peak_rss_mb": round(sum(receiver_peaks.values()), 6) if receiver_peaks else None,
        "receiver_max_process_rss_mb": round(max(receiver_peaks.values()), 6) if receiver_peaks else None,
        "minimum_mem_available_mb": round(min_available, 6) if min_available is not None else None,
        "metrics": metrics,
        "resources": proxy_metrics,
    })
    write_json(result_path, result)
    append_runs_csv({
        "run_id": run_id,
        "mode": experiment["mode"],
        "receiver_count": experiment["receiver_count"],
        "file_count": len(files),
        "total_file_bytes": sum(path.stat().st_size for path in files if path.exists()),
        "transfer_time_seconds": result.get("transfer_time_seconds", ""),
        "proxy_avg_cpu_percent": proxy_metrics.get("proxy_avg_cpu_percent") if metrics_valid else "",
        "proxy_cpu_seconds": proxy_metrics.get("proxy_cpu_seconds") if metrics_valid else "",
        "proxy_peak_rss_mb": proxy_metrics.get("proxy_peak_rss_mb") if metrics_valid else "",
        "receiver_total_peak_rss_mb": result["receiver_total_peak_rss_mb"] if metrics_valid else "",
        "receiver_max_process_rss_mb": result["receiver_max_process_rss_mb"] if metrics_valid else "",
        "proxy_repair_payload_bytes": metrics.get("proxy_repair_payload_bytes", ""),
        "proxy_repair_pread_blocks": metrics.get("proxy_repair_pread_blocks", ""),
        "peer_blocks": metrics.get("peer_blocks", ""),
        "sha256_success": sha_success,
        "status": benchmark_status,
        "failure_reason": "" if benchmark_status == "PASS" else (
            "; ".join(metric_errors) if transfer_status == "PASS" else "receiver completion/SHA check failed"),
    })
    rewrite_summary_csv()
    append_jsonl(HISTORY_DIR / "events.jsonl",
                 {"time": utc_now(), "run_id": run_id, "event": "COLLECT",
                  "status": benchmark_status, "completed": completed})
    print("collected run_id=%s into %s" % (run_id, local_run_dir))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hosts", default=DEFAULT_HOSTS, type=Path)
    parser.add_argument("--experiment", default=DEFAULT_EXPERIMENT, type=Path)
    parser.add_argument("--run-id")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("deploy", "start", "status", "stop", "run-central", "collect"):
        child = subparsers.add_parser(name)
        child.add_argument("--run-id", default=argparse.SUPPRESS)
    args = parser.parse_args()
    {
        "deploy": command_deploy,
        "start": command_start,
        "status": command_status,
        "stop": command_stop,
        "run-central": command_run_central,
        "collect": command_collect,
    }[args.command](args)


if __name__ == "__main__":
    main()
