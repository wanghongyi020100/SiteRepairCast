#!/usr/bin/env python3
"""Proxy-side controller for SiteRepairCast lab runs."""

import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

try:
    from common import (
        append_jsonl, cleanup_old_runs, load_json, loss_blocks, safe_run_dir,
        shell_join, split_receiver_indexes, ssh_run, utc_now, validate_experiment,
        validate_hosts, write_json,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import (
        append_jsonl, cleanup_old_runs, load_json, loss_blocks, safe_run_dir,
        shell_join, split_receiver_indexes, ssh_run, utc_now, validate_experiment,
        validate_hosts, write_json,
    )


STOP = False


def handle_signal(_signum, _frame):
    global STOP
    STOP = True


def stop_process(process):
    if not process:
        return

    if process.poll() is not None:
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        process.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        pass

    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass

    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def remote_stop(host, pid):
    if not pid:
        return
    script = (
        "kill -TERM -- -{pid} 2>/dev/null || kill -TERM {pid} 2>/dev/null || true; "
        "for i in $(seq 1 50); do kill -0 {pid} 2>/dev/null || exit 0; sleep 0.1; done; "
        "kill -KILL -- -{pid} 2>/dev/null || kill -KILL {pid} 2>/dev/null || true"
    ).format(pid=int(pid))
    ssh_run(host, script, check=False, timeout=15)


def start_remote_launcher(receiver, command, log_path):
    script = "mkdir -p {log_dir}; setsid {cmd} >{log} 2>&1 < /dev/null & echo $!".format(
        log_dir=shell_join([str(Path(log_path).parent)]),
        cmd=shell_join(command),
        log=shell_join([str(log_path)]),
    )
    result = ssh_run(receiver["host"], script, timeout=30)
    return int(result.stdout.strip().splitlines()[-1])


def registrations(proxy_log):
    text = Path(proxy_log).read_text(encoding="utf-8", errors="replace") if Path(proxy_log).exists() else ""
    endpoints = set(re.findall(r"\bregistered receiver=([0-9.]+:[0-9]+)", text))
    return text, len(endpoints)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hosts", required=True, type=Path)
    parser.add_argument("--experiment", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--repo", required=True, type=Path)
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    hosts = load_json(args.hosts)
    experiment = load_json(args.experiment)
    validate_hosts(hosts)
    validate_experiment(experiment)

    proxy = hosts["proxy"]
    receivers = hosts["receiver_hosts"]
    run_dir = safe_run_dir(args.run_id, args.repo / "test-runs" / "lab" / "runs")
    history_dir = args.repo / "test-runs" / "lab" / "history"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "ACTIVE").touch()
    (run_dir / "controller.pid").write_text(str(os.getpid()) + "\n", encoding="utf-8")
    write_json(run_dir / "status.json", {
        "time": utc_now(), "run_id": args.run_id, "lifecycle": "STARTING",
        "ready": False, "registered": 0, "receiver_count": experiment["receiver_count"],
    })
    append_jsonl(history_dir / "events.jsonl",
                 {"time": utc_now(), "run_id": args.run_id, "event": "START"})

    proxy_process = None
    proxy_monitor = None
    proxy_log_handle = None
    monitor_log_handle = None
    remote_launchers = []
    status = "FAIL"
    failure_reason = ""
    try:
        for receiver in receivers:
            result = ssh_run(receiver["host"], "true", check=False, timeout=10)
            if result.returncode != 0:
                raise RuntimeError(
                    "Proxy cannot SSH to receiver host %s.\n"
                    "Configure passwordless SSH from Proxy to Receiver first." %
                    receiver["interface_ip"])

        cleanup_old_runs(experiment["retention"], args.repo / "test-runs" / "lab" / "runs")
        groups = split_receiver_indexes(int(experiment["receiver_count"]), len(receivers))
        drop_json_paths = []
        loss = experiment.get("loss", {})
        for host_index, group in enumerate(groups, 1):
            drops = {}
            if loss.get("enabled", False):
                for receiver_index in group:
                    drops[str(receiver_index)] = loss_blocks(
                        receiver_index, int(experiment["receiver_count"]),
                        int(loss.get("blocks_per_receiver", 0)),
                        int(experiment["section_blocks"]))
            drop_path = run_dir / ("receiver-host-%d-drops.json" % host_index)
            write_json(drop_path, drops)
            drop_json_paths.append(drop_path)

        proxy_binary = args.repo / "build" / "site_proxy_sender"
        if not proxy_binary.exists():
            raise RuntimeError("site_proxy_sender not found: %s" % proxy_binary)
        proxy_log = run_dir / "proxy.log"
        proxy_args = [
            str(proxy_binary),
            "--multicast-ip", experiment["multicast_ip"],
            "--udp-port", str(experiment["udp_port"]),
            "--interface-ip", proxy["interface_ip"],
            "--control-port", str(experiment["control_port"]),
            "--receiver-count", str(experiment["receiver_count"]),
            "--pace-us", str(experiment["udp_pace_us"]),
            "--gap-ms", "0",
            "--max-repair-rounds", str(experiment["max_repair_rounds"]),
            "--section-blocks", str(experiment["section_blocks"]),
            "--repair-mode", experiment["mode"],
            "--central-listen", str(experiment["central_port"]),
            "--cache-dir", str(run_dir / "proxy-cache"),
        ]
        proxy_log_handle = proxy_log.open("w", encoding="utf-8")
        proxy_process = subprocess.Popen(proxy_args, stdout=proxy_log_handle,
                                         stderr=subprocess.STDOUT, cwd=args.repo,
                                         text=True, start_new_session=True)
        print("[%s] proxy pid=%d" % (utc_now(), proxy_process.pid), flush=True)

        monitor_args = [
            "python3", str(args.repo / "benchmark" / "lab" / "resource_monitor.py"),
            "--host-label", proxy["interface_ip"],
            "--output", str(run_dir / "resources.csv"),
            "--interval", str(experiment["monitor"]["interval_seconds"]),
            "--test-dir", str(args.repo / "test-runs" / "lab"),
            "--process", "proxy,,%d" % proxy_process.pid,
        ]
        monitor_log_handle = (run_dir / "resource-monitor.log").open("w", encoding="utf-8")
        proxy_monitor = subprocess.Popen(monitor_args, stdout=monitor_log_handle,
                                         stderr=subprocess.STDOUT, cwd=args.repo,
                                         text=True, start_new_session=True)

        deadline = time.monotonic() + 20
        while "waiting for central sender on port" not in (
                proxy_log.read_text(encoding="utf-8", errors="replace")
                if proxy_log.exists() else ""):
            if proxy_process.poll() is not None:
                raise RuntimeError("proxy exited during startup")
            if time.monotonic() >= deadline:
                raise TimeoutError("proxy did not start listening")
            time.sleep(0.1)

        process_json = {
            "time": utc_now(),
            "run_id": args.run_id,
            "proxy_pid": proxy_process.pid,
            "proxy_monitor_pid": proxy_monitor.pid,
            "receiver_launchers": [],
        }
        for host_index, receiver in enumerate(receivers, 1):
            indexes = groups[host_index - 1]
            if not indexes:
                continue
            receiver_run_dir = Path(receiver["repo"]) / "test-runs" / "lab" / "runs" / args.run_id
            launcher_log = receiver_run_dir / "launcher.log"
            command = [
                "python3", str(Path(receiver["repo"]) / "benchmark" / "lab" / "receiver_launcher.py"),
                "--repo", receiver["repo"],
                "--run-dir", str(receiver_run_dir),
                "--proxy-ip", proxy.get("receiver_connect_ip", proxy["interface_ip"]),
                "--interface-ip", receiver["interface_ip"],
                "--multicast-ip", experiment["multicast_ip"],
                "--udp-port", str(experiment["udp_port"]),
                "--control-port", str(experiment["control_port"]),
                "--receiver-indexes", ",".join(str(value) for value in indexes),
                "--drop-blocks-json", str(Path(receiver["repo"]) / "test-runs" / "lab" / "runs" /
                                          args.run_id / drop_json_paths[host_index - 1].name),
                "--monitor-interval", str(experiment["monitor"]["interval_seconds"]),
            ]
            # The drop plan can become large. Do not embed its contents
            # into one SSH argv string; copy it as a normal file instead.
            local_drop_path = drop_json_paths[host_index - 1]
            remote_drop_path = receiver_run_dir / local_drop_path.name

            ssh_run(
                receiver["host"],
                "mkdir -p " + shell_join([str(receiver_run_dir)]),
                timeout=30,
            )

            subprocess.run(
                [
                    "scp",
                    "-q",
                    "-o", "BatchMode=yes",
                    "-o", "ConnectTimeout=10",
                    str(local_drop_path),
                    "%s:%s" % (
                        receiver["host"],
                        str(remote_drop_path),
                    ),
                ],
                check=True,
                timeout=120,
            )
            launcher_pid = start_remote_launcher(receiver, command, launcher_log)
            remote_launchers.append((receiver["host"], launcher_pid))
            process_json["receiver_launchers"].append({
                "host": receiver["host"],
                "interface_ip": receiver["interface_ip"],
                "pid": launcher_pid,
                "indexes": indexes,
                "run_dir": str(receiver_run_dir),
            })
            print("[%s] receiver-host-%d started=%d" %
                  (utc_now(), host_index, len(indexes)), flush=True)

        write_json(run_dir / "process.json", process_json)

        ready_deadline = time.monotonic() + 60
        while True:
            text, count = registrations(proxy_log)
            print("[%s] registered=%d/%d" %
                  (utc_now(), count, int(experiment["receiver_count"])), flush=True)
            if count == int(experiment["receiver_count"]):
                append_jsonl(history_dir / "events.jsonl",
                             {"time": utc_now(), "run_id": args.run_id,
                              "event": "READY", "receivers": count})
                write_json(run_dir / "status.json", {
                    "time": utc_now(), "run_id": args.run_id,
                    "lifecycle": "READY_FOR_CENTRAL", "ready": True,
                    "registered": count, "receiver_count": experiment["receiver_count"],
                    "proxy_pid": proxy_process.pid,
                })
                print("[%s] READY_FOR_CENTRAL" % utc_now(), flush=True)
                break
            if proxy_process.poll() is not None:
                raise RuntimeError("proxy exited before receivers registered")
            if time.monotonic() >= ready_deadline:
                raise TimeoutError("timed out waiting for receiver registration")
            time.sleep(1)

        while not STOP:
            if proxy_process.poll() is not None:
                status = "PASS" if proxy_process.returncode == 0 else "FAIL"
                if status != "PASS":
                    failure_reason = "proxy exited with status %d" % proxy_process.returncode
                    write_json(run_dir / "status.json", {
                        "time": utc_now(), "run_id": args.run_id,
                        "lifecycle": "FAILED", "ready": True,
                        "proxy_pid": proxy_process.pid,
                    })
                else:
                    write_json(run_dir / "status.json", {
                        "time": utc_now(), "run_id": args.run_id,
                        "lifecycle": "COMPLETED", "ready": True,
                        "proxy_pid": proxy_process.pid,
                    })
                break
            proxy_text = proxy_log.read_text(encoding="utf-8", errors="replace")
            if "central sender connected" in proxy_text:
                write_json(run_dir / "status.json", {
                    "time": utc_now(), "run_id": args.run_id,
                    "lifecycle": "TRANSFERRING", "ready": True,
                    "proxy_pid": proxy_process.pid,
                })
            time.sleep(1)
        if STOP:
            status = "INTERRUPTED"
            failure_reason = "proxy_controller stopped"
            write_json(run_dir / "status.json", {
                "time": utc_now(), "run_id": args.run_id,
                "lifecycle": "STOPPED", "ready": False,
                "proxy_pid": proxy_process.pid if proxy_process else None,
            })
    except Exception as error:
        status = "FAIL"
        failure_reason = str(error)
        print(failure_reason, file=sys.stderr, flush=True)
        raise
    finally:
        for host, pid in reversed(remote_launchers):
            remote_stop(host, pid)
        stop_process(proxy_monitor)
        stop_process(proxy_process)
        if proxy_log_handle:
            proxy_log_handle.close()
        if monitor_log_handle:
            monitor_log_handle.close()
        (run_dir / "ACTIVE").unlink(missing_ok=True)
        marker = "DONE" if status == "PASS" else "FAILED"
        (run_dir / marker).touch()
        write_json(run_dir / "controller-result.json", {
            "time": utc_now(), "run_id": args.run_id, "status": status,
            "failure_reason": failure_reason,
        })
        append_jsonl(history_dir / "events.jsonl",
                     {"time": utc_now(), "run_id": args.run_id,
                      "event": status, "failure_reason": failure_reason})


if __name__ == "__main__":
    main()
