#!/usr/bin/env python3
"""Reproducible, dependency-free benchmark/validation runner for SiteRepairCast."""

import argparse
import csv
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time


PAYLOAD_BYTES = 1200
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILE = Path(__file__).resolve().parent / "profiles" / "smoke.json"
KINDS = {"transfer", "slow_shared", "slow_isolated", "central_resume", "proxy_crash"}


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def read_text(path):
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def command_version(command):
    try:
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=10, check=False)
        return result.stdout.splitlines()[0] if result.stdout else "unknown"
    except (OSError, subprocess.TimeoutExpired):
        return "unavailable"


def git_output(*args):
    result = subprocess.run(["git", "-C", str(ROOT), *args], text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False)
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def host_metadata():
    cpu_model = "unknown"
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.lower().startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    memory_kib = None
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                memory_kib = int(line.split()[1])
                break
    except (OSError, ValueError):
        pass
    return {
        "captured_at_utc": utc_now(),
        "hostname": platform.node(),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "cpu_model": cpu_model,
        "logical_cpu_count": os.cpu_count(),
        "memory_total_kib": memory_kib,
        "python": platform.python_version(),
        "cmake": command_version(["cmake", "--version"]),
        "compiler": command_version([os.environ.get("CXX", "c++"), "--version"]),
        "git_commit": git_output("rev-parse", "HEAD"),
        "git_status_porcelain": git_output("status", "--porcelain"),
    }


def validate_profile(profile):
    required = ["name", "file_size_bytes", "repetitions", "section_blocks", "scenarios"]
    for key in required:
        if key not in profile:
            raise ValueError("profile is missing required key: " + key)
    for key in ("file_size_bytes", "repetitions", "section_blocks"):
        if not isinstance(profile[key], int) or profile[key] <= 0:
            raise ValueError("profile.%s must be a positive integer" % key)
    seen = set()
    total_blocks = math.ceil(profile["file_size_bytes"] / PAYLOAD_BYTES)
    total_sections = math.ceil(total_blocks / profile["section_blocks"])
    for scenario in profile["scenarios"]:
        scenario_id = scenario.get("id", "")
        kind = scenario.get("kind", "")
        receivers = scenario.get("receivers", 0)
        if not scenario_id or scenario_id in seen:
            raise ValueError("scenario ids must be non-empty and unique")
        seen.add(scenario_id)
        if kind not in KINDS:
            raise ValueError("unsupported scenario kind %r" % kind)
        if not isinstance(receivers, int) or receivers <= 0:
            raise ValueError("%s.receivers must be positive" % scenario_id)
        loss_percent = scenario.get("deterministic_initial_data_drop_percent", 0)
        if not isinstance(loss_percent, (int, float)) or not 0 <= loss_percent < 100:
            raise ValueError("%s.deterministic_initial_data_drop_percent must be in [0, 100)" %
                             scenario_id)
        if kind.startswith("slow_"):
            laggers = scenario.get("lagging_receiver_ids", [])
            if len(laggers) < 2 or any(item < 1 or item > receivers for item in laggers):
                raise ValueError("%s needs at least two valid lagging receiver ids" % scenario_id)
            if scenario.get("lagging_drop_blocks", 0) <= 0:
                raise ValueError("%s.lagging_drop_blocks must be positive" % scenario_id)
        if kind == "central_resume":
            stop = scenario.get("stop_after_sections", 0)
            if stop <= 0 or stop >= total_sections:
                raise ValueError("%s.stop_after_sections must be within the file" % scenario_id)
        if kind == "proxy_crash":
            section = scenario.get("kill_after_section", -1)
            if section < 0 or section + 1 >= total_sections:
                raise ValueError("%s.kill_after_section must leave data to resume" % scenario_id)


def deterministic_input(path, size, seed):
    pattern = bytearray()
    counter = 0
    while len(pattern) < 1024 * 1024:
        pattern.extend(hashlib.sha256((seed + ":" + str(counter)).encode()).digest())
        counter += 1
    pattern = bytes(pattern[:1024 * 1024])
    remaining = size
    with Path(path).open("wb") as output:
        while remaining:
            chunk = pattern[:min(remaining, len(pattern))]
            output.write(chunk)
            remaining -= len(chunk)


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def pick_ports():
    ports = []
    while len(ports) < 3:
        sock_type = socket.SOCK_DGRAM if len(ports) == 0 else socket.SOCK_STREAM
        with socket.socket(socket.AF_INET, sock_type) as handle:
            handle.bind(("127.0.0.1", 0))
            port = handle.getsockname()[1]
        if port not in ports:
            ports.append(port)
    return ports[0], ports[1], ports[2]


def evenly_spaced_blocks(total_blocks, count):
    count = max(0, min(total_blocks, count))
    if count == 0:
        return []
    return sorted({min(total_blocks - 1, ((2 * index + 1) * total_blocks) // (2 * count))
                   for index in range(count)})


def count_values(pattern, text):
    return sum(int(value) for value in re.findall(pattern, text))


class ProcessSet:
    def __init__(self):
        self.items = []

    def spawn(self, name, command, log_path, env=None):
        log_path = Path(log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handle = log_path.open("w", encoding="utf-8")
        merged_env = os.environ.copy()
        if env:
            merged_env.update({key: str(value) for key, value in env.items()})
        process = subprocess.Popen([str(value) for value in command], stdout=handle,
                                   stderr=subprocess.STDOUT, env=merged_env, text=True)
        item = {"name": name, "process": process, "handle": handle,
                "command": [str(value) for value in command],
                "env": dict(env or {}), "log": str(log_path)}
        self.items.append(item)
        return item

    @staticmethod
    def close(item):
        if not item["handle"].closed:
            item["handle"].close()

    def stop(self, item, force=False):
        process = item["process"]
        if process.poll() is None:
            process.send_signal(signal.SIGKILL if force else signal.SIGTERM)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        self.close(item)

    def cleanup(self):
        for item in reversed(self.items):
            self.stop(item)


class BenchmarkRunner:
    def __init__(self, profile, output, build_dir, timeout):
        self.profile = profile
        self.output = Path(output)
        self.build_dir = Path(build_dir)
        self.timeout = timeout
        self.raw_dir = self.output / "raw"
        self.results = []

    def env(self, **extra):
        values = {"SRCAST_SECTION_BLOCKS": self.profile["section_blocks"]}
        values.update({key: value for key, value in extra.items() if value is not None})
        return values

    def wait_log(self, log, pattern, timeout, watched=None, count=1):
        deadline = time.monotonic() + timeout
        compiled = re.compile(pattern)
        while time.monotonic() < deadline:
            if len(compiled.findall(read_text(log))) >= count:
                return time.monotonic_ns()
            if watched and watched["process"].poll() is not None:
                raise RuntimeError("%s exited before log pattern %r" % (watched["name"], pattern))
            time.sleep(0.01)
        raise TimeoutError("timed out waiting for %r in %s" % (pattern, log))

    def start_components(self, processes, work, logs, receivers, proxy_env=None,
                         receiver_envs=None, suffix=""):
        udp_port, control_port, central_port = pick_ports()
        cache = work / "cache"
        cache.mkdir(exist_ok=True)
        proxy_log = logs / ("proxy%s.log" % suffix)
        proxy_command = [self.build_dir / "site_proxy_sender", "239.255.42.99", udp_port,
                         "127.0.0.1", control_port, receivers,
                         self.profile.get("udp_pace_us", 100), 0,
                         self.profile.get("max_repair_rounds", 3),
                         "--central-listen", central_port, cache]
        proxy = processes.spawn("proxy" + suffix, proxy_command, proxy_log,
                                self.env(**(proxy_env or {})))
        self.wait_log(proxy_log, r"waiting for central sender on port", 10, proxy)
        receiver_items = []
        receiver_dirs = []
        for receiver_id in range(1, receivers + 1):
            receiver_dir = work / ("receiver-%d" % receiver_id)
            receiver_dir.mkdir(exist_ok=True)
            receiver_dirs.append(receiver_dir)
            command = [self.build_dir / "receiver_agent", "239.255.42.99", udp_port,
                       receiver_dir, "127.0.0.1", control_port, receiver_id, "127.0.0.1"]
            item = processes.spawn("receiver-%d%s" % (receiver_id, suffix), command,
                                   logs / ("receiver-%d%s.log" % (receiver_id, suffix)),
                                   self.env(**(receiver_envs or {}).get(receiver_id, {})))
            receiver_items.append(item)
        self.wait_log(proxy_log, r"registered receiver_id=", 15, proxy, receivers)
        return {
            "proxy": proxy,
            "receivers": receiver_items,
            "receiver_dirs": receiver_dirs,
            "cache": cache,
            "central_port": central_port,
            "proxy_log": proxy_log,
        }

    def central(self, processes, input_path, central_port, logs, suffix="", extra_env=None):
        command = [self.build_dir / "central_sender", "127.0.0.1", central_port,
                   self.profile.get("central_pace_us", 0), input_path]
        return processes.spawn("central" + suffix, command,
                               logs / ("central%s.log" % suffix), self.env(**(extra_env or {})))

    def monitor(self, items, receiver_dirs, started_ns, timeout, allow_codes=None):
        allow_codes = allow_codes or {}
        completion_ns = [None] * len(receiver_dirs)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            now = time.monotonic_ns()
            for index, directory in enumerate(receiver_dirs):
                if completion_ns[index] is None and any(directory.glob("transfer-*.bin")):
                    completion_ns[index] = now
            if all(item["process"].poll() is not None for item in items):
                break
            time.sleep(0.01)
        else:
            raise TimeoutError("scenario exceeded %ss" % timeout)
        ended_ns = time.monotonic_ns()
        for item in items:
            code = item["process"].returncode
            if code not in allow_codes.get(item["name"], {0}):
                raise RuntimeError("%s exited with status %s; see %s" %
                                   (item["name"], code, item["log"]))
            ProcessSet.close(item)
        for index, directory in enumerate(receiver_dirs):
            if completion_ns[index] is None and any(directory.glob("transfer-*.bin")):
                completion_ns[index] = ended_ns
        return ended_ns, [None if value is None else (value - started_ns) / 1e6
                          for value in completion_ns]

    def verify(self, input_path, cache, receiver_dirs):
        expected = sha256_file(input_path)
        cached = list(cache.glob("central-transfer-*.bin"))
        outputs = [list(directory.glob("transfer-*.bin")) for directory in receiver_dirs]
        if len(cached) != 1 or any(len(paths) != 1 for paths in outputs):
            raise RuntimeError("expected exactly one cache file and one file per receiver")
        actual = [sha256_file(cached[0])] + [sha256_file(paths[0]) for paths in outputs]
        if any(value != expected for value in actual):
            raise RuntimeError("SHA-256 mismatch")
        return expected

    def base_metrics(self, scenario, repeat, logs, input_path, component, started_ns,
                     ended_ns, completion_ms):
        proxy_text = read_text(component["proxy_log"])
        receiver_texts = [read_text(logs / ("receiver-%d.log" % receiver_id))
                          for receiver_id in range(1, scenario["receivers"] + 1)]
        file_bytes = input_path.stat().st_size
        duration_ms = (ended_ns - started_ns) / 1e6
        laggers = scenario.get("lagging_receiver_ids", [])
        normal = [value for index, value in enumerate(completion_ms, 1)
                  if index not in laggers and value is not None]
        lagging = [value for index, value in enumerate(completion_ms, 1)
                   if index in laggers and value is not None]
        return {
            "scenario": scenario["id"],
            "kind": scenario["kind"],
            "comparison_group": scenario.get("comparison_group"),
            "repeat": repeat,
            "success": True,
            "file_bytes": file_bytes,
            "receivers": scenario["receivers"],
            "duration_ms": round(duration_ms, 3),
            "receiver_completion_ms": [None if value is None else round(value, 3)
                                       for value in completion_ms],
            "normal_completion_ms": round(max(normal), 3) if normal else None,
            "lagging_completion_ms": round(max(lagging), 3) if lagging else None,
            "lagging_minus_normal_ms": round(max(lagging) - max(normal), 3)
                                        if normal and lagging else None,
            "aggregate_verified_payload_mib_s": round(
                (file_bytes * scenario["receivers"] / (1024 * 1024)) /
                (duration_ms / 1000), 6),
            "repair_multicast_packets": count_values(r"multicast_packets=(\d+)", proxy_text),
            "repair_unicast_packets": count_values(r"unicast_packets=(\d+)", proxy_text),
            "tcp_backfill_blocks": count_values(r"TCP backfill receiver_id=\d+ blocks=(\d+)",
                                                 proxy_text),
            "isolated_receiver_ids": [int(value) for value in
                                      re.findall(r"receiver_id=(\d+) isolated for TCP backfill",
                                                 proxy_text)],
            "intentional_drops": sum(count_values(r"intentional_drops=(\d+)", text)
                                     for text in receiver_texts),
            "duplicates": sum(count_values(r"duplicates=(\d+)", text)
                              for text in receiver_texts),
            "rejected": sum(count_values(r"rejected=(\d+)", text)
                            for text in receiver_texts),
            "source_sha256": self.verify(input_path, component["cache"],
                                           component["receiver_dirs"]),
            "sha256_verified": True,
            "cross_site_payload_bytes_protocol_derived": file_bytes,
            "direct_tcp_payload_bytes_model": file_bytes * scenario["receivers"],
            "direct_tcp_payload_savings_percent_model": round(
                (1 - 1 / scenario["receivers"]) * 100, 3),
        }

    def receiver_envs(self, receivers, block_ids):
        value = ",".join(str(item) for item in block_ids)
        return {receiver_id: {"SRCAST_DROP_INITIAL_BLOCKS": value}
                for receiver_id in receivers if value}

    def run_transfer(self, scenario, repeat, work, logs, processes, input_path):
        receivers = scenario["receivers"]
        total_blocks = math.ceil(input_path.stat().st_size / PAYLOAD_BYTES)
        proxy_env = {"SRCAST_BACKFILL_ROUNDS": 4294967295}
        receiver_envs = {}
        if scenario["kind"] == "transfer":
            percent = scenario.get("deterministic_initial_data_drop_percent", 0)
            count = round(total_blocks * percent / 100)
            dropped = evenly_spaced_blocks(total_blocks, count)
            receiver_envs = self.receiver_envs(range(1, receivers + 1), dropped)
        else:
            dropped = list(range(min(scenario["lagging_drop_blocks"],
                                     self.profile["section_blocks"])))
            receiver_envs = self.receiver_envs(scenario["lagging_receiver_ids"], dropped)
            if scenario["kind"] == "slow_isolated":
                proxy_env["SRCAST_BACKFILL_ROUNDS"] = scenario["backfill_rounds"]
        component = self.start_components(processes, work, logs, receivers,
                                          proxy_env, receiver_envs)
        started_ns = time.monotonic_ns()
        central = self.central(processes, input_path, component["central_port"], logs)
        ended_ns, completion_ms = self.monitor(
            [central, component["proxy"], *component["receivers"]],
            component["receiver_dirs"], started_ns, self.timeout)
        metrics = self.base_metrics(scenario, repeat, logs, input_path, component,
                                    started_ns, ended_ns, completion_ms)
        metrics["configured_dropped_blocks_per_receiver"] = len(dropped)
        metrics["configured_initial_data_drop_percent"] = scenario.get(
            "deterministic_initial_data_drop_percent")
        metrics["effective_initial_data_drop_percent"] = round(
            len(dropped) / total_blocks * 100, 6)
        if scenario["kind"] == "slow_shared" and metrics["isolated_receiver_ids"]:
            raise RuntimeError("shared-repair case unexpectedly isolated receivers")
        if scenario["kind"] == "slow_isolated":
            if sorted(metrics["isolated_receiver_ids"]) != sorted(scenario["lagging_receiver_ids"]):
                raise RuntimeError("isolation case did not isolate the configured lagging receivers")
            if metrics["tcp_backfill_blocks"] == 0:
                raise RuntimeError("isolation case did not execute TCP backfill")
        if scenario["kind"] == "transfer" and dropped and metrics["repair_multicast_packets"] == 0:
            raise RuntimeError("loss case did not execute multicast repair")
        if scenario["kind"] == "transfer" and metrics["intentional_drops"] != len(dropped) * receivers:
            raise RuntimeError("configured drops do not match receiver-observed intentional drops")
        return metrics

    def run_resume(self, scenario, repeat, work, logs, processes, input_path):
        component = self.start_components(processes, work, logs, scenario["receivers"],
            {"SRCAST_BACKFILL_ROUNDS": 4294967295}, {})
        started_ns = time.monotonic_ns()
        first = self.central(processes, input_path, component["central_port"], logs,
                             "-first", {"SRCAST_STOP_AFTER_SECTIONS":
                                        scenario["stop_after_sections"]})
        first["process"].wait(timeout=self.timeout)
        ProcessSet.close(first)
        if first["process"].returncode != 0:
            raise RuntimeError("first central attempt failed unexpectedly")
        self.wait_log(component["proxy_log"], r"central session interrupted", 10,
                      component["proxy"])
        recovery_started_ns = time.monotonic_ns()
        second = self.central(processes, input_path, component["central_port"], logs, "-resume")
        ended_ns, completion_ms = self.monitor(
            [second, component["proxy"], *component["receivers"]],
            component["receiver_dirs"], started_ns, self.timeout)
        metrics = self.base_metrics(scenario, repeat, logs, input_path, component,
                                    started_ns, ended_ns, completion_ms)
        resume_text = read_text(logs / "central-resume.log")
        match = re.search(r"next_section=(\d+)/(\d+)", resume_text)
        if not match:
            raise RuntimeError("resume position missing from central log")
        resume_section, total_sections = map(int, match.groups())
        if resume_section != scenario["stop_after_sections"]:
            raise RuntimeError("resume started at section %d, expected %d" %
                               (resume_section, scenario["stop_after_sections"]))
        retained = min(input_path.stat().st_size,
                       resume_section * self.profile["section_blocks"] * PAYLOAD_BYTES)
        metrics.update({
            "resume_section": resume_section,
            "total_sections": total_sections,
            "recovery_ms": round((ended_ns - recovery_started_ns) / 1e6, 3),
            "checkpoint_payload_bytes_retained_protocol_derived": retained,
            "confirmed_payload_bytes_retransmitted_protocol_derived": 0,
        })
        return metrics

    def run_crash(self, scenario, repeat, work, logs, processes, input_path):
        component = self.start_components(processes, work, logs, scenario["receivers"],
            {"SRCAST_BACKFILL_ROUNDS": 4294967295}, {})
        started_ns = time.monotonic_ns()
        first = self.central(processes, input_path, component["central_port"], logs, "-first")
        self.wait_log(component["proxy_log"],
                      r"central section cached section=%d" % scenario["kill_after_section"],
                      self.timeout, component["proxy"])
        crash_ns = time.monotonic_ns()
        processes.stop(component["proxy"], force=True)
        for item in [first, *component["receivers"]]:
            processes.stop(item)
        time.sleep(0.2)
        first_proxy_text = read_text(component["proxy_log"])
        cached_sections = [int(value) for value in
                           re.findall(r"central section cached section=(\d+)", first_proxy_text)]
        expected_resume = max(cached_sections) + 1
        resumed = self.start_components(processes, work, logs, scenario["receivers"],
            {"SRCAST_BACKFILL_ROUNDS": 4294967295}, {}, "-resume")
        recovery_started_ns = time.monotonic_ns()
        second = self.central(processes, input_path, resumed["central_port"], logs, "-resume")
        ended_ns, completion_ms = self.monitor(
            [second, resumed["proxy"], *resumed["receivers"]], resumed["receiver_dirs"],
            started_ns, self.timeout)
        metrics = self.base_metrics(scenario, repeat, logs, input_path, resumed,
                                    started_ns, ended_ns, completion_ms)
        resume_text = read_text(logs / "central-resume.log")
        match = re.search(r"next_section=(\d+)/(\d+)", resume_text)
        if not match:
            raise RuntimeError("crash recovery resume position missing")
        resume_section, total_sections = map(int, match.groups())
        if resume_section != expected_resume:
            raise RuntimeError("crash recovery resumed at %d, checkpoint says %d" %
                               (resume_section, expected_resume))
        recovered = []
        for receiver_id in range(1, scenario["receivers"] + 1):
            text = read_text(logs / ("receiver-%d-resume.log" % receiver_id))
            found = re.search(r"recovered_blocks=(\d+)", text)
            recovered.append(int(found.group(1)) if found else 0)
        retained = min(input_path.stat().st_size,
                       resume_section * self.profile["section_blocks"] * PAYLOAD_BYTES)
        metrics.update({
            "failure_signal": "SIGKILL",
            "resume_section": resume_section,
            "total_sections": total_sections,
            "recovered_blocks_per_receiver": recovered,
            "recovery_ms": round((ended_ns - recovery_started_ns) / 1e6, 3),
            "restart_delay_ms": round((recovery_started_ns - crash_ns) / 1e6, 3),
            "crash_to_recovered_ms": round((ended_ns - crash_ns) / 1e6, 3),
            "checkpoint_payload_bytes_retained_protocol_derived": retained,
            "confirmed_payload_bytes_retransmitted_protocol_derived": 0,
        })
        # The killed attempt can have an unconfirmed Section in flight. Without packet capture,
        # total central payload across both attempts cannot be derived exactly.
        metrics["cross_site_payload_bytes_protocol_derived"] = None
        metrics["cross_site_payload_note"] = (
            "unknown after SIGKILL because an unconfirmed Section may have been in flight")
        return metrics

    def run_one(self, scenario, repeat):
        run_id = "%s-r%02d" % (scenario["id"], repeat)
        logs = self.raw_dir / run_id
        logs.mkdir(parents=True)
        processes = ProcessSet()
        started_utc = utc_now()
        result = None
        commands = []
        with tempfile.TemporaryDirectory(prefix="srcast-validation-") as temporary:
            work = Path(temporary)
            input_path = work / "input.bin"
            # A/B scenarios in the same repetition receive byte-identical input.
            deterministic_input(input_path, self.profile["file_size_bytes"],
                                "%s:%d" % (self.profile["name"], repeat))
            try:
                if scenario["kind"] in {"transfer", "slow_shared", "slow_isolated"}:
                    result = self.run_transfer(scenario, repeat, work, logs, processes, input_path)
                elif scenario["kind"] == "central_resume":
                    result = self.run_resume(scenario, repeat, work, logs, processes, input_path)
                else:
                    result = self.run_crash(scenario, repeat, work, logs, processes, input_path)
            except Exception as error:
                result = {
                    "scenario": scenario["id"], "kind": scenario["kind"],
                    "comparison_group": scenario.get("comparison_group"), "repeat": repeat,
                    "success": False, "file_bytes": self.profile["file_size_bytes"],
                    "receivers": scenario["receivers"], "sha256_verified": False,
                    "error": "%s: %s" % (type(error).__name__, error),
                }
            finally:
                commands = [{key: item[key] for key in ("name", "command", "env", "log")}
                            for item in processes.items]
                processes.cleanup()
        result["run_id"] = run_id
        result["started_at_utc"] = started_utc
        result["finished_at_utc"] = utc_now()
        (logs / "run.json").write_text(json.dumps({"scenario_config": scenario,
                                                   "result": result,
                                                   "processes": commands}, indent=2) + "\n",
                                             encoding="utf-8")
        self.results.append(result)
        status = "PASS" if result["success"] else "FAIL"
        print("[%s] %s" % (status, run_id), flush=True)
        if not result["success"]:
            print("       " + result["error"], file=sys.stderr, flush=True)


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percent / 100 * len(ordered)) - 1)
    return ordered[index]


def aggregate(results):
    rows = []
    for scenario in sorted({item["scenario"] for item in results}):
        runs = [item for item in results if item["scenario"] == scenario]
        passed = [item for item in runs if item["success"]]
        durations = [item["duration_ms"] for item in passed if item.get("duration_ms") is not None]
        normal = [item["normal_completion_ms"] for item in passed
                  if item.get("normal_completion_ms") is not None]
        recovery = [item["recovery_ms"] for item in passed if item.get("recovery_ms") is not None]
        rows.append({
            "scenario": scenario,
            "kind": runs[0]["kind"],
            "file_bytes": runs[0].get("file_bytes"),
            "receivers": runs[0].get("receivers"),
            "runs": len(runs),
            "passed": len(passed),
            "success_rate_percent": round(len(passed) / len(runs) * 100, 3),
            "duration_median_ms": round(statistics.median(durations), 3) if durations else None,
            "duration_p95_ms": round(percentile(durations, 95), 3) if durations else None,
            "duration_p99_ms": round(percentile(durations, 99), 3) if durations else None,
            "normal_completion_median_ms": round(statistics.median(normal), 3) if normal else None,
            "recovery_median_ms": round(statistics.median(recovery), 3) if recovery else None,
            "repair_multicast_packets_median": statistics.median(
                [item.get("repair_multicast_packets", 0) for item in passed]) if passed else None,
            "tcp_backfill_blocks_median": statistics.median(
                [item.get("tcp_backfill_blocks", 0) for item in passed]) if passed else None,
            "intentional_drops_median": statistics.median(
                [item.get("intentional_drops", 0) for item in passed]) if passed else None,
            "configured_dropped_blocks_per_receiver": passed[0].get(
                "configured_dropped_blocks_per_receiver") if passed else None,
            "effective_initial_data_drop_percent": passed[0].get(
                "effective_initial_data_drop_percent") if passed else None,
            "resume_section": passed[0].get("resume_section") if passed else None,
            "total_sections": passed[0].get("total_sections") if passed else None,
            "crash_to_recovered_median_ms": round(statistics.median(
                [item["crash_to_recovered_ms"] for item in passed
                 if item.get("crash_to_recovered_ms") is not None]), 3)
                if any(item.get("crash_to_recovered_ms") is not None for item in passed) else None,
            "direct_tcp_payload_savings_percent_model": passed[0].get(
                "direct_tcp_payload_savings_percent_model") if passed else None,
            "sha256_success_rate_percent": round(
                sum(bool(item.get("sha256_verified")) for item in runs) / len(runs) * 100, 3),
        })
    return rows


def write_csv(path, rows, fields):
    with Path(path).open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def markdown_report(profile, metadata, results, summary):
    lines = ["# SiteRepairCast benchmark report", "",
             "- Profile: `%s`" % profile["name"],
             "- File size: `%d` bytes" % profile["file_size_bytes"],
             "- Repetitions: `%d`" % profile["repetitions"],
             "- Section size: `%d` blocks (`%d` payload bytes/block)" %
             (profile["section_blocks"], PAYLOAD_BYTES),
             "- Host: `%s`, `%s`, %s logical CPUs" %
             (metadata["hostname"], metadata["cpu_model"], metadata["logical_cpu_count"]),
             "- Git commit: `%s`" % metadata["git_commit"], "",
             "## Measured results", "",
             "Wall-clock values start immediately before the central sender. Receiver completion is "
             "observed from the atomically renamed final file (10 ms polling resolution).", "",
             "| Scenario | Kind | Passed | Success | Median ms | P95 ms | P99 ms | Normal receivers median ms | Recovery median ms | SHA-256 |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in summary:
        def cell(key):
            value = row.get(key)
            return "-" if value is None else str(value)
        lines.append("| %s | %s | %s/%s | %s%% | %s | %s | %s | %s | %s | %s%% |" %
                     (row["scenario"], row["kind"], row["passed"], row["runs"],
                      cell("success_rate_percent"), cell("duration_median_ms"),
                      cell("duration_p95_ms"), cell("duration_p99_ms"),
                      cell("normal_completion_median_ms"), cell("recovery_median_ms"),
                      cell("sha256_success_rate_percent")))
    lines.extend(["", "## Distribution and repair evidence", "",
                  "| Scenario | Receivers | Dropped blocks / affected receiver | Multicast repair packets | "
                  "TCP backfill blocks | Protocol payload bytes (derived) | Direct TCP payload "
                  "bytes (model) | Model savings |",
                  "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
    for result in results:
        if not result.get("success") or result["kind"] not in {
                "transfer", "slow_shared", "slow_isolated"}:
            continue
        lines.append("| %s (r%d) | %d | %s | %s | %s | %s | %s | %s%% |" %
                     (result["scenario"], result["repeat"], result["receivers"],
                      result.get("configured_dropped_blocks_per_receiver", "-"),
                      result.get("repair_multicast_packets", "-"),
                      result.get("tcp_backfill_blocks", "-"),
                      result.get("cross_site_payload_bytes_protocol_derived", "-"),
                      result.get("direct_tcp_payload_bytes_model", "-"),
                      result.get("direct_tcp_payload_savings_percent_model", "-")))
    lines.extend(["", "## Slow-node A/B", ""])
    comparisons = {}
    for result in results:
        if result.get("success") and result.get("comparison_group") and \
                result.get("normal_completion_ms") is not None:
            comparisons.setdefault(result["comparison_group"], {}).setdefault(result["kind"], []).append(
                result["normal_completion_ms"])
    if not comparisons:
        lines.append("No complete slow-node comparison was produced.")
    for group, modes in comparisons.items():
        if "slow_shared" not in modes or "slow_isolated" not in modes:
            lines.append("- `%s`: incomplete pair." % group)
            continue
        shared = statistics.median(modes["slow_shared"])
        isolated = statistics.median(modes["slow_isolated"])
        delta = shared - isolated
        percent = delta / shared * 100 if shared else 0
        lines.append("- `%s`: normal receivers median completion %.3f ms with shared repair vs "
                     "%.3f ms with isolation; delta %.3f ms (%.3f%%)." %
                     (group, shared, isolated, delta, percent))
    lines.extend(["", "## Resume and crash-recovery evidence", "",
                  "| Scenario | Resume section | Total sections | Recovery ms | Crash-to-recovered "
                  "ms | Checkpoint payload retained (derived) | Confirmed payload retransmitted "
                  "(derived) | Recovered receiver blocks |",
                  "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"])
    for result in results:
        if not result.get("success") or result["kind"] not in {
                "central_resume", "proxy_crash"}:
            continue
        recovered = result.get("recovered_blocks_per_receiver", "-")
        if isinstance(recovered, list):
            recovered = ", ".join(str(value) for value in recovered)
        lines.append("| %s (r%d) | %s | %s | %s | %s | %s | %s | %s |" %
                     (result["scenario"], result["repeat"], result.get("resume_section", "-"),
                      result.get("total_sections", "-"), result.get("recovery_ms", "-"),
                      result.get("crash_to_recovered_ms", "-"),
                      result.get("checkpoint_payload_bytes_retained_protocol_derived", "-"),
                      result.get("confirmed_payload_bytes_retransmitted_protocol_derived", "-"),
                      recovered))
    lines.extend(["", "## Metric boundaries", "",
        "- Duration, completion time, recovery time, repair/backfill counts, checkpoint position, "
        "process exit status and SHA-256 verification are measured in this run.",
        "- `cross_site_payload_bytes_protocol_derived` is the known file payload for completed "
        "normal/checkpoint-disconnect runs. It is null for SIGKILL because an unconfirmed Section "
        "may be in flight. `direct_tcp_payload_bytes_model` and its savings percentage are a "
        "comparison model, not packet capture or measured wire bytes; headers/control frames are "
        "excluded.",
        "- Loss is injected deterministically: each receiver drops the first arrival of every "
        "selected DATA block and accepts its repair retransmission. The percentage is calculated "
        "over all file blocks, but this is a repeatable application hook, not a random network-loss "
        "emulator. Every receiver uses the same set, so repairs remain multicast-safe on loopback.",
        "- The slow-node A/B uses two equally lagging receivers because same-host receivers share "
        "one IP/UDP port and per-receiver unicast cannot be attributed reliably. It validates the "
        "isolation path under deterministic lag, not bandwidth throttling on separate machines.",
        "- P95/P99 are nearest-rank values across repetitions. A one-run smoke profile only checks "
        "the pipeline and must not be presented as a stable percentile or extrapolated to 1 GB.",
        "- Results are single-host loopback measurements. Run the same profile on isolated hosts "
        "before making production-network claims.", "",
        "## Raw evidence", "",
        "Every run has unmodified process logs, exact argv/environment test hooks and `run.json` "
        "under `raw/<run-id>/`. Machine-readable results are in `raw/metrics.jsonl` and "
        "`raw/metrics.csv`.", ""])
    return "\n".join(lines)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--output", type=Path,
                        help="new output directory (default: artifacts/validation/<timestamp>-<profile>)")
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build-benchmark-validation")
    parser.add_argument("--no-build", action="store_true", help="reuse existing binaries")
    parser.add_argument("--repetitions", type=int, help="override profile repetitions")
    parser.add_argument("--scenario", action="append", help="only run this scenario id; repeatable")
    parser.add_argument("--keep-going", action="store_true",
                        help="continue after a failed run (default stops after preserving evidence)")
    return parser.parse_args()


def main():
    args = parse_args()
    profile = json.loads(args.profile.read_text(encoding="utf-8"))
    if args.repetitions is not None:
        if args.repetitions <= 0:
            raise SystemExit("--repetitions must be positive")
        profile["repetitions"] = args.repetitions
    if args.scenario:
        requested = set(args.scenario)
        profile["scenarios"] = [item for item in profile["scenarios"] if item["id"] in requested]
        missing = requested - {item["id"] for item in profile["scenarios"]}
        if missing:
            raise SystemExit("unknown scenario(s): " + ", ".join(sorted(missing)))
    validate_profile(profile)
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output = args.output or ROOT / "artifacts" / "validation" / (timestamp + "-" + profile["name"])
    output = output.resolve()
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        raise SystemExit("output directory already exists; choose a new --output: %s" % output)
    (output / "raw").mkdir()
    lock_path = ROOT / "artifacts" / "validation" / ".run.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("another SiteRepairCast benchmark is already running")
        metadata = host_metadata()
        manifest = {"profile_source": str(args.profile.resolve()), "profile": profile,
                    "environment": metadata, "command": sys.argv, "output": str(output)}
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n",
                                              encoding="utf-8")
        if not args.no_build:
            with (output / "build.log").open("w", encoding="utf-8") as build_log:
                for command in (["cmake", "-S", str(ROOT), "-B", str(args.build_dir),
                                 "-DCMAKE_BUILD_TYPE=Release"],
                                ["cmake", "--build", str(args.build_dir),
                                 "-j", str(os.cpu_count() or 1)]):
                    build_log.write("$ " + " ".join(command) + "\n")
                    build_log.flush()
                    subprocess.run(command, stdout=build_log, stderr=subprocess.STDOUT, check=True)
        for binary in ("central_sender", "site_proxy_sender", "receiver_agent"):
            if not (args.build_dir / binary).is_file():
                raise SystemExit("missing binary: %s" % (args.build_dir / binary))
        runner = BenchmarkRunner(profile, output, args.build_dir.resolve(),
                                 profile.get("timeout_seconds", 120))
        stop = False
        for scenario in profile["scenarios"]:
            for repeat in range(1, profile["repetitions"] + 1):
                runner.run_one(scenario, repeat)
                if not runner.results[-1]["success"] and not args.keep_going:
                    stop = True
                    break
            if stop:
                break
        metrics_jsonl = output / "raw" / "metrics.jsonl"
        metrics_jsonl.write_text("".join(json.dumps(item, sort_keys=True) + "\n"
                                         for item in runner.results), encoding="utf-8")
        raw_fields = ["run_id", "scenario", "kind", "repeat", "success", "file_bytes",
                      "receivers", "duration_ms", "normal_completion_ms",
                      "lagging_completion_ms", "lagging_minus_normal_ms", "recovery_ms",
                      "crash_to_recovered_ms",
                      "repair_multicast_packets", "repair_unicast_packets", "tcp_backfill_blocks",
                      "intentional_drops", "duplicates", "rejected", "isolated_receiver_ids",
                      "configured_dropped_blocks_per_receiver",
                      "effective_initial_data_drop_percent", "resume_section", "total_sections",
                      "checkpoint_payload_bytes_retained_protocol_derived",
                      "confirmed_payload_bytes_retransmitted_protocol_derived",
                      "recovered_blocks_per_receiver",
                      "cross_site_payload_bytes_protocol_derived",
                      "direct_tcp_payload_bytes_model",
                      "direct_tcp_payload_savings_percent_model", "sha256_verified", "error"]
        write_csv(output / "raw" / "metrics.csv", runner.results, raw_fields)
        summary = aggregate(runner.results)
        summary_fields = list(summary[0].keys()) if summary else ["scenario"]
        write_csv(output / "summary.csv", summary, summary_fields)
        (output / "summary.json").write_text(json.dumps({"profile": profile["name"],
                                                         "summary": summary}, indent=2) + "\n",
                                                   encoding="utf-8")
        (output / "report.md").write_text(markdown_report(profile, metadata,
                                                           runner.results, summary),
                                                 encoding="utf-8")
        failures = [item for item in runner.results if not item["success"]]
        print("Report: %s" % (output / "report.md"))
        print("Raw metrics: %s" % metrics_jsonl)
        return 1 if failures or stop else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        sys.exit(130)
