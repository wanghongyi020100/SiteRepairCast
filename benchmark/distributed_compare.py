#!/usr/bin/env python3
"""Run controlled distributed SiteRepairCast baseline/peer comparisons.

The controller is intended to run on Machine A (Central). Proxy and Receiver
processes are started through SSH; using host="local" is supported for a small
single-machine validation only.
"""

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import shlex
import signal
import statistics
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
PAYLOAD_BYTES = 1200
METRIC_FIELDS = [
    "transfer_time_seconds",
    "proxy_avg_cpu_percent",
    "proxy_cpu_seconds",
    "proxy_repair_payload_bytes",
    "proxy_repair_pread_blocks",
]
SUMMARY_NAMES = {
    "transfer_time_seconds": "transfer_time",
    "proxy_avg_cpu_percent": "proxy_avg_cpu",
}
RAW_FIELDS = [
    "mode", "receivers", "repeat", "file_size_bytes", "transfer_time_seconds",
    "proxy_avg_cpu_percent", "proxy_cpu_seconds", "proxy_repair_payload_bytes",
    "proxy_repair_pread_blocks", "peer_blocks", "sha256_success",
]


def shell_join(values):
    return " ".join(shlex.quote(str(value)) for value in values)


def atomic_write(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


class Host:
    def __init__(self, details, ssh_options):
        self.name = details["host"]
        self.repo = details["repo"]
        self.interface_ip = details.get("interface_ip")
        self.local = self.name in {"local", "localhost", "127.0.0.1"}
        self.ssh_options = [str(value) for value in ssh_options]

    def command(self, script):
        if self.local:
            return ["bash", "-lc", script]
        return ["ssh", *self.ssh_options, self.name, "--", "bash", "-lc",
                shlex.quote(script)]

    def run(self, script, timeout=30, check=True):
        result = subprocess.run(self.command(script), text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=timeout, check=False)
        if check and result.returncode != 0:
            raise RuntimeError("command failed on %s: %s\n%s" %
                               (self.name, script, result.stderr.strip()))
        return result

    def start(self, arguments, log_path, env=None):
        command = []
        if env:
            command.extend(["env", *("%s=%s" % item for item in env.items())])
        command.extend(str(value) for value in arguments)
        script = "mkdir -p %s; setsid %s >%s 2>&1 < /dev/null & echo $!" % (
            shlex.quote(str(Path(log_path).parent)), shell_join(command), shlex.quote(log_path))
        result = self.run(script)
        try:
            return int(result.stdout.strip().splitlines()[-1])
        except (IndexError, ValueError) as error:
            raise RuntimeError("could not read process PID on %s" % self.name) from error

    def alive(self, pid):
        return self.run("kill -0 %d 2>/dev/null" % pid, check=False).returncode == 0

    def stop(self, pid):
        if not pid:
            return
        self.run("kill -TERM -- -%d 2>/dev/null || kill -TERM %d 2>/dev/null || true" %
                 (pid, pid), check=False)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and self.alive(pid):
            time.sleep(0.05)
        if self.alive(pid):
            self.run("kill -KILL -- -%d 2>/dev/null || kill -KILL %d 2>/dev/null || true" %
                     (pid, pid), check=False)

    def read_text(self, path):
        result = self.run("test -f %s && cat %s" %
                          (shlex.quote(path), shlex.quote(path)), check=False)
        return result.stdout if result.returncode == 0 else ""

    def clean_directory(self, path):
        normalized = str(Path(path))
        if normalized in {"", ".", "/"} or "/runs/" not in normalized:
            raise RuntimeError("refusing unsafe cleanup path: %s" % normalized)
        self.run("rm -rf -- %s; mkdir -p %s" %
                 (shlex.quote(normalized), shlex.quote(normalized)))


def validate_config(config):
    for role in ("central", "proxy"):
        if role not in config or "host" not in config[role] or "repo" not in config[role]:
            raise ValueError("config.%s requires host and repo" % role)
    receivers = config.get("receiver_hosts", [])
    if len(receivers) != 2:
        raise ValueError("exactly two receiver_hosts are required for the four-machine test")
    for index, item in enumerate(receivers, 1):
        if not all(key in item for key in ("host", "repo", "interface_ip")):
            raise ValueError("receiver_hosts[%d] requires host, repo, and interface_ip" % index)
    if "interface_ip" not in config["proxy"]:
        raise ValueError("config.proxy.interface_ip is required")
    serialized = json.dumps(config)
    if re.search(r"(?:_IP|_SSH_HOST)\b", serialized):
        raise ValueError("replace placeholder hosts and IP addresses in the config")
    work_root = config.get("work_root", "")
    if not work_root or Path(work_root).is_absolute() or ".." in Path(work_root).parts:
        raise ValueError("work_root must be a safe path relative to each repo")
    for key in ("udp_port", "control_port", "central_port", "section_blocks",
                "max_repair_rounds", "drop_blocks_per_receiver"):
        if int(config.get(key, 0)) <= 0:
            raise ValueError("config.%s must be positive" % key)


def deterministic_input(path, size):
    pattern = bytearray()
    counter = 0
    while len(pattern) < 1024 * 1024:
        pattern.extend(hashlib.sha256(("srcast-distributed:%d" % counter).encode()).digest())
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


def loss_blocks(receiver_index, receiver_count, count, section_blocks):
    if receiver_count * count > section_blocks - 17:
        raise ValueError("section_blocks is too small for disjoint sparse-loss sets")
    first = 17 + (receiver_index - 1) * count
    return list(range(first, first + count))


def split_receiver_indexes(count, host_count):
    groups = []
    first = 1
    for host_index in range(host_count):
        size = count // host_count + (1 if host_index < count % host_count else 0)
        groups.append(list(range(first, first + size)))
        first += size
    return groups


def percentile(values, fraction):
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def parse_metrics(text, expected_mode):
    totals = {
        "proxy_repair_payload_bytes": 0,
        "proxy_repair_pread_blocks": 0,
        "peer_blocks": 0,
    }
    rows = 0
    for line in text.splitlines():
        match = re.search(r"\bREPAIR_METRIC\s+(.*)$", line)
        if not match:
            continue
        values = dict(re.findall(r"([a-z_]+)=([a-z]+|[0-9]+)", match.group(1)))
        if values.get("mode") != expected_mode:
            raise RuntimeError("repair metric mode mismatch: %s" % line)
        for key in totals:
            totals[key] += int(values.get(key, 0))
        rows += 1
    if rows == 0:
        raise RuntimeError("proxy log contains no REPAIR_METRIC rows")
    totals["metric_rows"] = rows
    return totals


def write_results(output, results):
    output.mkdir(parents=True, exist_ok=True)
    with (output / "raw.csv").open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=RAW_FIELDS)
        writer.writeheader()
        for result in results:
            row = {key: result[key] for key in RAW_FIELDS}
            row["sha256_success"] = str(bool(row["sha256_success"])).lower()
            writer.writerow(row)

    summary_fields = ["mode", "receivers", "runs"]
    for field in METRIC_FIELDS:
        name = SUMMARY_NAMES.get(field, field)
        summary_fields.extend([name + "_median", name + "_p95"])
    summary_rows = []
    keys = sorted({(result["mode"], result["receivers"]) for result in results},
                  key=lambda item: (item[1], item[0]))
    for mode, receiver_count in keys:
        selected = [result for result in results
                    if result["mode"] == mode and result["receivers"] == receiver_count]
        row = {"mode": mode, "receivers": receiver_count, "runs": len(selected)}
        for field in METRIC_FIELDS:
            name = SUMMARY_NAMES.get(field, field)
            values = [result[field] for result in selected]
            row[name + "_median"] = statistics.median(values)
            row[name + "_p95"] = percentile(values, 0.95)
        summary_rows.append(row)
    with (output / "summary.csv").open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(summary_rows)


class DistributedRunner:
    def __init__(self, config, output, timeout, keep_transfer_files=False):
        self.config = config
        self.output = Path(output)
        self.timeout = timeout
        self.keep_transfer_files = keep_transfer_files
        ssh_options = config.get("ssh_options", ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10"])
        self.central = Host(config["central"], ssh_options)
        self.proxy = Host(config["proxy"], ssh_options)
        self.receiver_hosts = [Host(item, ssh_options) for item in config["receiver_hosts"]]
        if not self.central.local:
            raise ValueError("run distributed_compare.py on Machine A and set central.host to local")

    def role_path(self, host, run_id, role):
        return str(Path(host.repo) / self.config["work_root"] / run_id / role)

    def check_binary(self, host, name):
        path = str(Path(host.repo) / self.config["build_dir"] / name)
        host.run("test -x %s" % shlex.quote(path))
        return path

    def check_space(self, host, path, required_bytes):
        host.run("mkdir -p %s" % shlex.quote(path))
        result = host.run("df -Pk %s | awk 'NR==2 {print $4 * 1024}'" % shlex.quote(path))
        try:
            available = int(float(result.stdout.strip()))
        except ValueError as error:
            raise RuntimeError("could not determine disk space on %s" % host.name) from error
        if available < required_bytes:
            raise RuntimeError("insufficient disk space on %s: need %d, available %d" %
                               (host.name, required_bytes, available))

    def wait_registrations(self, log_path, expected, proxy_pid):
        deadline = time.monotonic() + min(self.timeout, 60)
        while time.monotonic() < deadline:
            text = self.proxy.read_text(log_path)
            endpoints = set(re.findall(r"\bregistered receiver=([0-9.]+:[0-9]+)", text))
            if len(endpoints) == expected:
                return text, sorted(endpoints)
            if not self.proxy.alive(proxy_pid):
                raise RuntimeError("proxy exited before all receivers registered")
            time.sleep(0.1)
        raise TimeoutError("timed out waiting for %d receiver registrations" % expected)

    def completed_count(self, host, receiver_base):
        pattern = str(Path(receiver_base) / "logs" / "receiver-*.log")
        command = "grep -h 'COMPLETED output=' %s 2>/dev/null | wc -l" % pattern
        return int(host.run(command).stdout.strip())

    def collect_logs(self, raw_dir, central_log, proxy_log, receiver_paths):
        raw_dir.mkdir(parents=True, exist_ok=True)
        if Path(central_log).exists():
            atomic_write(raw_dir / "central.log", Path(central_log).read_text(
                encoding="utf-8", errors="replace"))
        atomic_write(raw_dir / "proxy.log", self.proxy.read_text(proxy_log))
        for host_index, (host, base, indexes) in enumerate(receiver_paths, 1):
            for receiver_index in indexes:
                remote = str(Path(base) / "logs" / ("receiver-%03d.log" % receiver_index))
                atomic_write(raw_dir / ("receiver-%03d.log" % receiver_index),
                             host.read_text(remote))
            atomic_write(raw_dir / ("receiver-host-%d.txt" % host_index), host.name + "\n")

    def run_once(self, mode, receiver_count, repeat, file_size):
        run_id = "%s-%03d-r%02d" % (mode, receiver_count, repeat)
        raw_dir = self.output / "raw" / run_id
        central_base = self.role_path(self.central, run_id, "central")
        proxy_base = self.role_path(self.proxy, run_id, "proxy")
        groups = split_receiver_indexes(receiver_count, len(self.receiver_hosts))
        receiver_paths = [
            (host, self.role_path(host, run_id, "receiver-host-%d" % (index + 1)), groups[index])
            for index, host in enumerate(self.receiver_hosts)
        ]
        cleanup_paths = [(self.central, central_base), (self.proxy, proxy_base)] + [
            (host, base) for host, base, _ in receiver_paths
        ]
        remote_processes = []
        central_process = None
        central_handle = None
        proxy_log = str(Path(proxy_base) / "proxy.log")
        central_log = str(Path(central_base) / "central.log")

        try:
            for host, path in cleanup_paths:
                host.clean_directory(path)
            self.check_space(self.central, central_base, int(file_size * 1.2))
            self.check_space(self.proxy, proxy_base, int(file_size * 1.2))
            for host, base, indexes in receiver_paths:
                self.check_space(host, base, int(file_size * len(indexes) * 1.1))

            central_binary = self.check_binary(self.central, "central_sender")
            proxy_binary = self.check_binary(self.proxy, "site_proxy_sender")
            receiver_binaries = {
                host.name: self.check_binary(host, "receiver_agent") for host in self.receiver_hosts
            }

            source_path = Path(central_base) / ("input-%d.bin" % file_size)
            deterministic_input(source_path, file_size)
            source_sha = sha256_file(source_path)
            output_name = source_path.name

            proxy_arguments = [
                proxy_binary,
                "--multicast-ip", self.config["multicast_ip"],
                "--udp-port", self.config["udp_port"],
                "--interface-ip", self.config["proxy"]["interface_ip"],
                "--control-port", self.config["control_port"],
                "--receiver-count", receiver_count,
                "--pace-us", self.config["udp_pace_us"],
                "--gap-ms", 0,
                "--max-repair-rounds", self.config["max_repair_rounds"],
                "--section-blocks", self.config["section_blocks"],
                "--repair-mode", mode,
                "--central-listen", self.config["central_port"],
                "--cache-dir", str(Path(proxy_base) / "cache"),
            ]
            proxy_pid = self.proxy.start(proxy_arguments, proxy_log)
            remote_processes.append((self.proxy, proxy_pid))

            deadline = time.monotonic() + 15
            while "waiting for central sender on port" not in self.proxy.read_text(proxy_log):
                if not self.proxy.alive(proxy_pid):
                    raise RuntimeError("proxy failed during startup")
                if time.monotonic() >= deadline:
                    raise TimeoutError("proxy did not start listening")
                time.sleep(0.1)

            proxy_connect_ip = self.config["proxy"].get(
                "receiver_connect_ip", self.config["proxy"]["interface_ip"])
            for host, base, indexes in receiver_paths:
                host.run("mkdir -p %s %s" %
                         (shlex.quote(str(Path(base) / "logs")),
                          shlex.quote(str(Path(base) / "receivers"))))
                for receiver_index in indexes:
                    output_dir = str(Path(base) / "receivers" /
                                     ("receiver-%03d" % receiver_index))
                    host.run("mkdir -p %s" % shlex.quote(output_dir))
                    receiver_log = str(Path(base) / "logs" /
                                       ("receiver-%03d.log" % receiver_index))
                    drops = loss_blocks(receiver_index, receiver_count,
                                         self.config["drop_blocks_per_receiver"],
                                         self.config["section_blocks"])
                    receiver_arguments = [
                        receiver_binaries[host.name],
                        "--multicast-ip", self.config["multicast_ip"],
                        "--udp-port", self.config["udp_port"],
                        "--output-dir", output_dir,
                        "--proxy-ip", proxy_connect_ip,
                        "--control-port", self.config["control_port"],
                        "--interface-ip", host.interface_ip,
                    ]
                    receiver_pid = host.start(receiver_arguments, receiver_log, {
                        "SRCAST_DROP_INITIAL_BLOCKS": ",".join(map(str, drops)),
                    })
                    remote_processes.append((host, receiver_pid))

            proxy_text, endpoints = self.wait_registrations(proxy_log, receiver_count, proxy_pid)
            if "repair_mode=%s" % mode not in proxy_text:
                raise RuntimeError("proxy did not confirm repair mode %s" % mode)
            endpoint_pairs = [(value.rsplit(":", 1)[0], int(value.rsplit(":", 1)[1]))
                              for value in endpoints]
            expected_by_address = {}
            for host, _, indexes in receiver_paths:
                expected_by_address[host.interface_ip] = (
                    expected_by_address.get(host.interface_ip, 0) + len(indexes))
            for address, expected_count in expected_by_address.items():
                ports = [port for endpoint_address, port in endpoint_pairs
                         if endpoint_address == address]
                if len(ports) != expected_count or len(set(ports)) != len(ports):
                    raise RuntimeError("peer_udp_port uniqueness check failed for %s" % address)

            cpu_script = str(Path(self.proxy.repo) / "benchmark" / "process_cpu.py")
            cpu_start = json.loads(self.proxy.run(
                "python3 %s --pid %d" % (shlex.quote(cpu_script), proxy_pid)
            ).stdout)
            cpu_watch_path = str(Path(proxy_base) / "proxy-cpu.json")
            cpu_watch_pid = self.proxy.start(
                ["python3", cpu_script, "--pid", proxy_pid, "--watch", "--output",
                 cpu_watch_path], str(Path(proxy_base) / "proxy-cpu.log"))
            remote_processes.append((self.proxy, cpu_watch_pid))

            central_arguments = [
                central_binary,
                "--proxy", self.config["proxy"].get(
                    "central_connect_ip", self.config["proxy"]["interface_ip"]),
                self.config["central_port"],
                "--pace-us", self.config["central_pace_us"],
                "--section-blocks", self.config["section_blocks"],
                "--file", source_path,
            ]
            central_handle = Path(central_log).open("w", encoding="utf-8")
            start = time.monotonic()
            central_process = subprocess.Popen([str(value) for value in central_arguments],
                                               stdout=central_handle, stderr=subprocess.STDOUT,
                                               cwd=self.central.repo, text=True)
            deadline = start + self.timeout

            def completed_total():
                return sum(self.completed_count(host, base)
                           for host, base, _ in receiver_paths)

            def wait_final_completion(grace_seconds=2.0):
                grace_deadline = time.monotonic() + grace_seconds
                while time.monotonic() < grace_deadline:
                    if completed_total() == receiver_count:
                        return True
                    time.sleep(0.05)
                return completed_total() == receiver_count

            def finish_success():
                finish_time = time.monotonic()
                try:
                    cpu_result = json.loads(self.proxy.run(
                        "python3 %s --pid %d" %
                        (shlex.quote(cpu_script), proxy_pid)).stdout)
                except (RuntimeError, json.JSONDecodeError):
                    cpu_result = None
                return finish_time, cpu_result

            while True:
                if completed_total() == receiver_count:
                    finish, cpu_finish = finish_success()
                    break

                central_status = central_process.poll()
                if central_status is not None:
                    if wait_final_completion():
                        finish, cpu_finish = finish_success()
                        break
                    if central_status != 0:
                        raise RuntimeError("central sender failed before all receivers completed")
                    raise RuntimeError("central sender exited before all receivers completed")

                if not self.proxy.alive(proxy_pid):
                    if wait_final_completion():
                        finish, cpu_finish = finish_success()
                        break
                    raise RuntimeError("proxy exited before all receivers completed")

                if time.monotonic() >= deadline:
                    raise TimeoutError("timed out waiting for all receiver completions")
                time.sleep(0.1)

            central_return = central_process.wait(timeout=15)
            central_handle.close()
            central_handle = None
            if central_return != 0:
                raise RuntimeError("central sender exited with status %d" % central_return)

            for host, base, indexes in receiver_paths:
                outputs = [str(Path(base) / "receivers" / ("receiver-%03d" % index) /
                               output_name) for index in indexes]
                command = "sha256sum -- %s" % shell_join(outputs)
                lines = host.run(command, timeout=max(30, self.timeout)).stdout.splitlines()
                if len(lines) != len(outputs) or any(line.split()[0] != source_sha for line in lines):
                    raise RuntimeError("receiver SHA-256 verification failed on %s" % host.name)

            exit_deadline = time.monotonic() + 10
            while self.proxy.alive(proxy_pid) and time.monotonic() < exit_deadline:
                time.sleep(0.05)
            cpu_data = None
            cpu_deadline = time.monotonic() + 5
            while time.monotonic() < cpu_deadline:
                raw_cpu = self.proxy.read_text(cpu_watch_path)
                if raw_cpu:
                    cpu_data = json.loads(raw_cpu)
                    if cpu_data.get("process_exited"):
                        break
                time.sleep(0.05)
            if not cpu_data:
                raise RuntimeError("proxy CPU sampler produced no data")

            proxy_text = self.proxy.read_text(proxy_log)
            metrics = parse_metrics(proxy_text, mode)
            if (mode == "peer" and self.config.get("require_peer_blocks", True) and
                    metrics["peer_blocks"] <= 0):
                raise RuntimeError("peer mode did not schedule any peer blocks")
            if mode == "baseline" and metrics["peer_blocks"] != 0:
                raise RuntimeError("baseline unexpectedly reported peer blocks")

            cpu_end = cpu_finish or cpu_data["latest"]
            cpu_seconds = max(0.0, (cpu_end["total_ticks"] - cpu_start["total_ticks"]) /
                              float(cpu_start["clk_tck"]))
            wall_seconds = finish - start
            result = {
                "mode": mode,
                "receivers": receiver_count,
                "repeat": repeat,
                "file_size_bytes": file_size,
                "transfer_time_seconds": round(wall_seconds, 6),
                "proxy_avg_cpu_percent": round(cpu_seconds / wall_seconds * 100.0, 6),
                "proxy_cpu_seconds": round(cpu_seconds, 6),
                "proxy_repair_payload_bytes": metrics["proxy_repair_payload_bytes"],
                "proxy_repair_pread_blocks": metrics["proxy_repair_pread_blocks"],
                "peer_blocks": metrics["peer_blocks"],
                "sha256_success": True,
            }
            self.collect_logs(raw_dir, central_log, proxy_log, receiver_paths)
            atomic_write(raw_dir / "run.json", json.dumps({
                "result": result,
                "source_sha256": source_sha,
                "registered_endpoints": endpoints,
            }, indent=2, sort_keys=True) + "\n")
            return result
        except Exception:
            try:
                if central_handle:
                    central_handle.flush()
                self.collect_logs(raw_dir, central_log, proxy_log, receiver_paths)
            except Exception:
                pass
            raise
        finally:
            if central_process and central_process.poll() is None:
                central_process.send_signal(signal.SIGTERM)
                try:
                    central_process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    central_process.kill()
            if central_handle:
                central_handle.close()
            for host, pid in reversed(remote_processes):
                try:
                    host.stop(pid)
                except Exception as error:
                    print("cleanup warning: could not stop pid %d on %s: %s" %
                          (pid, host.name, error), file=sys.stderr)
            if not self.keep_transfer_files:
                for host, path in cleanup_paths:
                    try:
                        host.clean_directory(path)
                    except Exception as error:
                        print("cleanup warning: could not clean %s on %s: %s" %
                              (path, host.name, error), file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=ROOT / "benchmark" / "distributed_config.json",
                        type=Path)
    parser.add_argument("--output", default=ROOT / "test-runs" / "artifacts" / "distributed",
                        type=Path)
    parser.add_argument("--receivers", nargs="+", type=int, default=[20])
    parser.add_argument("--modes", nargs="+", choices=["baseline", "peer"],
                        default=["baseline", "peer"])
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--file-size", type=int, default=128 * 1024 * 1024)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--allow-large-run", action="store_true",
                        help="required for >20 receivers, >128 MiB, or repeated runs")
    parser.add_argument("--keep-transfer-files", action="store_true")
    args = parser.parse_args()
    if any(value <= 0 for value in args.receivers) or args.repetitions <= 0 or args.file_size <= 0:
        parser.error("receiver counts, repetitions, and file size must be positive")
    if (max(args.receivers) > 20 or args.file_size > 128 * 1024 * 1024 or
            args.repetitions > 1) and not args.allow_large_run:
        parser.error("large run refused; pass --allow-large-run only after calibration is stable")

    config = json.loads(args.config.read_text(encoding="utf-8"))
    validate_config(config)
    runner = DistributedRunner(config, args.output, args.timeout, args.keep_transfer_files)
    results = []
    for receiver_count in args.receivers:
        for mode in args.modes:
            for repeat in range(1, args.repetitions + 1):
                print("RUN mode=%s receivers=%d repeat=%d size=%d" %
                      (mode, receiver_count, repeat, args.file_size), flush=True)
                result = runner.run_once(mode, receiver_count, repeat, args.file_size)
                results.append(result)
                write_results(args.output, results)
                print("PASS transfer_time_seconds=%s peer_blocks=%s" %
                      (result["transfer_time_seconds"], result["peer_blocks"]), flush=True)
    print("raw_csv=%s" % (args.output / "raw.csv"))
    print("summary_csv=%s" % (args.output / "summary.csv"))


if __name__ == "__main__":
    main()
