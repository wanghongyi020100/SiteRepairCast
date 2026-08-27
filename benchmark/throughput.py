
import argparse
import csv
import datetime as dt
import json
import math
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "benchmark" / "run.py"


def parse_int_list(text):
    values = []
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        value = int(item)
        if value <= 0:
            raise argparse.ArgumentTypeError("values must be positive integers")
        values.append(value)
    if not values:
        raise argparse.ArgumentTypeError("list must not be empty")
    return values


def parse_float_list(text):
    values = []
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        value = float(item)
        if value <= 0 or value >= 100:
            raise argparse.ArgumentTypeError("loss percent must be in (0, 100)")
        values.append(value)
    return values


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percent / 100 * len(ordered)) - 1)
    return ordered[index]


def fmt(value, digits=3):
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def make_profile(args):
    scenarios = []

    for receivers in args.receivers:
        scenarios.append({
            "id": f"throughput-clean-{receivers}",
            "kind": "transfer",
            "receivers": receivers,
            "deterministic_initial_data_drop_percent": 0
        })

    # benchmark/run.py currently expects loss repair in transfer scenarios
    # to exercise multicast repair, so skip 1-receiver loss cases.
    for loss in args.loss_percent:
        loss_tag = str(loss).replace(".", "p")
        for receivers in args.receivers:
            if receivers < 2:
                continue
            scenarios.append({
                "id": f"throughput-loss-{loss_tag}pct-{receivers}",
                "kind": "transfer",
                "receivers": receivers,
                "deterministic_initial_data_drop_percent": loss
            })

    return {
        "name": "throughput",
        "description": (
            "SiteRepairCast end-to-end throughput benchmark. "
            "Measures clean fan-out and deterministic-loss overhead."
        ),
        "file_size_bytes": args.size_mib * 1024 * 1024,
        "repetitions": args.repetitions,
        "section_blocks": args.section_blocks,
        "udp_pace_us": args.udp_pace_us,
        "central_pace_us": args.central_pace_us,
        "max_repair_rounds": args.max_repair_rounds,
        "timeout_seconds": args.timeout_seconds,
        "scenarios": scenarios
    }


def load_metrics(path):
    rows = []
    with path.open("r", encoding="utf-8") as source:
        for line in source:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def enrich_metric(row):
    result = dict(row)

    completion = result.get("receiver_completion_ms") or []
    completion = [value for value in completion if value is not None]

    all_receivers_ms = max(completion) if completion else None
    file_bytes = result.get("file_bytes")
    receivers = result.get("receivers")

    result["all_receivers_completion_ms"] = all_receivers_ms

    if result.get("success") and all_receivers_ms and file_bytes and receivers:
        seconds = all_receivers_ms / 1000.0
        file_mib = file_bytes / (1024 * 1024)

        # Main metric: one file distributed to the complete receiver group.
        result["distribution_throughput_mib_s"] = file_mib / seconds

        # Logical aggregate amount successfully delivered to all receivers.
        # This is deliberately labelled "aggregate verified delivery", not bandwidth.
        result["aggregate_verified_delivery_mib_s"] = (
            file_mib * receivers / seconds
        )
    else:
        result["distribution_throughput_mib_s"] = None
        result["aggregate_verified_delivery_mib_s"] = None

    return result


def summarize(rows):
    grouped = {}

    for row in rows:
        grouped.setdefault(row["scenario"], []).append(row)

    summary = []
    for scenario, runs in sorted(grouped.items()):
        passed = [
            row for row in runs
            if row.get("success")
            and row.get("sha256_verified")
            and row.get("distribution_throughput_mib_s") is not None
        ]

        distribution = [
            row["distribution_throughput_mib_s"] for row in passed
        ]
        aggregate_delivery = [
            row["aggregate_verified_delivery_mib_s"] for row in passed
        ]
        completion = [
            row["all_receivers_completion_ms"] for row in passed
        ]

        first = runs[0]
        summary.append({
            "scenario": scenario,
            "receivers": first.get("receivers"),
            "configured_loss_percent": first.get(
                "configured_initial_data_drop_percent", 0
            ),
            "runs": len(runs),
            "passed": len(passed),
            "sha256_verified_runs": sum(
                bool(row.get("sha256_verified")) for row in runs
            ),
            "completion_median_ms": (
                statistics.median(completion) if completion else None
            ),
            "completion_p95_ms": (
                percentile(completion, 95) if completion else None
            ),
            "distribution_throughput_median_mib_s": (
                statistics.median(distribution) if distribution else None
            ),
            "distribution_throughput_p95_mib_s": (
                percentile(distribution, 95) if distribution else None
            ),
            "aggregate_verified_delivery_median_mib_s": (
                statistics.median(aggregate_delivery)
                if aggregate_delivery else None
            ),
            "intentional_drops_median": (
                statistics.median(
                    [row.get("intentional_drops", 0) for row in passed]
                )
                if passed else None
            ),
            "repair_multicast_packets_median": (
                statistics.median(
                    [row.get("repair_multicast_packets", 0) for row in passed]
                )
                if passed else None
            )
        })

    return summary


def loss_penalties(summary):
    clean = {}
    losses = []

    for row in summary:
        receivers = row["receivers"]
        loss = row["configured_loss_percent"] or 0
        throughput = row["distribution_throughput_median_mib_s"]

        if throughput is None:
            continue

        if float(loss) == 0:
            clean[receivers] = throughput
        else:
            losses.append(row)

    result = []
    for row in losses:
        receivers = row["receivers"]
        clean_throughput = clean.get(receivers)
        loss_throughput = row["distribution_throughput_median_mib_s"]

        penalty = None
        if clean_throughput:
            penalty = (
                (clean_throughput - loss_throughput)
                / clean_throughput
                * 100.0
            )

        result.append({
            "receivers": receivers,
            "loss_percent": row["configured_loss_percent"],
            "clean_median_mib_s": clean_throughput,
            "loss_median_mib_s": loss_throughput,
            "throughput_penalty_percent": penalty
        })

    return result


def write_csv(path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fields = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def make_report(profile, summary, penalties, output_dir):
    file_mib = profile["file_size_bytes"] / (1024 * 1024)

    lines = [
        "# SiteRepairCast throughput report",
        "",
        f"- File size: `{file_mib:.0f} MiB`",
        f"- Repetitions per scenario: `{profile['repetitions']}`",
        f"- UDP pace: `{profile['udp_pace_us']} us/packet`",
        f"- Central pace: `{profile['central_pace_us']} us/block`",
        f"- Section size: `{profile['section_blocks']} blocks`",
        "",
        "## Main results",
        "",
        "The main throughput is measured from immediately before `central_sender` starts "
        "until the last Receiver exposes the atomically renamed final file.",
        "",
        "| Scenario | Receiver | Loss | Pass | All receivers median | "
        "Distribution throughput median | Aggregate verified delivery | SHA-256 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    ]

    for row in summary:
        lines.append(
            "| {scenario} | {receivers} | {loss}% | {passed}/{runs} | "
            "{completion} ms | {throughput} MiB/s | {aggregate} MiB/s | "
            "{sha}/{runs} |".format(
                scenario=row["scenario"],
                receivers=row["receivers"],
                loss=fmt(float(row["configured_loss_percent"] or 0), 3),
                passed=row["passed"],
                runs=row["runs"],
                completion=fmt(row["completion_median_ms"]),
                throughput=fmt(
                    row["distribution_throughput_median_mib_s"]
                ),
                aggregate=fmt(
                    row["aggregate_verified_delivery_median_mib_s"]
                ),
                sha=row["sha256_verified_runs"]
            )
        )

    lines.extend([
        "",
        "## Loss overhead",
        "",
        "| Receiver | Loss | Clean median | Loss median | Throughput penalty |",
        "| ---: | ---: | ---: | ---: | ---: |"
    ])

    if penalties:
        for row in penalties:
            lines.append(
                "| {receivers} | {loss}% | {clean} MiB/s | {loss_tp} MiB/s | "
                "{penalty}% |".format(
                    receivers=row["receivers"],
                    loss=fmt(float(row["loss_percent"]), 3),
                    clean=fmt(row["clean_median_mib_s"]),
                    loss_tp=fmt(row["loss_median_mib_s"]),
                    penalty=fmt(row["throughput_penalty_percent"])
                )
            )
    else:
        lines.append("| - | - | - | - | - |")

    lines.extend([
        "",
        "## Metric boundary",
        "",
        "- `distribution_throughput_mib_s = file_size / last_receiver_completion_time`.",
        "- `aggregate_verified_delivery_mib_s = file_size * receiver_count / "
        "last_receiver_completion_time`; it is logical delivered payload, not NIC wire bandwidth.",
        "- Process startup and Receiver registration happen before timing starts.",
        "- Completion is observed only after the Receiver has produced the final atomically renamed file.",
        "- Every successful run is also checked by the existing benchmark runner with SHA-256.",
        "- Loss is the existing deterministic application-layer DATA-drop hook, not real network loss.",
        "- These are single-host loopback results unless you run the same script across separate hosts.",
        "",
        f"Raw runner output: `{output_dir / 'raw' / 'metrics.jsonl'}`",
        f"Raw throughput rows: `{output_dir / 'throughput_raw.csv'}`",
        f"Summary CSV: `{output_dir / 'throughput_summary.csv'}`",
        ""
    ])

    return "\n".join(lines)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--size-mib", type=int, default=256,
        help="file size for each run; default: 256"
    )
    parser.add_argument(
        "--repetitions", type=int, default=5,
        help="runs per scenario; default: 5"
    )
    parser.add_argument(
        "--receivers", type=parse_int_list, default=parse_int_list("1,2,4,8"),
        help="comma-separated receiver counts; default: 1,2,4,8"
    )
    parser.add_argument(
        "--loss-percent", type=parse_float_list, default=[],
        help="comma-separated deterministic loss percentages; default: disabled"
    )
    parser.add_argument(
        "--udp-pace-us", type=int, default=0,
        help="proxy UDP packet pacing in microseconds; default: 0"
    )
    parser.add_argument(
        "--central-pace-us", type=int, default=0,
        help="central sender pacing in microseconds; default: 0"
    )
    parser.add_argument(
        "--section-blocks", type=int, default=4096,
        help="blocks per Section; default: 4096"
    )
    parser.add_argument(
        "--max-repair-rounds", type=int, default=3,
        help="max UDP repair rounds; default: 3"
    )
    parser.add_argument(
        "--timeout-seconds", type=int, default=300,
        help="timeout for one run; default: 300"
    )
    parser.add_argument(
        "--no-build", action="store_true",
        help="reuse benchmark/run.py's existing Release binaries"
    )
    parser.add_argument(
        "--build-dir", type=Path,
        default=ROOT / "build-benchmark-validation",
        help="build directory used by benchmark/run.py"
    )
    parser.add_argument(
        "--output", type=Path,
        help="output directory; default: artifacts/throughput/<timestamp>"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.size_mib <= 0:
        raise SystemExit("--size-mib must be positive")
    if args.repetitions <= 0:
        raise SystemExit("--repetitions must be positive")
    if args.udp_pace_us < 0 or args.central_pace_us < 0:
        raise SystemExit("pace values must be >= 0")
    if args.section_blocks <= 0 or args.max_repair_rounds <= 0:
        raise SystemExit("section/repair values must be positive")
    if not RUNNER.is_file():
        raise SystemExit(f"missing existing benchmark runner: {RUNNER}")

    profile = make_profile(args)

    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = (
        args.output.resolve()
        if args.output
        else (ROOT / "artifacts" / "throughput" / timestamp).resolve()
    )

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if output_dir.exists():
        raise SystemExit(f"output directory already exists: {output_dir}")

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix="-srcast-throughput.json",
        encoding="utf-8",
        delete=False
    ) as profile_file:
        json.dump(profile, profile_file, indent=2)
        profile_file.write("\n")
        profile_path = Path(profile_file.name)

    command = [
        sys.executable,
        str(RUNNER),
        "--profile", str(profile_path),
        "--output", str(output_dir),
        "--build-dir", str(args.build_dir.resolve()),
        "--keep-going"
    ]
    if args.no_build:
        command.append("--no-build")

    print("Running:")
    print("  " + " ".join(command))
    print()

    try:
        completed = subprocess.run(command, cwd=ROOT, check=False)
    finally:
        try:
            profile_path.unlink()
        except FileNotFoundError:
            pass

    metrics_path = output_dir / "raw" / "metrics.jsonl"
    if not metrics_path.is_file():
        raise SystemExit(
            f"benchmark runner did not produce metrics: {metrics_path}"
        )

    rows = [enrich_metric(row) for row in load_metrics(metrics_path)]
    summary = summarize(rows)
    penalties = loss_penalties(summary)

    write_csv(output_dir / "throughput_raw.csv", rows)
    write_csv(output_dir / "throughput_summary.csv", summary)
    write_csv(output_dir / "throughput_loss_penalty.csv", penalties)

    report = make_report(profile, summary, penalties, output_dir)
    (output_dir / "throughput_report.md").write_text(
        report, encoding="utf-8"
    )

    print()
    print("Throughput summary")
    print(
        f"{'scenario':34} {'rx':>3} {'loss':>7} {'pass':>7} "
        f"{'median MiB/s':>14} {'aggregate MiB/s':>17}"
    )
    print("-" * 90)

    for row in summary:
        print(
            f"{row['scenario'][:34]:34} "
            f"{row['receivers']:>3} "
            f"{float(row['configured_loss_percent'] or 0):>6.2f}% "
            f"{row['passed']:>3}/{row['runs']:<3} "
            f"{fmt(row['distribution_throughput_median_mib_s']):>14} "
            f"{fmt(row['aggregate_verified_delivery_median_mib_s']):>17}"
        )

    print()
    print(f"Report: {output_dir / 'throughput_report.md'}")
    print(f"Summary: {output_dir / 'throughput_summary.csv'}")

    failures = [row for row in rows if not row.get("success")]
    if completed.returncode != 0 or failures:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
