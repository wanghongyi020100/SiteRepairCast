#!/usr/bin/env python3
"""Plot SiteRepairCast distributed baseline/peer summary CSV data."""

import argparse
import csv
from pathlib import Path


PLOTS = [
    ("transfer_time", "Total transfer time (seconds)", "transfer_time.png"),
    ("proxy_avg_cpu", "Proxy average CPU (%)", "proxy_cpu.png"),
    ("proxy_cpu_seconds", "Proxy CPU-seconds", "proxy_cpu_seconds.png"),
    ("proxy_repair_payload_bytes", "Proxy repair payload bytes", "proxy_repair_bytes.png"),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("summary_csv")
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    try:
        import matplotlib.pyplot as plt
    except ImportError as error:
        raise SystemExit("matplotlib is required: %s" % error)

    summary = Path(args.summary_csv)
    output = Path(args.output_dir) if args.output_dir else summary.parent
    output.mkdir(parents=True, exist_ok=True)
    with summary.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))
    if not rows:
        raise SystemExit("summary CSV has no rows")

    styles = {"baseline": ("Baseline", "#c44e52"), "peer": ("Peer", "#4c72b0")}
    for metric, ylabel, filename in PLOTS:
        figure, axis = plt.subplots(figsize=(7.2, 4.5))
        plotted = False
        for mode, (label, color) in styles.items():
            selected = sorted((row for row in rows if row["mode"] == mode),
                              key=lambda row: int(row["receivers"]))
            if not selected:
                continue
            x_values = [int(row["receivers"]) for row in selected]
            medians = [float(row[metric + "_median"]) for row in selected]
            p95_values = [float(row[metric + "_p95"]) for row in selected]
            upper = [max(0.0, p95 - median) for median, p95 in zip(medians, p95_values)]
            axis.errorbar(x_values, medians, yerr=[len(upper) * [0.0], upper],
                          marker="o", capsize=4, linewidth=2, label=label, color=color)
            axis.plot(x_values, p95_values, linestyle="--", linewidth=1, color=color,
                      alpha=0.55)
            plotted = True
        if not plotted:
            plt.close(figure)
            continue
        axis.set_xlabel("Receiver count")
        axis.set_ylabel(ylabel)
        axis.grid(True, alpha=0.25)
        axis.legend()
        figure.tight_layout()
        figure.savefig(output / filename, dpi=160)
        plt.close(figure)


if __name__ == "__main__":
    main()
