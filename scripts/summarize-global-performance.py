#!/usr/bin/env python3
"""Summarize independent serial benchmark logs, without timing pass/fail gates.

Usage: python3 scripts/summarize-global-performance.py before/*.log
Writes CSV to stdout. Each key must occur in every supplied run; unmatched keys
are errors, not silently dropped controls. Compare separately by benchmark suite.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
import re
import statistics
import sys


HISTORICAL = re.compile(
    r"^(\S+)\s+(.+?)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(.+)$"
)


def read(path: Path) -> dict[str, tuple[int, str]]:
    observations: dict[str, tuple[int, str]] = {}
    layout = ""
    boundary_plans: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if line.startswith("boundary_plan,"):
            _, case, plan = line.split(",", 2)
            required = {"scalar": "OneRow", "point": "IndexScan {", "result": "SeqScan {"}[case.rsplit("_", 1)[1]]
            if re.search(r"\b" + re.escape(required), plan) is None:
                raise ValueError(f"{path}: {case} missing {required}")
            boundary_plans[case] = plan
            continue
        if line.startswith("conflict,"):
            if line.split(",")[-2:] != ["true", "true"]:
                raise ValueError(f"{path}: conflict/result correctness gate failed: {line}")
            continue
        if line.startswith("scenario,engine,transactions,rows_per_tx,"):
            layout = "commit"
            continue
        if line.startswith("engine,transactions,group_size,"):
            layout = "group"
            continue
        if line.startswith("scenario,rows,mean_us,"):
            layout = "columnar"
            continue
        if line.startswith("scenario,engine,table_id,"):
            layout = ""
            continue
        if layout == "commit" and line.startswith(("single,", "sequential,")):
            fields = line.split(",")
            key = "commit_" + "_".join(fields[:4])
            median, shape = int(fields[4]), "within_run=mean;" + ",".join(fields[6:])
        elif layout == "group" and line.startswith(("heap,", "lsm,", "heap+lsm,")):
            fields = line.split(",")
            key = "group_" + "_".join(fields[:3])
            median, shape = int(fields[8]), "within_run=mean;" + ",".join(fields[3:7] + fields[9:])
        elif layout == "columnar" and line.startswith(("heap-seq,", "heap-btree-point,", "lsm-scan,", "columnar,", "stale-authoritative-fallback,")):
            fields = line.split(",")
            key = "columnar_" + fields[0]
            median, shape = int(fields[2]) * 1000, "within_run=mean_us;" + ",".join(fields[1:2] + fields[3:])
        elif line.startswith("audit_csv,") and not line.startswith("audit_csv,scenario,"):
            fields = line.split(",", 11)
            key, median, shape = fields[1], int(fields[6]), ",".join(fields[2:4] + fields[8:])
        elif line.startswith("boundary_csv,") and not line.startswith("boundary_csv,scenario,"):
            fields = line.split(",")
            key, median, shape = fields[1] + "_clients" + fields[2], int(fields[5]), fields[2] + "," + fields[3]
            case = re.search(r"(w\d+_(?:scalar|point|result))$", fields[1])
            if case is None or case[1] not in boundary_plans:
                raise ValueError(f"{path}: missing boundary plan for {fields[1]}")
            shape += "," + boundary_plans[case[1]]
        elif match := HISTORICAL.fullmatch(line):
            key, rows, _iterations, _minimum, median_text, _p95, plan = match.groups()
            median, shape = int(median_text), rows + "," + plan
        else:
            continue
        if key in observations:
            raise ValueError(f"{path}: duplicate scenario {key}")
        observations[key] = median, shape
    if not observations:
        raise ValueError(f"{path}: no measurements")
    return observations


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    if len(args.logs) < 3:
        parser.error("at least three independent runs are required")
    runs = [read(path) for path in args.logs]
    keys = set(runs[0])
    for path, run in zip(args.logs, runs):
        if set(run) != keys:
            raise ValueError(f"{path}: missing={keys-set(run)} extra={set(run)-keys}")
    writer = csv.writer(sys.stdout)
    writer.writerow(["scenario", "runs", "median_of_runs_ns", "min_run_ns", "max_run_ns", "spread_pct", "shape"])
    for key in runs[0]:
        values = [run[key][0] for run in runs]
        shapes = {run[key][1] for run in runs}
        if len(shapes) != 1:
            raise ValueError(f"{key}: shape or plan changed across runs: {shapes}")
        median = statistics.median(values)
        spread = (max(values)-min(values))*100/median if median else 0
        writer.writerow([key, len(values), median, min(values), max(values), f"{spread:.2f}", shapes.pop()])


if __name__ == "__main__":
    main()
