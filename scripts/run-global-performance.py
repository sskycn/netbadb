#!/usr/bin/env python3
"""Run benchmarks serially with macOS /usr/bin/time -l observations.

Build optimized Cargo bench targets first, and copy their executables into a
directory by target name, or use --build to create those immutable copies first.
A separate binary directory for each revision avoids rebuilding during
measurements. Output/fixtures must stay outside the repository.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


def build_binaries(directory: Path, targets: set[str]) -> None:
    """Finish all compilation before any timings, preserving immutable copies."""
    directory.mkdir(parents=True, exist_ok=False)
    environment = os.environ.copy()
    environment["CARGO_PROFILE_BENCH_DEBUG"] = "1"
    for package in ["netbadb-core", "netbadb-server"]:
        selected = sorted(name for name in targets if
                          (name == "global_boundary") == (package == "netbadb-server"))
        if not selected:
            continue
        command = ["cargo", "bench", "-p", package]
        for target in selected:
            command.extend(["--bench", target])
        command.extend(["--no-run", "--message-format=json"])
        log_path = directory / f"build-{package}.log"
        print(f"BUILD {package}: {', '.join(selected)}", flush=True)
        with log_path.open("w") as log:
            status = subprocess.run(command, env=environment, stdout=log, stderr=subprocess.STDOUT).returncode
        if status:
            raise RuntimeError(f"build exited {status}: {log_path}")
        found = set()
        for line in log_path.read_text().splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("reason") != "compiler-artifact" or not event.get("executable"):
                continue
            name = event["target"]["name"]
            if name in selected:
                shutil.copy2(event["executable"], directory / name)
                found.add(name)
        if found != set(selected):
            raise RuntimeError(f"missing benchmark executables: {set(selected) - found}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binaries", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--build", action="store_true", help="build and preserve executables in a new binary directory before measuring")
    parser.add_argument("--rows", default="1000,10000,100000")
    parser.add_argument("--suites", default="reads,values,pages,joins,writes,partitions,lifecycle,boundary,boundary_null,commit,group,columnar")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("runs must be positive")
    programs = {
        "reads": "phase7_baseline", "joins": "phase7_baseline",
        "writes": "phase7_baseline", "partitions": "phase7_baseline",
        "lifecycle": "phase7_baseline", "boundary": "global_boundary",
        "values": "phase7_baseline",
        "pages": "phase7_baseline",
        "boundary_null": "global_boundary",
        "commit": "global_commit_pipeline_phase3b",
        "group": "global_group_commit_phase3c", "columnar": "columnar_phase1",
    }
    suites = args.suites.split(",")
    if any(suite not in programs for suite in suites):
        parser.error("unknown suite")
    if len(set(suites)) != len(suites):
        parser.error("duplicate suites would overwrite measurements")
    if args.output.exists():
        parser.error("output directory must be new")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    lock = (args.output.parent / ".measurement.lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if args.build:
        build_binaries(args.binaries, {programs[suite] for suite in suites})
    args.output.mkdir(parents=True, exist_ok=False)
    outcomes = []
    for run in range(1, args.runs + 1):
        for suite in suites:
            binary = args.binaries.resolve() / programs[suite]
            environment = os.environ.copy()
            environment.pop("NETBADB_BENCH_SUITE", None)
            environment.pop("NETBADB_BOUNDARY_NULLS", None)
            if suite == "boundary_null":
                environment["NETBADB_BOUNDARY_NULLS"] = "1"
            if programs[suite] == "phase7_baseline":
                environment["NETBADB_BENCH_SUITE"] = suite
                environment["NETBADB_AUDIT_ROWS"] = args.rows
            name = f"{suite}-run{run}"
            print(f"START {name}", flush=True)
            # All database resources, including historical benchmark sidecars,
            # belong to this one temporary directory and are removed on exit.
            with tempfile.TemporaryDirectory(prefix="fixtures-", dir=args.output) as temporary:
                environment["TMPDIR"] = temporary
                start = time.monotonic()
                with (args.output / f"{name}.log").open("w") as log:
                    status = subprocess.run(["/usr/bin/time", "-l", str(binary)], env=environment, stdout=log, stderr=subprocess.STDOUT).returncode
                elapsed = time.monotonic() - start
            outcomes.append({"name": name, "status": status, "seconds": elapsed, "binary": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "rows": args.rows})
            (args.output / "runs.json").write_text(json.dumps(outcomes, indent=2))
            print(f"DONE {name} exit={status} seconds={elapsed:.3f}", flush=True)
            if status:
                raise SystemExit(f"failed run: {args.output / (name + '.log')}")


if __name__ == "__main__":
    main()
