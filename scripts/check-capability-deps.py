#!/usr/bin/env python3
"""Check capability boundaries from Cargo package metadata, including optional edges."""
import json
import subprocess
import sys

metadata = json.loads(subprocess.check_output([
    "cargo", "metadata", "--no-deps", "--format-version", "1", "--offline"
]))
packages = {package["name"]: package for package in metadata["packages"]}
engines = {"netbadb-heap", "netbadb-lsm", "netbadb-columnar", "netbadb-change-stream"}
facade = "netbadb-storage"
core = "netbadb-core"
forbidden = {
    "netbadb-storage-api": engines | {facade, core, "netbadb-executor"},
    "netbadb-row-codec": engines | {facade, core, "netbadb-executor"},
    "netbadb-change-stream": {facade, core, "netbadb-heap", "netbadb-lsm", "netbadb-columnar"},
    "netbadb-lsm": {facade, core, "netbadb-heap", "netbadb-columnar"},
    "netbadb-columnar": {facade, core, "netbadb-heap", "netbadb-lsm"},
    "netbadb-heap": {facade, core, "netbadb-lsm", "netbadb-columnar"},
    "netbadb-query-feedback": {facade, core, "netbadb-executor"},
    "netbadb-advisor": {facade, core, "netbadb-executor"},
    "netbadb-planner": {core, "netbadb-executor", "netbadb-advisor"},
}


def edges(package_name, include_dev):
    """Cargo reports actual package names even for renamed dependencies."""
    for dep in packages[package_name]["dependencies"]:
        if dep["kind"] == "dev" and not include_dev:
            continue
        if dep["name"] in packages:
            yield dep["name"], dep["kind"] or "normal", dep["optional"]


def find_forbidden(start, bad, include_dev):
    stack = [(start, [])]
    seen = set()
    while stack:
        current, path = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        for target, kind, optional in edges(current, include_dev and current == start):
            step = f"{current} -[{kind}{', optional' if optional else ''}]-> {target}"
            if target in bad:
                return path + [step]
            stack.append((target, path + [step]))
    return None


errors = []
for name, bad in forbidden.items():
    if name not in packages:
        continue
    # Production graph includes all normal and build edges, even optional ones.
    path = find_forbidden(name, bad, include_dev=False)
    if path:
        errors.append("production: " + " / ".join(path))
    # Capability tests also stay independent. No dev-dependency exception is
    # currently needed; if one becomes justified, list it explicitly here.
    path = find_forbidden(name, bad, include_dev=True)
    if path:
        errors.append("test: " + " / ".join(path))
if errors:
    print("Forbidden capability dependencies:", *errors, sep="\n", file=sys.stderr)
    sys.exit(1)
print("Capability dependency boundaries passed")
