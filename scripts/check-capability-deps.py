#!/usr/bin/env python3
"""Check declared optional edges and the complete resolved Cargo dependency graph.

Normal and build edges are traversed at every level. Dev edges are included only
at the checked package root: a consumer does not build its dependencies' tests.
"""
import argparse
from collections import deque
import json
from pathlib import Path
import subprocess
import sys

ENGINES = {"netbadb-heap", "netbadb-lsm", "netbadb-columnar", "netbadb-change-stream"}
UPPER = {"netbadb-storage", "netbadb-core", "netbadb-executor", "netbadb-server"}
FORBIDDEN = {
    "netbadb-storage-api": ENGINES | UPPER,
    "netbadb-row-codec": ENGINES | UPPER,
    "netbadb-change-stream": (ENGINES - {"netbadb-change-stream"}) | UPPER,
    "netbadb-lsm": (ENGINES - {"netbadb-lsm", "netbadb-change-stream"}) | UPPER,
    "netbadb-columnar": (ENGINES - {"netbadb-columnar", "netbadb-change-stream"}) | UPPER,
    "netbadb-heap": (ENGINES - {"netbadb-heap", "netbadb-change-stream"}) | UPPER,
    "netbadb-query-feedback": UPPER,
    "netbadb-advisor": UPPER,
    "netbadb-planner": {"netbadb-core", "netbadb-executor", "netbadb-advisor", "netbadb-server"},
}


def inspect(metadata):
    packages = {p["id"]: p for p in metadata["packages"]}
    workspace = {packages[i]["name"]: i for i in metadata["workspace_members"]}
    errors = [f"required workspace package missing: {n}" for n in sorted(FORBIDDEN.keys() - workspace.keys())]
    paths = {str(Path(p["manifest_path"]).parent): p["id"] for p in metadata["packages"]}
    resolved = {n["id"]: n for n in metadata["resolve"]["nodes"]}

    def label(i):
        p = packages[i]
        return f'{p["name"]}@{p["version"]} ({i})'

    def declared(i):
        # Inactive optional declarations are absent from the resolved graph.
        for d in packages[i]["dependencies"]:
            target = paths.get(str(Path(d["path"]))) if d.get("path") else None
            if target:
                yield target, d["kind"] or "normal", bool(d["optional"]), d.get("rename") or d["name"]

    def actual(i):
        # Package IDs, including source and version, prevent name collisions.
        for d in resolved.get(i, {}).get("deps", []):
            for k in d["dep_kinds"]:
                yield d["pkg"], k["kind"] or "normal", False, d["name"]

    def find(start, bad, graph, include_dev):
        queue = deque([(start, [])])
        seen = {start}
        while queue:
            current, route = queue.popleft()
            for target, kind, optional, alias in graph(current):
                if kind == "dev" and (not include_dev or current != start):
                    continue
                if target not in packages:
                    continue
                step = f'{label(current)} -[{kind}{", optional" if optional else ""}, as {alias}]-> {label(target)}'
                next_route = route + [step]
                if target in bad:
                    yield "\n    ".join(next_route)
                elif target not in seen:
                    seen.add(target)
                    queue.append((target, next_route))

    for name, forbidden in FORBIDDEN.items():
        if name not in workspace:
            continue
        bad = {workspace[n] for n in forbidden if n in workspace}
        for graph_name, graph in (("declaration", declared), ("resolved", actual)):
            for scope, include_dev in (("normal/build", False), ("root dev plus normal/build", True)):
                for route in find(workspace[name], bad, graph, include_dev):
                    errors.append(f"{name}: {graph_name} {scope} forbidden path:\n    {route}")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, help="Cargo metadata JSON fixture")
    args = parser.parse_args()
    if not args.metadata:
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        toolchain = subprocess.check_output(["cargo", "--version"], text=True).strip()
        print(f"capability dependencies: current={revision} toolchain={toolchain}", flush=True)
    metadata = (json.loads(args.metadata.read_text()) if args.metadata else
                json.loads(subprocess.check_output(["cargo", "metadata", "--format-version", "1", "--offline"])))
    errors = inspect(metadata)
    if errors:
        print("Capability dependency boundaries failed:\n" + "\n".join(errors), file=sys.stderr)
        return 1
    print("Capability dependency boundaries passed (declared and resolved graphs)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
