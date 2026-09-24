#!/usr/bin/env python3
"""Isolated positive/negative metadata fixtures for the dependency checker."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
import importlib.util

SPEC = importlib.util.spec_from_file_location("checker", Path(__file__).with_name("check-capability-deps.py"))
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


def fixture():
    names = sorted(set(checker.FORBIDDEN) | {"netbadb-storage", "netbadb-core", "netbadb-executor", "netbadb-server"})
    packages = [
        {"id": f"path+file:///fixture/{n}#0.1.0", "name": n, "version": "0.1.0",
         "manifest_path": f"/fixture/{n}/Cargo.toml", "dependencies": []}
        for n in names
    ]
    return {"packages": packages, "workspace_members": [p["id"] for p in packages],
            "resolve": {"nodes": [{"id": p["id"], "deps": []} for p in packages]}}


def add_edge(data, source, target, *, kind=None, optional=False, alias=None, resolved=True):
    by_name = {p["name"]: p for p in data["packages"]}
    source_pkg, target_pkg = by_name[source], by_name[target]
    source_pkg["dependencies"].append({
        "name": target, "path": str(Path(target_pkg["manifest_path"]).parent),
        "kind": kind, "optional": optional, "rename": alias,
    })
    if resolved:
        node = next(n for n in data["resolve"]["nodes"] if n["id"] == source_pkg["id"])
        node["deps"].append({"name": alias or target, "pkg": target_pkg["id"],
                             "dep_kinds": [{"kind": kind, "target": None}]})


class CheckerFixtures(unittest.TestCase):
    def assert_result(self, data, expected, fragment=None):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metadata.json"
            path.write_text(json.dumps(data))
            result = subprocess.run([sys.executable, str(Path(__file__).with_name("check-capability-deps.py")),
                                     "--metadata", str(path)], capture_output=True, text=True)
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        if fragment:
            self.assertIn(fragment, result.stderr)

    def test_legal(self):
        data = fixture()
        add_edge(data, "netbadb-advisor", "netbadb-lsm")
        add_edge(data, "netbadb-lsm", "netbadb-change-stream")
        self.assert_result(data, 0)

    def test_direct(self):
        data = fixture()
        add_edge(data, "netbadb-row-codec", "netbadb-core")
        self.assert_result(data, 1, "netbadb-row-codec@0.1.0")

    def test_renamed(self):
        data = fixture()
        add_edge(data, "netbadb-row-codec", "netbadb-core", alias="renamed_core")
        self.assert_result(data, 1, "as renamed_core")

    def test_transitive_external_adapter(self):
        data = fixture()
        adapter = {"id": "path+file:///external/adapter#1.0.0", "name": "adapter",
                   "version": "1.0.0", "manifest_path": "/external/adapter/Cargo.toml",
                   "dependencies": []}
        data["packages"].append(adapter)
        data["resolve"]["nodes"].append({"id": adapter["id"], "deps": []})
        add_edge(data, "netbadb-row-codec", "adapter")
        add_edge(data, "adapter", "netbadb-core")
        self.assert_result(data, 1, "adapter@1.0.0")

    def test_optional_inactive(self):
        data = fixture()
        add_edge(data, "netbadb-row-codec", "netbadb-core", optional=True, resolved=False)
        self.assert_result(data, 1, "normal, optional")

    def test_build(self):
        data = fixture()
        add_edge(data, "netbadb-row-codec", "netbadb-core", kind="build")
        self.assert_result(data, 1, "[build")

    def test_root_dev(self):
        data = fixture()
        add_edge(data, "netbadb-row-codec", "netbadb-core", kind="dev")
        self.assert_result(data, 1, "[dev")

    def test_missing_required(self):
        data = fixture()
        victim = next(p for p in data["packages"] if p["name"] == "netbadb-advisor")
        data["workspace_members"].remove(victim["id"])
        self.assert_result(data, 1, "required workspace package missing: netbadb-advisor")

    def test_external_same_name_is_not_workspace_identity(self):
        data = fixture()
        external = {"id": "registry+https://example.test/index#netbadb-core@9.0.0",
                    "name": "netbadb-core", "version": "9.0.0",
                    "manifest_path": "/external/fake/Cargo.toml", "dependencies": []}
        data["packages"].append(external)
        data["resolve"]["nodes"].append({"id": external["id"], "deps": []})
        source = next(p for p in data["packages"] if p["name"] == "netbadb-row-codec")
        node = next(n for n in data["resolve"]["nodes"] if n["id"] == source["id"])
        node["deps"].append({"name": "fake", "pkg": external["id"],
                             "dep_kinds": [{"kind": None, "target": None}]})
        self.assert_result(data, 0)


if __name__ == "__main__":
    unittest.main()
