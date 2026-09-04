#!/usr/bin/env python3
"""Round 37 real-psql DROP-first current-behavior baselines."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "/private/tmp/netbadb-round37-target"))
PSQL = os.environ.get("PSQL", "/opt/local/lib/pgsql/bin/psql")
FIXTURE = TARGET / "debug/examples/sql_alter_table_fixture"


def run_fixture(probe: str, sql: str, expected: tuple[str, ...]) -> None:
    environment = dict(
        os.environ,
        CARGO_TARGET_DIR=str(TARGET),
        DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib",
        NETBADB_POSTGRES_TRACE="1",
        NETBADB_ROUND37_PROBE=probe,
    )
    with tempfile.TemporaryFile(mode="w+") as trace:
        process = subprocess.Popen(
            [str(FIXTURE)],
            cwd=ROOT,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=trace,
            text=True,
        )
        try:
            address = process.stdout.readline().strip()
            assert address, "fixture did not start"
            completed = subprocess.run(
                [
                    PSQL,
                    "-X",
                    "-w",
                    "-qAt",
                    "-v",
                    "ON_ERROR_STOP=0",
                    f"postgresql://netbadb@{address}/netbadb",
                ],
                input=sql,
                text=True,
                capture_output=True,
                env=environment,
                timeout=60,
            )
            transcript = completed.stdout + completed.stderr
            assert completed.returncode == 0, transcript
            for marker in expected:
                assert marker in transcript, transcript
        finally:
            output, _ = process.communicate(timeout=60)
            trace.seek(0)
            diagnostics = trace.read()
            assert process.returncode == 0, diagnostics
            assert "REOPEN PASS" in output, output
            print(output.strip())


def main() -> None:
    environment = dict(os.environ, DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib")
    version = subprocess.check_output([PSQL, "--version"], text=True, env=environment).strip()
    assert "17.11" in version, version
    subprocess.run(
        [
            "cargo",
            "build",
            "--offline",
            "-p",
            "netbadb-server",
            "--example",
            "sql_alter_table_fixture",
        ],
        cwd=ROOT,
        env=dict(environment, CARGO_TARGET_DIR=str(TARGET)),
        check=True,
    )

    run_fixture(
        "ordinary",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_name_idx;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
COMMIT;
\echo ORDINARY_COMMIT_STATE :SQLSTATE
SELECT id, name FROM projects ORDER BY id;
""",
        ("ORDINARY_COMMIT_STATE 00000", "1|one", "2|filled"),
    )
    run_fixture(
        "negative",
        r"""
BEGIN;
DROP INDEX projects_name_idx;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
ALTER TABLE projects ALTER COLUMN name SET NOT NULL;
\echo DROP_FIRST_ALTER_STATE :SQLSTATE
ROLLBACK;
""",
        ("DROP_FIRST_ALTER_STATE 00000",),
    )
    print(f"{version}: Round 37 ordinary commit and Round 39 routed ALTER rollback PASS")


if __name__ == "__main__":
    main()
