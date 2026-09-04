#!/usr/bin/env python3
"""Round 36 real-psql staged indexed-nullability acceptance."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "/private/tmp/netbadb-round36-target"))
PSQL = os.environ.get("PSQL", "/opt/local/lib/pgsql/bin/psql")
FIXTURE = TARGET / "debug/examples/sql_alter_table_fixture"


def run_fixture(probe: str, sql: str, expected: tuple[str, ...]) -> None:
    environment = dict(
        os.environ,
        CARGO_TARGET_DIR=str(TARGET),
        DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib",
        NETBADB_POSTGRES_TRACE="1",
        NETBADB_ROUND36_PROBE=probe,
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
    version = subprocess.check_output(
        [PSQL, "--version"],
        text=True,
        env=dict(os.environ, DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib"),
    ).strip()
    assert "17.11" in version, version
    build_environment = dict(
        os.environ,
        CARGO_TARGET_DIR=str(TARGET),
        DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib",
    )
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
        env=build_environment,
        check=True,
    )

    run_fixture(
        "replacement",
        r"""
BEGIN;
DROP INDEX projects_name_idx;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
ALTER TABLE projects ALTER COLUMN name SET NOT NULL;
\echo DROP_FIRST_STATE :SQLSTATE
ROLLBACK;

BEGIN;
ALTER TABLE projects ADD COLUMN migration_marker TEXT;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
UPDATE projects SET migration_marker = 'done';
DROP INDEX projects_name_idx;
UPDATE projects SET migration_marker = 'late';
\echo DML_AFTER_STATE :SQLSTATE
ROLLBACK;

BEGIN;
ALTER TABLE projects ADD COLUMN migration_marker TEXT;
UPDATE projects SET migration_marker = 'done';
DROP INDEX projects_name_idx;
ALTER TABLE projects ALTER COLUMN name SET NOT NULL;
\echo PARTIAL_STATE :SQLSTATE
SELECT id FROM projects;
\echo FAILED_STATE :SQLSTATE
ROLLBACK;

BEGIN;
ALTER TABLE projects ADD COLUMN migration_marker TEXT;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
UPDATE projects SET migration_marker = 'done';
DROP INDEX projects_name_idx;
COMMIT;
\echo COMMIT_GATE_STATE :SQLSTATE
ROLLBACK;

\set ON_ERROR_STOP on
BEGIN;
ALTER TABLE projects ADD COLUMN migration_marker TEXT;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
UPDATE projects SET migration_marker = 'done';
DROP INDEX projects_name_idx;
ALTER TABLE projects ALTER COLUMN name SET NOT NULL;
CREATE INDEX projects_name_idx ON projects(name);
COMMIT;
SELECT id, name, migration_marker FROM projects ORDER BY id;
""",
        (
            "DROP_FIRST_STATE 25000",
            "DML_AFTER_STATE 25000",
            "PARTIAL_STATE 23502",
            "FAILED_STATE 25P02",
            "COMMIT_GATE_STATE 25000",
            "1|one|done",
            "2|filled|done",
        ),
    )
    run_fixture(
        "no-replacement",
        r"""
\set ON_ERROR_STOP on
BEGIN;
ALTER TABLE projects ADD COLUMN migration_marker TEXT;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
UPDATE projects SET migration_marker = 'done';
DROP INDEX projects_name_idx;
ALTER TABLE projects ALTER COLUMN name SET NOT NULL;
COMMIT;
SELECT id, name, migration_marker FROM projects ORDER BY id;
""",
        ("1|one|done", "2|filled|done"),
    )
    print(f"{version}: Round 36 staged DROP/ALTER/CREATE and required negatives PASS")


if __name__ == "__main__":
    main()
