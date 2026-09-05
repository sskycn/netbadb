#!/usr/bin/env python3
"""Round 46 real-psql surviving-base nullability acceptance."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "/private/tmp/netbadb-round46-target"))
PSQL = os.environ.get("PSQL", "/opt/local/lib/pgsql/bin/psql")
FIXTURE = TARGET / "debug/examples/sql_alter_table_fixture"


def run_fixture(probe: str, sql: str, expected: tuple[str, ...]) -> None:
    environment = dict(
        os.environ,
        CARGO_TARGET_DIR=str(TARGET),
        DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib",
        NETBADB_POSTGRES_TRACE="1",
        NETBADB_ROUND46_PROBE=probe,
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
            assert process.stdout is not None
            address = process.stdout.readline().strip()
            assert address, "fixture did not start"
            completed = subprocess.run(
                [
                    PSQL,
                    "-X",
                    "-w",
                    "-At",
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


def failed_set(probe: str, setup: str, marker: str) -> None:
    run_fixture(
        probe,
        f"""
BEGIN;
{setup}
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
\echo {marker} :SQLSTATE
SELECT id FROM projects;
\echo FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        (f"{marker} 23502", "FAILED_STATE 25P02", "ROLLBACK"),
    )


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
        "set",
        """
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = 'filled@example.test' WHERE email IS NULL;
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
COMMIT;
SELECT id, email FROM projects ORDER BY id;
""",
        ("UPDATE 1", "ALTER TABLE", "COMMIT", "1|filled@example.test"),
    )
    failed_set(
        "set-failure",
        "UPDATE projects SET email = email WHERE id = -1;",
        "SET_FAILURE_STATE",
    )
    run_fixture(
        "drop",
        """
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = email WHERE id = 1;
ALTER TABLE projects ALTER COLUMN email DROP NOT NULL;
COMMIT;
""",
        ("UPDATE 1", "ALTER TABLE", "COMMIT"),
    )
    run_fixture(
        "indexed-set",
        """
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = 'filled' WHERE email IS NULL;
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
COMMIT;
SELECT id FROM projects WHERE email = 'filled';
""",
        ("ALTER TABLE", "COMMIT", "1"),
    )
    run_fixture(
        "indexed-drop",
        """
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = email WHERE id = 1;
ALTER TABLE projects ALTER COLUMN email DROP NOT NULL;
COMMIT;
SELECT id FROM projects WHERE email = 'one@example.test';
""",
        ("ALTER TABLE", "COMMIT", "1"),
    )
    run_fixture(
        "own-update",
        """
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = 'repaired' WHERE email IS NULL;
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
COMMIT;
SELECT email FROM projects WHERE id = 1;
""",
        ("UPDATE 1", "ALTER TABLE", "repaired"),
    )
    failed_set(
        "own-insert-null",
        "UPDATE projects SET email = 'repaired' WHERE email IS NULL;\n"
        "INSERT INTO projects VALUES (4, 'old-four', NULL);",
        "INSERT_NULL_STATE",
    )
    run_fixture(
        "own-delete",
        """
\set ON_ERROR_STOP on
BEGIN;
DELETE FROM projects WHERE email IS NULL;
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
COMMIT;
SELECT id FROM projects ORDER BY id;
""",
        ("DELETE 1", "ALTER TABLE", "COMMIT", "2", "3"),
    )
    failed_set(
        "zero-row",
        "UPDATE projects SET email = email WHERE id = -1;",
        "ZERO_ROW_STATE",
    )
    run_fixture(
        "set-drop",
        """
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = 'repaired' WHERE email IS NULL;
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
ALTER TABLE projects ALTER COLUMN email DROP NOT NULL;
COMMIT;
""",
        ("UPDATE 1", "ALTER TABLE", "COMMIT"),
    )
    run_fixture(
        "drop-set",
        """
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = email WHERE id = 1;
ALTER TABLE projects ALTER COLUMN email DROP NOT NULL;
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
COMMIT;
""",
        ("UPDATE 1", "ALTER TABLE", "COMMIT"),
    )
    run_fixture(
        "rename-set",
        """
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = 'repaired' WHERE email IS NULL;
ALTER TABLE projects RENAME COLUMN email TO contact;
ALTER TABLE projects ALTER COLUMN contact SET NOT NULL;
COMMIT;
SELECT contact FROM projects WHERE id = 1;
""",
        ("UPDATE 1", "ALTER TABLE", "COMMIT", "repaired"),
    )
    run_fixture(
        "add-set",
        """
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = 'repaired' WHERE email IS NULL;
ALTER TABLE projects ADD COLUMN marker TEXT;
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
COMMIT;
SELECT id, marker FROM projects ORDER BY id;
""",
        ("UPDATE 1", "ALTER TABLE", "COMMIT", "1|", "2|", "3|"),
    )
    for probe, operation, sqlstate in (
        ("cnew-set", "SET NOT NULL", "25000"),
        ("cnew-drop", "DROP NOT NULL", "58000"),
    ):
        run_fixture(
            probe,
            f"""
BEGIN;
UPDATE projects SET legacy = legacy WHERE id = 1;
ALTER TABLE projects ADD COLUMN marker TEXT;
ALTER TABLE projects ALTER COLUMN marker {operation};
\echo CNEW_STATE :SQLSTATE
SELECT id FROM projects;
\echo FAILED_STATE :SQLSTATE
ROLLBACK;
""",
            (f"CNEW_STATE {sqlstate}", "FAILED_STATE 25P02", "ROLLBACK"),
        )
    run_fixture(
        "post-refinement-dml",
        """
BEGIN;
UPDATE projects SET email = 'repaired' WHERE email IS NULL;
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
SELECT id FROM projects;
\echo POST_REFINEMENT_STATE :SQLSTATE
DELETE FROM projects WHERE id = 1;
\echo FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        ("POST_REFINEMENT_STATE 25000", "FAILED_STATE 25P02", "ROLLBACK"),
    )
    run_fixture(
        "extended-set",
        """
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = 'repaired' WHERE email IS NULL;
\\bind
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
COMMIT;
""",
        ("UPDATE 1", "ALTER TABLE", "COMMIT"),
    )
    print(f"{version}: Round 46 surviving-base SET/DROP NOT NULL SQL acceptance PASS")


if __name__ == "__main__":
    main()
