#!/usr/bin/env python3
"""Round 39/42 real-psql DROP-first migration acceptance."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "/private/tmp/netbadb-round39-target"))
PSQL = os.environ.get("PSQL", "/opt/local/lib/pgsql/bin/psql")
FIXTURE = TARGET / "debug/examples/sql_alter_table_fixture"


def run_fixture(
    probe: str,
    sql: str,
    expected: tuple[str, ...],
    round_number: int = 39,
) -> None:
    environment = dict(
        os.environ,
        CARGO_TARGET_DIR=str(TARGET),
        DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib",
        NETBADB_POSTGRES_TRACE="1",
    )
    environment[f"NETBADB_ROUND{round_number}_PROBE"] = probe
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
        "replacement",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_name_idx;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
ALTER TABLE projects ALTER COLUMN name SET NOT NULL;
CREATE INDEX projects_name_idx ON projects(name);
COMMIT;
SELECT id, name FROM projects ORDER BY id;
\d projects
""",
        (
            "BEGIN",
            "DROP INDEX",
            "UPDATE 1",
            "ALTER TABLE",
            "CREATE INDEX",
            "COMMIT",
            "1|one",
            "2|filled",
            "name|text||not null|",
        ),
    )
    run_fixture(
        "no-replacement",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_name_idx;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
ALTER TABLE projects ALTER COLUMN name SET NOT NULL;
COMMIT;
SELECT id, name FROM projects ORDER BY id;
""",
        ("ALTER TABLE", "COMMIT", "2|filled"),
    )
    run_fixture(
        "no-alter",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_name_idx;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
COMMIT;
SELECT id, name FROM projects ORDER BY id;
""",
        ("DROP INDEX", "UPDATE 1", "COMMIT", "2|filled"),
    )
    run_fixture(
        "net-noop",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_name_idx;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
ALTER TABLE projects RENAME TO migration_projects;
ALTER TABLE migration_projects RENAME TO projects;
COMMIT;
""",
        ("DROP INDEX", "UPDATE 1", "ALTER TABLE", "COMMIT"),
    )
    run_fixture(
        "partial",
        r"""
BEGIN;
DROP INDEX projects_name_idx;
UPDATE projects SET name = 'one-updated' WHERE id = 1;
ALTER TABLE projects ALTER COLUMN name SET NOT NULL;
\echo PARTIAL_STATE :SQLSTATE
SELECT id FROM projects;
\echo FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        ("PARTIAL_STATE 23502", "FAILED_STATE 25P02", "ROLLBACK"),
    )
    run_fixture(
        "dml-after",
        r"""
BEGIN;
DROP INDEX projects_name_idx;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
ALTER TABLE projects ALTER COLUMN name SET NOT NULL;
UPDATE projects SET name = 'late' WHERE id = 1;
\echo DML_AFTER_STATE :SQLSTATE
SELECT id FROM projects;
\echo FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        ("DML_AFTER_STATE 25000", "FAILED_STATE 25P02", "ROLLBACK"),
    )
    run_fixture(
        "rollback",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_name_idx;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
ALTER TABLE projects ALTER COLUMN name SET NOT NULL;
CREATE INDEX projects_name_idx ON projects(name);
ROLLBACK;
""",
        ("ALTER TABLE", "CREATE INDEX", "ROLLBACK"),
    )
    run_fixture(
        "rollback",
        r"""
BEGIN;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
ALTER TABLE projects ADD COLUMN marker TEXT;
\echo ADD_COLUMN_STATE :SQLSTATE
SELECT id FROM projects;
\echo ADD_COLUMN_DML_STATE :SQLSTATE
SELECT id FROM projects;
\echo ADD_COLUMN_FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        (
            "ADD_COLUMN_STATE 00000",
            "ADD_COLUMN_DML_STATE 25000",
            "ADD_COLUMN_FAILED_STATE 25P02",
            "ROLLBACK",
        ),
    )
    run_fixture(
        "rollback",
        r"""
BEGIN;
UPDATE projects SET name = 'filled' WHERE name IS NULL;
ALTER TABLE projects DROP COLUMN name;
\echo DROP_COLUMN_STATE :SQLSTATE
SELECT id FROM projects;
\echo DROP_COLUMN_FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        (
            "DROP_COLUMN_STATE 2BP01",
            "DROP_COLUMN_FAILED_STATE 25P02",
            "ROLLBACK",
        ),
    )

    run_fixture(
        "combined",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_legacy_idx;
DROP INDEX projects_email_idx;
UPDATE projects SET email = 'filled@example.test' WHERE email IS NULL;
INSERT INTO projects VALUES (4, 'old-four', 'four@example.test');
DELETE FROM projects WHERE id = 2;
ALTER TABLE projects DROP COLUMN legacy;
ALTER TABLE projects ADD COLUMN legacy TEXT;
ALTER TABLE projects ALTER COLUMN email SET NOT NULL;
ALTER TABLE projects RENAME COLUMN email TO contact;
CREATE INDEX projects_contact_idx ON projects(contact);
COMMIT;
SELECT id, contact, legacy FROM projects ORDER BY id;
\d projects
""",
        (
            "UPDATE 1",
            "INSERT 0 1",
            "DELETE 1",
            "ALTER TABLE",
            "CREATE INDEX",
            "COMMIT",
            "1|filled@example.test|",
            "3|three@example.test|",
            "4|four@example.test|",
            "contact|text||not null|",
        ),
        42,
    )
    run_fixture(
        "add",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_legacy_idx;
DROP INDEX projects_email_idx;
UPDATE projects SET email = email WHERE id = 1;
ALTER TABLE projects ADD COLUMN marker TEXT;
COMMIT;
SELECT id, marker FROM projects ORDER BY id;
""",
        ("ALTER TABLE", "COMMIT", "1|", "2|", "3|"),
        42,
    )
    run_fixture(
        "drop",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_legacy_idx;
DROP INDEX projects_email_idx;
UPDATE projects SET email = email WHERE id = 1;
ALTER TABLE projects DROP COLUMN legacy;
COMMIT;
SELECT id, email FROM projects ORDER BY id;
""",
        ("ALTER TABLE", "COMMIT", "2|two@example.test"),
        42,
    )
    run_fixture(
        "multiple-add",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_legacy_idx;
DROP INDEX projects_email_idx;
UPDATE projects SET email = email WHERE id = 1;
ALTER TABLE projects ADD COLUMN marker TEXT;
ALTER TABLE projects ADD COLUMN score BIGINT;
COMMIT;
SELECT id, marker, score FROM projects ORDER BY id;
""",
        ("ALTER TABLE", "COMMIT", "1||", "2||", "3||"),
        42,
    )
    run_fixture(
        "noop",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_legacy_idx;
DROP INDEX projects_email_idx;
UPDATE projects SET email = email WHERE id = 1;
ALTER TABLE projects ADD COLUMN temporary BOOLEAN;
ALTER TABLE projects DROP COLUMN temporary;
COMMIT;
""",
        ("ALTER TABLE", "COMMIT"),
        42,
    )
    run_fixture(
        "rollback",
        r"""
\set ON_ERROR_STOP on
BEGIN;
DROP INDEX projects_legacy_idx;
DROP INDEX projects_email_idx;
UPDATE projects SET email = 'changed' WHERE id = 1;
ALTER TABLE projects DROP COLUMN legacy;
ALTER TABLE projects ADD COLUMN legacy TEXT;
ROLLBACK;
""",
        ("ALTER TABLE", "ROLLBACK"),
        42,
    )
    for probe, setup, failing, marker, sqlstate in (
        (
            "post-dml",
            "DROP INDEX projects_legacy_idx;\nDROP INDEX projects_email_idx;\n"
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "INSERT INTO projects VALUES (4, 'four', 'four@example.test', NULL);",
            "POST_DML_STATE",
            "25000",
        ),
        (
            "read-only-add",
            "SELECT email FROM projects WHERE id = 1;",
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "READ_ONLY_ADD_STATE",
            "25000",
        ),
        (
            "pending-index",
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "CREATE INDEX projects_id_idx ON projects(id);",
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "PENDING_INDEX_STATE",
            "0A000",
        ),
        (
            "new-index",
            "DROP INDEX projects_legacy_idx;\nDROP INDEX projects_email_idx;\n"
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "CREATE INDEX projects_marker_idx ON projects(marker);",
            "NEW_INDEX_STATE",
            "0A000",
        ),
        (
            "new-not-null",
            "DROP INDEX projects_legacy_idx;\nDROP INDEX projects_email_idx;\n"
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "ALTER TABLE projects ALTER COLUMN marker SET NOT NULL;",
            "NEW_NOT_NULL_STATE",
            "0A000",
        ),
    ):
        run_fixture(
            probe,
            f"""
BEGIN;
{setup}
{failing}
\echo {marker} :SQLSTATE
SELECT id FROM projects;
\echo FAILED_STATE :SQLSTATE
ROLLBACK;
""",
            (f"{marker} {sqlstate}", "FAILED_STATE 25P02", "ROLLBACK"),
            42,
        )
    print(f"{version}: Round 39 + Round 42 DROP-first migration SQL acceptance PASS")


if __name__ == "__main__":
    main()
