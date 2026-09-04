#!/usr/bin/env python3
"""Round 44 real-psql post-DML source-adoption acceptance."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "/private/tmp/netbadb-round44-target"))
PSQL = os.environ.get("PSQL", "/opt/local/lib/pgsql/bin/psql")
FIXTURE = TARGET / "debug/examples/sql_alter_table_fixture"


def run_fixture(
    probe: str, sql: str, expected: tuple[str, ...], *, round_number: int = 44
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
        "add",
        """
\\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = email WHERE id = 1;
ALTER TABLE projects ADD COLUMN marker TEXT;
COMMIT;
SELECT id, marker FROM projects ORDER BY id;
""",
        ("UPDATE 1", "ALTER TABLE", "COMMIT", "1|", "2|", "3|"),
    )
    run_fixture(
        "drop",
        """
\\set ON_ERROR_STOP on
BEGIN;
DELETE FROM projects WHERE id = 2;
ALTER TABLE projects DROP COLUMN legacy;
COMMIT;
SELECT id, email FROM projects ORDER BY id;
""",
        ("DELETE 1", "ALTER TABLE", "COMMIT", "3|three@example.test"),
    )
    run_fixture(
        "rename-column",
        """
\\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = email WHERE id = 1;
ALTER TABLE projects RENAME COLUMN email TO contact;
COMMIT;
SELECT id, contact FROM projects ORDER BY id;
""",
        ("ALTER TABLE", "COMMIT", "2|two@example.test"),
    )
    run_fixture(
        "rename-table",
        """
\\set ON_ERROR_STOP on
BEGIN;
INSERT INTO projects VALUES (4, 'old-four', 'four@example.test');
ALTER TABLE projects RENAME TO accounts;
COMMIT;
SELECT id, email FROM accounts ORDER BY id;
""",
        ("INSERT 0 1", "ALTER TABLE", "COMMIT", "4|four@example.test"),
    )
    run_fixture(
        "multiple-add",
        """
\\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = email WHERE id = -1;
ALTER TABLE projects ADD COLUMN marker TEXT;
ALTER TABLE projects ADD COLUMN score BIGINT;
COMMIT;
SELECT id, marker, score FROM projects ORDER BY id;
""",
        ("UPDATE 0", "ALTER TABLE", "COMMIT", "1||", "2||", "3||"),
    )
    run_fixture(
        "same-name",
        """
\\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = email WHERE id = 1;
ALTER TABLE projects DROP COLUMN legacy;
ALTER TABLE projects ADD COLUMN legacy TEXT;
COMMIT;
SELECT id, legacy FROM projects ORDER BY id;
""",
        ("ALTER TABLE", "COMMIT", "1|", "2|", "3|"),
    )
    run_fixture(
        "noop",
        """
\\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET legacy = 'winner' WHERE id = 1;
ALTER TABLE projects ADD COLUMN temporary TEXT;
ALTER TABLE projects DROP COLUMN temporary;
COMMIT;
SELECT legacy FROM projects WHERE id = 1;
""",
        ("ALTER TABLE", "COMMIT", "winner"),
    )
    run_fixture(
        "mixed",
        """
\\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET email = 'filled@example.test' WHERE email IS NULL;
INSERT INTO projects VALUES (4, 'old-four', 'four@example.test');
DELETE FROM projects WHERE id = 2;
ALTER TABLE projects ADD COLUMN marker TEXT;
ALTER TABLE projects DROP COLUMN legacy;
ALTER TABLE projects RENAME COLUMN email TO contact;
COMMIT;
SELECT id, contact, marker FROM projects ORDER BY id;
""",
        (
            "UPDATE 1",
            "INSERT 0 1",
            "DELETE 1",
            "COMMIT",
            "1|filled@example.test|",
            "3|three@example.test|",
            "4|four@example.test|",
        ),
    )
    run_fixture(
        "rollback",
        """
\\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET legacy = 'loser' WHERE id = 1;
ALTER TABLE projects ADD COLUMN marker TEXT;
ROLLBACK;
""",
        ("UPDATE 1", "ALTER TABLE", "ROLLBACK"),
    )
    for probe, setup, failing, marker, sqlstate in (
        (
            "set-not-null",
            "UPDATE projects SET email = email WHERE id = 1;",
            "ALTER TABLE projects ALTER COLUMN email SET NOT NULL;",
            "SET_NOT_NULL_STATE",
            "25000",
        ),
        (
            "dml-after",
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "UPDATE projects SET email = email WHERE id = 1;",
            "DML_AFTER_STATE",
            "25000",
        ),
        (
            "indexed-drop",
            "UPDATE projects SET email = email WHERE id = 1;",
            "ALTER TABLE projects DROP COLUMN email;",
            "INDEXED_DROP_STATE",
            "2BP01",
        ),
        (
            "index-after",
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "CREATE INDEX projects_marker_idx ON projects(marker);",
            "INDEX_AFTER_STATE",
            "25000",
        ),
    ):
        run_fixture(
            probe,
            f"""
BEGIN;
{setup}
{failing}
\\echo {marker} :SQLSTATE
SELECT id FROM projects;
\\echo FAILED_STATE :SQLSTATE
ROLLBACK;
""",
            (f"{marker} {sqlstate}", "FAILED_STATE 25P02", "ROLLBACK"),
        )

    for probe, setup, failing, marker in (
        (
            "set-not-null",
            "UPDATE projects SET email = email WHERE id = 1;",
            "ALTER TABLE projects ALTER COLUMN email SET NOT NULL;",
            "ROUND45_SET_NOT_NULL_STATE",
        ),
        (
            "drop-not-null",
            "UPDATE projects SET email = email WHERE id = 1;",
            "ALTER TABLE projects ALTER COLUMN email DROP NOT NULL;",
            "ROUND45_DROP_NOT_NULL_STATE",
        ),
        (
            "create-index",
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "CREATE INDEX projects_marker_idx ON projects(marker);",
            "ROUND45_CREATE_INDEX_STATE",
        ),
        (
            "select-after",
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "SELECT id, marker FROM projects;",
            "ROUND45_SELECT_STATE",
        ),
        (
            "insert-after",
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "INSERT INTO projects VALUES (4, 'old-four', 'four@example.test', NULL);",
            "ROUND45_INSERT_STATE",
        ),
        (
            "update-after",
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "UPDATE projects SET email = email WHERE id = 1;",
            "ROUND45_UPDATE_STATE",
        ),
        (
            "delete-after",
            "UPDATE projects SET email = email WHERE id = 1;\n"
            "ALTER TABLE projects ADD COLUMN marker TEXT;",
            "DELETE FROM projects WHERE id = 1;",
            "ROUND45_DELETE_STATE",
        ),
    ):
        run_fixture(
            probe,
            f"""
BEGIN;
{setup}
{failing}
\\echo {marker} :SQLSTATE
SELECT id FROM projects;
\\echo ROUND45_FAILED_STATE :SQLSTATE
ROLLBACK;
""",
            (f"{marker} 25000", "ROUND45_FAILED_STATE 25P02", "ROLLBACK"),
            round_number=45,
        )
    print(
        f"{version}: Round 44 post-DML source adoption and Round 45 negative SQL acceptance PASS"
    )


if __name__ == "__main__":
    main()
