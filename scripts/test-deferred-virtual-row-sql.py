#!/usr/bin/env python3
"""Round 54 real PostgreSQL VirtualRow late-column acceptance."""
from __future__ import annotations

import ctypes
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile


def load_retained():
    path = Path(__file__).with_name("test-post-dml-source-adoption-sql.py")
    spec = importlib.util.spec_from_file_location("retained", path)
    assert spec is not None and spec.loader is not None
    retained = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(retained)
    return retained


def named_prepared_consumer(retained, environment) -> None:
    """psql 17 has no named Parse command, so use its matching libpq."""
    pq = ctypes.CDLL("/opt/local/lib/pgsql/lib/libpq.dylib")
    pointer = ctypes.c_void_p
    signatures = {
        "PQconnectdb": ([ctypes.c_char_p], pointer),
        "PQstatus": ([pointer], ctypes.c_int),
        "PQexec": ([pointer, ctypes.c_char_p], pointer),
        "PQprepare": (
            [pointer, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int, pointer],
            pointer,
        ),
        "PQexecPrepared": (
            [pointer, ctypes.c_char_p, ctypes.c_int, pointer, pointer, pointer, ctypes.c_int],
            pointer,
        ),
        "PQresultStatus": ([pointer], ctypes.c_int),
        "PQresultErrorField": ([pointer, ctypes.c_int], ctypes.c_char_p),
        "PQcmdStatus": ([pointer], ctypes.c_char_p),
        "PQclear": ([pointer], None),
        "PQfinish": ([pointer], None),
    }
    for name, (arguments, result) in signatures.items():
        function = getattr(pq, name)
        function.argtypes = arguments
        function.restype = result

    def check(result, command=None):
        try:
            state = pq.PQresultErrorField(result, ord("C"))
            assert state is None, state
            assert pq.PQresultStatus(result) in (1, 2)
            if command is not None:
                assert pq.PQcmdStatus(result).decode() == command
        finally:
            pq.PQclear(result)

    with tempfile.TemporaryFile(mode="w+") as trace:
        process = subprocess.Popen(
            [str(retained.FIXTURE)],
            cwd=retained.ROOT,
            env=dict(environment, NETBADB_ROUND54_PROBE="commit"),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=trace,
            text=True,
        )
        connection = None
        try:
            assert process.stdout is not None
            address = process.stdout.readline().strip()
            assert address
            connection = pq.PQconnectdb(
                f"postgresql://netbadb@{address}/netbadb".encode()
            )
            assert pq.PQstatus(connection) == 0
            for sql in (
                "BEGIN",
                "UPDATE projects SET legacy = 'updated1' WHERE id = 1",
                "INSERT INTO projects VALUES (4, 'inserted4', 'four@example.test')",
                "DELETE FROM projects WHERE id = 2",
                "UPDATE projects SET legacy = NULL WHERE id = 3",
                "ALTER TABLE projects ADD COLUMN marker TEXT",
                "ALTER TABLE projects ADD COLUMN normalized TEXT",
            ):
                check(pq.PQexec(connection, sql.encode()))
            check(
                pq.PQprepare(
                    connection,
                    b"consumer",
                    b"UPDATE projects SET normalized = marker WHERE marker IS NOT NULL",
                    0,
                    None,
                )
            )
            check(
                pq.PQexec(
                    connection,
                    b"UPDATE projects SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL",
                ),
                "UPDATE 2",
            )
            check(
                pq.PQexec(
                    connection,
                    b"UPDATE projects SET marker = 'missing' WHERE marker IS NULL",
                ),
                "UPDATE 1",
            )
            check(
                pq.PQexecPrepared(connection, b"consumer", 0, None, None, None, 0),
                "UPDATE 3",
            )
            for sql in (
                "ALTER TABLE projects ALTER COLUMN marker SET NOT NULL",
                "ALTER TABLE projects ALTER COLUMN normalized SET NOT NULL",
                "CREATE INDEX projects_normalized_idx ON projects(normalized)",
                "COMMIT",
            ):
                check(pq.PQexec(connection, sql.encode()))
        finally:
            if connection:
                pq.PQfinish(connection)
            process.communicate(timeout=60)
            trace.seek(0)
            assert process.returncode == 0, trace.read()


def main() -> None:
    retained = load_retained()
    environment = dict(
        os.environ,
        DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib",
        CARGO_TARGET_DIR=str(retained.TARGET),
    )
    subprocess.run([retained.PSQL, "--version"], env=environment, check=True)
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
        cwd=retained.ROOT,
        env=environment,
        check=True,
    )

    adoption = """BEGIN;
UPDATE projects SET legacy = 'updated1' WHERE id = 1;
INSERT INTO projects VALUES (4, 'inserted4', 'four@example.test');
DELETE FROM projects WHERE id = 2;
UPDATE projects SET legacy = NULL WHERE id = 3;
ALTER TABLE projects ADD COLUMN marker TEXT;
ALTER TABLE projects ADD COLUMN normalized TEXT;
"""
    retained.run_fixture(
        "commit",
        "\\set ON_ERROR_STOP on\n"
        + adoption
        + """UPDATE projects SET marker = legacy
WHERE marker IS NULL AND legacy IS NOT NULL;
UPDATE projects SET marker = 'missing' WHERE marker IS NULL;
UPDATE projects SET normalized = marker WHERE marker IS NOT NULL;
ALTER TABLE projects ALTER COLUMN marker SET NOT NULL;
ALTER TABLE projects ALTER COLUMN normalized SET NOT NULL;
CREATE INDEX projects_normalized_idx ON projects(normalized);
COMMIT;
SELECT id, marker, normalized FROM projects ORDER BY id;
""",
        (
            "UPDATE 2",
            "UPDATE 1",
            "UPDATE 3",
            "ALTER TABLE",
            "CREATE INDEX",
            "COMMIT",
            "1|updated1|updated1",
            "3|missing|missing",
            "4|inserted4|inserted4",
        ),
        round_number=54,
    )

    retained.run_fixture(
        "rollback",
        adoption
        + """UPDATE projects SET marker = legacy WHERE id = 1;
ALTER TABLE projects ALTER COLUMN marker SET NOT NULL;
\\echo PARTIAL_SET :SQLSTATE
SELECT id FROM projects;
\\echo FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        ("PARTIAL_SET 23502", "FAILED_STATE 25P02", "ROLLBACK"),
        round_number=54,
    )

    retained.run_fixture(
        "enabled",
        """BEGIN;
UPDATE projects SET legacy = legacy WHERE id = 999;
ALTER TABLE projects ADD COLUMN marker TEXT;
ALTER TABLE projects ADD COLUMN normalized TEXT;
UPDATE projects SET marker = legacy WHERE marker IS NULL;
UPDATE projects SET normalized = marker WHERE marker IS NOT NULL;
ALTER TABLE projects ALTER COLUMN marker SET NOT NULL;
ALTER TABLE projects ALTER COLUMN normalized SET NOT NULL;
CREATE INDEX projects_normalized_idx ON projects(normalized);
COMMIT;
\\echo BLOCKED :SQLSTATE
SELECT id FROM projects;
\\echo FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        ("UPDATE 3", "BLOCKED 0A000", "FAILED_STATE 25000", "ROLLBACK"),
        round_number=54,
    )

    named_prepared_consumer(retained, environment)
    print(
        "Round 54 VirtualRow psql/libpq acceptance passed; every fixture verified three reopens"
    )


if __name__ == "__main__":
    main()
