#!/usr/bin/env python3
"""Round 48 real psql terminal adopted-source final-index acceptance."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess


def named_prepared(retained, environment) -> None:
    """psql 17 has no named Parse command; use the same installation's libpq."""
    import ctypes
    import tempfile

    pq = ctypes.CDLL("/opt/local/lib/pgsql/lib/libpq.dylib")
    pointer = ctypes.c_void_p
    signatures = {
        "PQconnectdb": ([ctypes.c_char_p], pointer),
        "PQstatus": ([pointer], ctypes.c_int),
        "PQexec": ([pointer, ctypes.c_char_p], pointer),
        "PQprepare": ([pointer, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int, pointer], pointer),
        "PQexecPrepared": ([pointer, ctypes.c_char_p, ctypes.c_int, pointer, pointer, pointer, ctypes.c_int], pointer),
        "PQresultStatus": ([pointer], ctypes.c_int),
        "PQresultErrorField": ([pointer, ctypes.c_int], ctypes.c_char_p),
        "PQclear": ([pointer], None),
        "PQfinish": ([pointer], None),
    }
    for name, (args, result) in signatures.items():
        function = getattr(pq, name)
        function.argtypes, function.restype = args, result

    def check(result, expected=None):
        try:
            state = pq.PQresultErrorField(result, ord("C"))
            assert state == expected, (state, expected)
            if expected is None:
                assert pq.PQresultStatus(result) in (1, 2)
        finally:
            pq.PQclear(result)

    for is_drop in (False, True):
        with tempfile.TemporaryFile(mode="w+") as trace:
            process = subprocess.Popen([str(retained.FIXTURE)], cwd=retained.ROOT,
                env=dict(environment, NETBADB_ROUND48_PROBE="rollback"),
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=trace, text=True)
            connection = None
            try:
                address = process.stdout.readline().strip()
                assert address
                connection = pq.PQconnectdb(f"postgresql://netbadb@{address}/netbadb".encode())
                assert pq.PQstatus(connection) == 0
                for sql in ("BEGIN", "UPDATE projects SET legacy = 'updated1' WHERE id = 1",
                            "ALTER TABLE projects RENAME COLUMN email TO contact"):
                    check(pq.PQexec(connection, sql.encode()))
                query = b"DROP INDEX projects_email_idx" if is_drop else b"CREATE INDEX IF NOT EXISTS projects_legacy_idx ON projects(legacy)"
                check(pq.PQprepare(connection, b"old", query, 0, None))
                statements = ("DROP INDEX projects_email_idx", "CREATE INDEX projects_email_idx ON projects(contact)") if is_drop else ("ALTER TABLE projects RENAME COLUMN contact TO email",)
                for sql in statements:
                    check(pq.PQexec(connection, sql.encode()))
                check(pq.PQexecPrepared(connection, b"old", 0, None, None, None, 0), b"42704" if is_drop else b"25000")
                check(pq.PQexec(connection, b"SELECT id FROM projects"), b"25P02")
                check(pq.PQexec(connection, b"ROLLBACK"))
            finally:
                if connection:
                    pq.PQfinish(connection)
                output, _ = process.communicate(timeout=60)
                trace.seek(0)
                assert process.returncode == 0, trace.read()
                assert "REOPEN PASS" in output, output
                print(output.strip())


def main() -> None:
    path = Path(__file__).with_name("test-post-dml-source-adoption-sql.py")
    spec = importlib.util.spec_from_file_location("retained", path)
    assert spec is not None and spec.loader is not None
    retained = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(retained)
    environment = dict(os.environ, DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib", CARGO_TARGET_DIR=str(retained.TARGET))
    subprocess.run([retained.PSQL, "--version"], env=environment, check=True)
    subprocess.run(["cargo", "build", "--offline", "-p", "netbadb-server", "--example", "sql_alter_table_fixture"], cwd=retained.ROOT, env=environment, check=True)
    dml = """BEGIN;
UPDATE projects SET legacy = 'updated1' WHERE id = 1;
INSERT INTO projects VALUES (4, 'inserted4', 'four@example.test');
DELETE FROM projects WHERE id = 2;
"""
    noop = """ALTER TABLE projects RENAME COLUMN email TO tmp;
ALTER TABLE projects RENAME COLUMN tmp TO email;
"""
    create = "CREATE INDEX projects_legacy_idx ON projects(legacy);\n"
    cases = [
        ("cnew", "ALTER TABLE projects ADD COLUMN marker TEXT;\nCREATE INDEX projects_marker_idx ON projects(marker);"),
        ("rename", "ALTER TABLE projects RENAME COLUMN email TO contact;\nDROP INDEX projects_email_idx;\nCREATE INDEX projects_contact_idx ON projects(contact);"),
        ("create", noop + create),
        ("drop", noop + "DROP INDEX projects_email_idx;"),
        ("create-drop", noop + create + "DROP INDEX projects_legacy_idx;"),
        ("replacement", noop + "DROP INDEX projects_email_idx;\nCREATE INDEX projects_email_idx ON projects(email);"),
        ("multiple", noop + create + "CREATE INDEX projects_id_idx ON projects(id);\nDROP INDEX projects_email_idx;"),
    ]
    for probe, ddl in cases:
        retained.run_fixture(probe, "\\set ON_ERROR_STOP on\n" + dml + ddl + "\nCOMMIT;\nSELECT id, legacy FROM projects ORDER BY id;\n", ("COMMIT", "1|updated1", "3|old-three", "4|inserted4"), round_number=48)
    retained.run_fixture("rollback", dml + noop + create + "ROLLBACK;\n", ("CREATE INDEX", "ROLLBACK"), round_number=48)
    # psql's unnamed extended-query path (Parse/Bind/Describe/Execute).
    retained.run_fixture("create", dml + noop + "CREATE INDEX projects_legacy_idx ON projects(legacy)\n\\bind\n\\g\nCOMMIT;\n", ("CREATE INDEX", "COMMIT"), round_number=48)
    for failing in [
        "ALTER TABLE projects ADD COLUMN extra TEXT;",
        "SELECT id FROM projects;",
        "INSERT INTO projects VALUES (5, 'five', 'five');",
        "UPDATE projects SET legacy = legacy;",
        "DELETE FROM projects WHERE id = 1;",
    ]:
        retained.run_fixture("rollback", dml + noop + create + failing + "\n\\echo TERMINAL :SQLSTATE\nSELECT id FROM projects;\n\\echo FAILED :SQLSTATE\nROLLBACK;\n", ("TERMINAL 25000", "FAILED 25P02", "ROLLBACK"), round_number=48)
    named_prepared(retained, environment)
    print("Round 48 final-index psql acceptance passed; every fixture verified three reopens")


if __name__ == "__main__":
    main()
