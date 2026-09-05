#!/usr/bin/env python3
"""Round 47 real PostgreSQL negatives; reuse the retained Round 45 base verifier."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess


def main() -> None:
    path = Path(__file__).with_name("test-post-dml-source-adoption-sql.py")
    spec = importlib.util.spec_from_file_location("round44", path)
    assert spec is not None and spec.loader is not None
    retained = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(retained)
    environment = dict(os.environ, DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib", CARGO_TARGET_DIR=str(retained.TARGET))
    subprocess.run(["cargo", "build", "--offline", "-p", "netbadb-server", "--example", "sql_alter_table_fixture"], cwd=retained.ROOT, env=environment, check=True)
    for name, alter, ddl in [
        ("CREATE", "ALTER TABLE projects ADD COLUMN marker TEXT;", "CREATE INDEX projects_marker_idx ON projects(marker);"),
        ("DROP", "ALTER TABLE projects RENAME COLUMN email TO contact;", "DROP INDEX projects_email_idx;"),
    ]:
        retained.run_fixture("create-index", f"""
BEGIN;
UPDATE projects SET legacy = 'round47' WHERE id = 1;
{alter}
{ddl}
\\echo ROUND47_{name} :SQLSTATE
SELECT id FROM projects;
\\echo FAILED_STATE :SQLSTATE
ROLLBACK;
SELECT id, legacy FROM projects ORDER BY id;
""", (f"ROUND47_{name} 25000", "FAILED_STATE 25P02", "1|old-one", "2|old-two", "3|old-three", "ROLLBACK"), round_number=45)
    print("Round 47 CREATE/DROP negatives passed; each base reopened three times")


if __name__ == "__main__":
    main()
