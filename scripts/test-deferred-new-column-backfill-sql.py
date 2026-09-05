#!/usr/bin/env python3
"""Round 50 real-psql deferred new-column backfill acceptance."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess


def main() -> None:
    retained_path = Path(__file__).with_name("test-post-dml-source-adoption-sql.py")
    spec = importlib.util.spec_from_file_location("retained", retained_path)
    assert spec is not None and spec.loader is not None
    retained = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(retained)
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

    dml = """BEGIN;
UPDATE projects SET legacy = 'updated1' WHERE id = 1;
INSERT INTO projects VALUES (4, 'inserted4', 'four@example.test');
DELETE FROM projects WHERE id = 2;
ALTER TABLE projects ADD COLUMN marker TEXT;
"""
    positive = dml + """UPDATE projects SET marker = legacy WHERE legacy IS NOT NULL;
ALTER TABLE projects ALTER COLUMN marker SET NOT NULL;
CREATE INDEX projects_marker_idx ON projects(marker);
COMMIT;
SELECT id, marker FROM projects ORDER BY id;
"""
    retained.run_fixture(
        "commit",
        "\\set ON_ERROR_STOP on\n" + positive,
        (
            "UPDATE 3",
            "ALTER TABLE",
            "CREATE INDEX",
            "COMMIT",
            "1|updated1",
            "3|old-three",
            "4|inserted4",
        ),
        round_number=50,
    )

    extended = dml + """UPDATE projects SET marker = $1 WHERE id = $2
\\bind 'bound-value' '1'
\\g
UPDATE projects SET marker = legacy WHERE id != 1;
ALTER TABLE projects ALTER COLUMN marker SET NOT NULL;
CREATE INDEX projects_marker_idx ON projects(marker);
COMMIT;
"""
    retained.run_fixture(
        "commit-bound",
        extended,
        ("UPDATE 1", "UPDATE 2", "ALTER TABLE", "CREATE INDEX", "COMMIT"),
        round_number=50,
    )

    partial = dml + """UPDATE projects SET marker = legacy WHERE id = 1;
ALTER TABLE projects ALTER COLUMN marker SET NOT NULL;
\\echo PARTIAL_SET :SQLSTATE
SELECT id FROM projects;
\\echo FAILED_STATE :SQLSTATE
ROLLBACK;
"""
    retained.run_fixture(
        "rollback",
        partial,
        ("PARTIAL_SET 23502", "FAILED_STATE 25P02", "ROLLBACK"),
        round_number=50,
    )

    terminal = dml + """UPDATE projects SET marker = legacy;
CREATE INDEX projects_marker_idx ON projects(marker);
UPDATE projects SET marker = 'too-late';
\\echo TERMINAL_UPDATE :SQLSTATE
SELECT id FROM projects;
\\echo FAILED_STATE :SQLSTATE
ROLLBACK;
"""
    retained.run_fixture(
        "rollback",
        terminal,
        ("TERMINAL_UPDATE 25000", "FAILED_STATE 25P02", "ROLLBACK"),
        round_number=50,
    )
    print("Round 50 deferred new-column backfill psql acceptance passed; every fixture verified three reopens")


if __name__ == "__main__":
    main()
