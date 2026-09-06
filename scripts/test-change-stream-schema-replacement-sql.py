#!/usr/bin/env python3
"""Round 52 real-psql Change Stream replacement admission acceptance."""
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

    retained.run_fixture(
        "blocked",
        """
BEGIN;
ALTER TABLE projects ADD COLUMN marker TEXT;
COMMIT;
\echo COMMIT_STATE :SQLSTATE
SELECT id FROM projects;
\echo AFTER_COMMIT_STATE :SQLSTATE
ROLLBACK;
""",
        (
            "ALTER TABLE",
            "COMMIT_STATE 0A000",
            "AFTER_COMMIT_STATE 25000",
            "ROLLBACK",
        ),
        round_number=52,
    )
    retained.run_fixture(
        "disabled",
        """
\set ON_ERROR_STOP on
BEGIN;
ALTER TABLE projects ADD COLUMN marker TEXT;
COMMIT;
SELECT id, marker FROM projects ORDER BY id;
""",
        ("ALTER TABLE", "COMMIT", "1|", "2|", "3|"),
        round_number=52,
    )
    print(
        "Round 52 Change Stream schema-replacement psql acceptance passed: "
        "blocked COMMIT mapped to 0A000 and disabled-stream migration committed"
    )


if __name__ == "__main__":
    main()
