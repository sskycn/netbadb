#!/usr/bin/env python3
"""Round 57 real-psql audit of the retained production index-swap blocker."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess


def load_retained():
    path = Path(__file__).with_name("test-post-dml-source-adoption-sql.py")
    spec = importlib.util.spec_from_file_location("retained", path)
    assert spec is not None and spec.loader is not None
    retained = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(retained)
    return retained


def main() -> None:
    retained = load_retained()
    environment = dict(
        os.environ,
        DYLD_LIBRARY_PATH="/opt/local/lib/icu/lib",
        CARGO_TARGET_DIR=str(retained.TARGET),
    )
    version = subprocess.check_output(
        [retained.PSQL, "--version"], text=True, env=environment
    ).strip()
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
        cwd=retained.ROOT,
        env=environment,
        check=True,
    )

    retained.run_fixture(
        "indexed-drop",
        r"""
BEGIN;
UPDATE projects SET legacy = 'updated1' WHERE id = 1;
ALTER TABLE projects ADD COLUMN shadow TEXT;
UPDATE projects SET shadow = legacy WHERE legacy IS NOT NULL;
UPDATE projects SET shadow = 'missing' WHERE shadow IS NULL;
ALTER TABLE projects ALTER COLUMN shadow SET NOT NULL;
DROP INDEX projects_legacy_idx;
\echo ROUND57_DROP_INDEX :SQLSTATE
ALTER TABLE projects DROP COLUMN legacy;
\echo ROUND57_DROP_COLUMN :SQLSTATE
ALTER TABLE projects RENAME COLUMN shadow TO legacy;
\echo ROUND57_ABORTED :SQLSTATE
ROLLBACK;
""",
        (
            "DROP INDEX",
            "ROUND57_DROP_INDEX 00000",
            "ROUND57_DROP_COLUMN 25000",
            "ROUND57_ABORTED 25P02",
            "ROLLBACK",
        ),
        # The retained fixture's indexed source is deliberately the unchanged
        # Round 56 production route. Round 57 adds no production behavior.
        round_number=56,
    )
    print("Round 57 indexed shadow-swap production blocker audit passed")


if __name__ == "__main__":
    main()
