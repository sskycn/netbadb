#!/usr/bin/env python3
"""Round 59 real-psql negative cross-physical CAST acceptance."""
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
        "cast-negative",
        r"""
SELECT 42::BIGINT, 'one'::TEXT, true::BOOL;
BEGIN;
SELECT name::BIGINT FROM projects;
\echo CROSS_CAST_STATE :SQLSTATE
SELECT id FROM projects;
\echo CROSS_CAST_FAILED_STATE :SQLSTATE
ROLLBACK;
BEGIN;
ALTER TABLE projects ALTER COLUMN name TYPE BIGINT;
\echo ALTER_TYPE_STATE :SQLSTATE
SELECT id FROM projects;
\echo ALTER_TYPE_FAILED_STATE :SQLSTATE
ROLLBACK;
SELECT id, name FROM projects;
""",
        (
            "42|one|t",
            "CROSS_CAST_STATE 42804",
            "CROSS_CAST_FAILED_STATE 25P02",
            "ALTER_TYPE_STATE 0A000",
            "ALTER_TYPE_FAILED_STATE 25P02",
            "ROLLBACK",
            "1|one",
        ),
        round_number=59,
    )
    print(f"{version}: Round 59 production negative SQL acceptance passed")


if __name__ == "__main__":
    main()
