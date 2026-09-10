#!/usr/bin/env python3
"""Round 62 real-psql ALTER COLUMN TYPE ... USING acceptance."""
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
        "autocommit",
        r"""
\set ON_ERROR_STOP on
UPDATE projects SET legacy = '0' WHERE legacy = 'bad';
ALTER TABLE projects ALTER COLUMN legacy TYPE BIGINT USING legacy::BIGINT;
SELECT id, legacy, flag FROM projects ORDER BY id;
""",
        ("UPDATE 1", "ALTER TABLE", "1|42|t", "2|-7|f", "3|0|"),
        round_number=62,
    )
    retained.run_fixture(
        "adopted",
        r"""
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET legacy = '43' WHERE id = 1;
UPDATE projects SET legacy = '0' WHERE legacy = 'bad';
ALTER TABLE projects ALTER COLUMN legacy TYPE BIGINT USING legacy::BIGINT;
COMMIT;
SELECT id, legacy FROM projects ORDER BY id;
""",
        ("BEGIN", "UPDATE 1", "ALTER TABLE", "COMMIT", "1|43", "3|0"),
        round_number=62,
    )
    retained.run_fixture(
        "nullable",
        r"""
\set ON_ERROR_STOP on
ALTER TABLE projects ALTER COLUMN legacy TYPE BIGINT USING legacy::BIGINT;
SELECT id, legacy FROM projects ORDER BY id;
""",
        ("ALTER TABLE", "1|42", "2|", "3|99"),
        round_number=62,
    )
    retained.run_fixture(
        "invalid",
        r"""
BEGIN;
ALTER TABLE projects ALTER COLUMN legacy TYPE BIGINT USING legacy::BIGINT;
\echo INVALID_TEXT_STATE :SQLSTATE
SELECT id FROM projects;
\echo INVALID_TEXT_FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        ("INVALID_TEXT_STATE 22P02", "INVALID_TEXT_FAILED_STATE 25P02", "ROLLBACK"),
        round_number=62,
    )
    retained.run_fixture(
        "range",
        r"""
BEGIN;
UPDATE projects SET legacy = '128' WHERE id = 3;
ALTER TABLE projects ALTER COLUMN legacy TYPE TINYINT USING legacy::TINYINT;
\echo RANGE_STATE :SQLSTATE
SELECT id FROM projects;
\echo RANGE_FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        ("RANGE_STATE 22003", "RANGE_FAILED_STATE 25P02", "ROLLBACK"),
        round_number=62,
    )
    retained.run_fixture(
        "unsupported",
        r"""
ALTER TABLE projects ALTER COLUMN legacy TYPE BIGINT USING flag::BIGINT;
\echo UNSUPPORTED_CAST_STATE :SQLSTATE
""",
        ("UNSUPPORTED_CAST_STATE 42846",),
        round_number=62,
    )
    retained.run_fixture(
        "missing",
        r"""
ALTER TABLE projects ALTER COLUMN legacy TYPE BIGINT;
\echo MISSING_USING_STATE :SQLSTATE
ALTER TABLE projects ALTER COLUMN legacy SET DATA TYPE BIGINT USING legacy::BIGINT;
\echo SET_DATA_TYPE_STATE :SQLSTATE
ALTER TABLE projects ALTER COLUMN legacy TYPE BIGINT USING $1;
\echo PARAMETER_STATE :SQLSTATE
""",
        (
            "MISSING_USING_STATE 0A000",
            "SET_DATA_TYPE_STATE 0A000",
            "PARAMETER_STATE 0A000",
        ),
        round_number=62,
    )
    retained.run_fixture(
        "same",
        r"""
ALTER TABLE projects ALTER COLUMN legacy TYPE TEXT USING legacy;
\echo SAME_PHYSICAL_STATE :SQLSTATE
""",
        ("SAME_PHYSICAL_STATE 0A000",),
        round_number=62,
    )
    retained.run_fixture(
        "stream",
        r"""
BEGIN;
ALTER TABLE projects ALTER COLUMN legacy TYPE BIGINT USING legacy::BIGINT;
\echo STREAM_STATE :SQLSTATE
SELECT id FROM projects;
\echo STREAM_FAILED_STATE :SQLSTATE
ROLLBACK;
""",
        ("STREAM_STATE 0A000", "STREAM_FAILED_STATE 25P02", "ROLLBACK"),
        round_number=62,
    )
    retained.run_fixture(
        "rollback",
        r"""
\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET legacy = '0' WHERE legacy = 'bad';
ALTER TABLE projects ALTER COLUMN legacy TYPE BIGINT USING legacy::BIGINT;
ROLLBACK;
SELECT id, legacy FROM projects ORDER BY id;
""",
        ("ALTER TABLE", "ROLLBACK", "1|42", "2|-7", "3|bad"),
        round_number=62,
    )
    print(f"{version}: Round 62 ALTER TYPE USING acceptance passed")


if __name__ == "__main__":
    main()
