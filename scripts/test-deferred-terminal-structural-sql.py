#!/usr/bin/env python3
"""Round 56 real-psql terminal shadow-column replacement acceptance."""
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

    prefix = """\set ON_ERROR_STOP on
BEGIN;
UPDATE projects SET legacy = 'updated1' WHERE id = 1;
INSERT INTO projects VALUES (4, 'inserted4', true);
DELETE FROM projects WHERE id = 2;
ALTER TABLE projects ADD COLUMN shadow TEXT;
UPDATE projects SET shadow = legacy WHERE legacy IS NOT NULL;
UPDATE projects SET shadow = 'missing' WHERE shadow IS NULL;
ALTER TABLE projects ALTER COLUMN shadow SET NOT NULL;
ALTER TABLE projects DROP COLUMN legacy;
ALTER TABLE projects RENAME COLUMN shadow TO legacy;
"""
    retained.run_fixture(
        "commit",
        prefix
        + """CREATE INDEX projects_legacy_idx ON projects(legacy);
COMMIT;
SELECT id, legacy FROM projects ORDER BY id;
""",
        (
            "UPDATE 1",
            "INSERT 0 1",
            "DELETE 1",
            "ALTER TABLE",
            "UPDATE 2",
            "CREATE INDEX",
            "COMMIT",
            "1|updated1",
            "3|missing",
            "4|inserted4",
        ),
        round_number=56,
    )

    retained.run_fixture(
        "rename-table",
        prefix
        + """ALTER TABLE projects RENAME TO people;
CREATE INDEX projects_legacy_idx ON people(legacy);
COMMIT;
SELECT id, legacy FROM people ORDER BY id;
""",
        (
            "ALTER TABLE",
            "CREATE INDEX",
            "COMMIT",
            "1|updated1",
            "3|missing",
            "4|inserted4",
        ),
        round_number=56,
    )

    retained.run_fixture(
        "rollback",
        prefix
        + """ROLLBACK;
SELECT id, legacy FROM projects ORDER BY id;
""",
        ("ROLLBACK", "1|old1", "2|old2"),
        round_number=56,
    )

    retained.run_fixture(
        "indexed-drop",
        """BEGIN;
UPDATE projects SET legacy = legacy WHERE id = 1;
ALTER TABLE projects ADD COLUMN shadow TEXT;
UPDATE projects SET shadow = legacy WHERE legacy IS NOT NULL;
ALTER TABLE projects DROP COLUMN legacy;
\echo INDEXED_DROP :SQLSTATE
ROLLBACK;
""",
        ("UPDATE 1", "UPDATE 2", "INDEXED_DROP 2BP01", "ROLLBACK"),
        round_number=56,
    )
    print("Round 56 terminal shadow-column psql acceptance passed; every fixture verified three reopens")


if __name__ == "__main__":
    main()
