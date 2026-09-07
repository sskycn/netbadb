#!/usr/bin/env python3
"""Round 58 real-psql indexed shadow-column replacement acceptance."""
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
        "indexed-swap",
        r"""
BEGIN;
UPDATE projects SET legacy = 'updated1' WHERE id = 1;
INSERT INTO projects VALUES (4, 'inserted4', true);
DELETE FROM projects WHERE id = 2;
ALTER TABLE projects ADD COLUMN shadow TEXT;
UPDATE projects SET shadow = legacy WHERE legacy IS NOT NULL;
UPDATE projects SET shadow = 'missing' WHERE shadow IS NULL;
ALTER TABLE projects ALTER COLUMN shadow SET NOT NULL;
DROP INDEX projects_legacy_idx;
ALTER TABLE projects DROP COLUMN legacy;
ALTER TABLE projects RENAME COLUMN shadow TO legacy;
CREATE INDEX projects_legacy_idx ON projects(legacy);
COMMIT;
SELECT id, legacy FROM projects ORDER BY id;
""",
        (
            "BEGIN",
            "UPDATE 1",
            "INSERT 0 1",
            "DELETE 1",
            "ALTER TABLE",
            "UPDATE 2",
            "UPDATE 1",
            "DROP INDEX",
            "CREATE INDEX",
            "COMMIT",
            "1|updated1",
            "3|missing",
            "4|inserted4",
        ),
        round_number=58,
    )
    print("Round 58 indexed shadow-swap production acceptance passed")


if __name__ == "__main__":
    main()
