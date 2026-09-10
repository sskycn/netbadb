#!/usr/bin/env python3
"""Round 61 real-psql negative acceptance for ALTER TYPE ... USING."""
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
        "cast-migration",
        r"""
BEGIN;
UPDATE projects SET legacy = '43' WHERE id = 1;
INSERT INTO projects VALUES (4, '99', true);
DELETE FROM projects WHERE id = 2;
ALTER TABLE projects ADD COLUMN shadow BIGINT;
UPDATE projects SET shadow = 0 WHERE legacy = 'bad' AND shadow IS NULL;
UPDATE projects
SET shadow = legacy::BIGINT
WHERE shadow IS NULL AND legacy IS NOT NULL;
ALTER TABLE projects ALTER COLUMN shadow SET NOT NULL;
DROP INDEX projects_legacy_idx;
ALTER TABLE projects DROP COLUMN legacy;
ALTER TABLE projects RENAME COLUMN shadow TO legacy;
CREATE INDEX projects_legacy_idx ON projects(legacy);
COMMIT;
BEGIN;
ALTER TABLE projects
ALTER COLUMN legacy TYPE BIGINT
USING legacy::BIGINT;
\echo ALTER_TYPE_USING_STATE :SQLSTATE
SELECT id FROM projects;
\echo ALTER_TYPE_USING_FAILED_STATE :SQLSTATE
ROLLBACK;
SELECT id, legacy, flag FROM projects ORDER BY id;
""",
        (
            "INSERT 0 1",
            "DELETE 1",
            "UPDATE 1",
            "ALTER TABLE",
            "UPDATE 2",
            "COMMIT",
            "ALTER_TYPE_USING_STATE 0A000",
            "ALTER_TYPE_USING_FAILED_STATE 25P02",
            "ROLLBACK",
            "1|43|t",
            "3|0|",
            "4|99|t",
        ),
        round_number=60,
    )
    print(f"{version}: Round 61 ALTER TYPE USING production negative passed")


if __name__ == "__main__":
    main()
