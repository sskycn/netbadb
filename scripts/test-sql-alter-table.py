#!/usr/bin/env python3
"""Round 29 real-client schema/index composition acceptance."""
from __future__ import annotations

import io
import os
from pathlib import Path
import subprocess
import tempfile

import alembic
from alembic.migration import MigrationContext
from alembic.operations import Operations
import psycopg
import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.engine.reflection import ObjectKind, ObjectScope

ROOT = Path(__file__).resolve().parents[1]
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "/private/tmp/netbadb-round26-target"))
PSQL = os.environ.get("PSQL", "/opt/local/lib/pgsql/bin/psql")


def psql_probe(dsn: str) -> None:
    version = subprocess.check_output([PSQL, "--version"], text=True).strip()
    assert "17.11" in version, version
    composed = subprocess.run(
        [PSQL, "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", dsn],
        input="""\\set VERBOSITY verbose
BEGIN;
ALTER TABLE projects ADD COLUMN active BOOLEAN;
CREATE INDEX projects_active_idx ON projects(active);
ALTER TABLE projects RENAME COLUMN name TO title;
ALTER TABLE projects ALTER COLUMN title SET NOT NULL;
ALTER TABLE projects ALTER COLUMN title DROP NOT NULL;
COMMIT;
SELECT id, title, active FROM projects ORDER BY id;
BEGIN;
DROP INDEX projects_active_idx;
ALTER TABLE projects DROP COLUMN active;
COMMIT;
SELECT id, title FROM projects ORDER BY id;
""",
        text=True,
        capture_output=True,
    )
    assert composed.returncode == 0, composed.stderr
    assert composed.stdout.strip().splitlines() == ["1|one|", "1|one"], composed.stdout
    print(
        f"{version}: ALTER + CREATE INDEX and DROP INDEX + DROP COLUMN transactions PASS"
    )


def psycopg_probe(dsn: str) -> None:
    assert psycopg.__version__ == "3.2.13", psycopg.__version__
    with psycopg.connect(dsn) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(
                    "ALTER TABLE projects ADD COLUMN active BOOLEAN", prepare=True
                )
                cursor.execute(
                    "CREATE INDEX projects_active_idx ON projects(active)",
                    prepare=True,
                )
                cursor.execute("ALTER TABLE projects RENAME COLUMN name TO title")
                cursor.execute("ALTER TABLE projects ALTER COLUMN title SET NOT NULL")
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("DROP INDEX projects_active_idx", prepare=True)
                cursor.execute("ALTER TABLE projects DROP COLUMN active")
        with connection.cursor() as cursor:
            cursor.execute("SELECT id, title FROM projects ORDER BY id", prepare=True)
            assert cursor.fetchall() == [(1, "one")]
        connection.commit()
    print(
        f"psycopg {psycopg.__version__}: prepare=True mixed create/drop index transactions PASS"
    )


def sqlalchemy_probe(dsn: str) -> None:
    assert sa.__version__ == "2.0.52", sa.__version__
    engine = sa.create_engine(dsn.replace("postgresql://", "postgresql+psycopg://", 1))

    def table_oid(name: str) -> int:
        with engine.connect() as connection:
            rows = engine.dialect._get_table_oids(
                connection, None, [name], ObjectScope.ANY, ObjectKind.TABLE
            )
            assert len(rows) == 1, rows
            return rows[0][0]

    before_oid = table_oid("projects")
    metadata = sa.MetaData()
    projects = sa.Table(
        "projects",
        metadata,
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.Text()),
        sa.Column("active", sa.Boolean()),
    )
    active_index = sa.Index("projects_active_idx", projects.c.active)
    with engine.begin() as connection:
        connection.exec_driver_sql("ALTER TABLE projects ADD COLUMN active BOOLEAN")
        active_index.create(connection)
        connection.exec_driver_sql("ALTER TABLE projects RENAME COLUMN name TO title")
        connection.exec_driver_sql("ALTER TABLE projects ALTER COLUMN title SET NOT NULL")
    add_oid = table_oid("projects")
    inspector = sa.inspect(engine)
    assert [(c["name"], c["nullable"]) for c in inspector.get_columns("projects")] == [
        ("id", False),
        ("title", False),
        ("active", True),
    ]
    with engine.begin() as connection:
        connection.exec_driver_sql("ALTER TABLE projects RENAME TO work")
    rename_oid = table_oid("work")
    assert "work" in sa.inspect(engine).get_table_names()
    assert [(c["name"], c["nullable"]) for c in sa.inspect(engine).get_columns("work")] == [
        ("id", False),
        ("title", False),
        ("active", True),
    ]
    assert sorted(
        (index["name"], tuple(index["column_names"]))
        for index in sa.inspect(engine).get_indexes("work")
    ) == [
        ("projects_active_idx", ("active",)),
        ("projects_name_idx", ("title",)),
    ]
    with engine.begin() as connection:
        active_index.drop(connection)
        connection.exec_driver_sql("ALTER TABLE work DROP COLUMN active")
    assert [column["name"] for column in sa.inspect(engine).get_columns("work")] == [
        "id",
        "title",
    ]
    assert before_oid != add_oid and add_oid != rename_oid
    engine.dispose()
    print(
        f"SQLAlchemy {sa.__version__}: Index.create/drop in mixed DDL transactions PASS; "
        f"fingerprint-keyed table OIDs {before_oid} -> {add_oid} -> {rename_oid}"
    )


def alembic_probe(dsn: str) -> None:
    assert alembic.__version__ == "1.16.5", alembic.__version__
    engine = sa.create_engine(dsn.replace("postgresql://", "postgresql+psycopg://", 1))
    generated: list[str] = []
    boundaries: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def capture(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith(
            ("ALTER TABLE", "CREATE INDEX", "DROP INDEX")
        ):
            generated.append(" ".join(statement.split()))

    @event.listens_for(engine, "begin")
    def capture_begin(_connection):
        boundaries.append("BEGIN")

    @event.listens_for(engine, "commit")
    def capture_commit(_connection):
        boundaries.append("COMMIT")

    @event.listens_for(engine, "rollback")
    def capture_rollback(_connection):
        boundaries.append("ROLLBACK")

    offline = io.StringIO()
    offline_context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": offline},
    )
    offline_ops = Operations(offline_context)
    offline_ops.add_column(
        "projects", sa.Column("blocked", sa.Boolean(), nullable=True)
    )
    offline_ops.alter_column("projects", "name", new_column_name="display_name")
    offline_ops.alter_column("projects", "display_name", nullable=False)
    offline_ops.create_index("projects_blocked_idx", "projects", ["blocked"])
    planned_sql = [
        " ".join(statement.split())
        for statement in offline.getvalue().split(";")
        if statement.strip()
    ]
    assert planned_sql == [
        "ALTER TABLE projects ADD COLUMN blocked BOOLEAN",
        "ALTER TABLE projects RENAME name TO display_name",
        "ALTER TABLE projects ALTER COLUMN display_name SET NOT NULL",
        "CREATE INDEX projects_blocked_idx ON projects (blocked)",
    ], planned_sql

    generated.clear()
    boundaries.clear()
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        operations.add_column("projects", sa.Column("blocked", sa.Boolean(), nullable=True))
        operations.alter_column("projects", "name", new_column_name="display_name")
        operations.alter_column("projects", "display_name", nullable=False)
        operations.create_index("projects_blocked_idx", "projects", ["blocked"])
    assert boundaries == ["BEGIN", "COMMIT"], boundaries
    assert generated == planned_sql, generated
    assert [(c["name"], c["nullable"]) for c in sa.inspect(engine).get_columns("projects")] == [
        ("id", False),
        ("display_name", False),
        ("blocked", True),
    ]
    assert ("projects_blocked_idx", ["blocked"]) in [
        (index["name"], index["column_names"])
        for index in sa.inspect(engine).get_indexes("projects")
    ]

    generated.clear()
    boundaries.clear()
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        operations.drop_index("projects_blocked_idx", table_name="projects")
        operations.drop_column("projects", "blocked")
    assert boundaries == ["BEGIN", "COMMIT"], boundaries
    assert generated == [
        "DROP INDEX projects_blocked_idx",
        "ALTER TABLE projects DROP COLUMN blocked",
    ], generated
    assert [(c["name"], c["nullable"]) for c in sa.inspect(engine).get_columns("projects")] == [
        ("id", False),
        ("display_name", False),
    ]
    assert "projects_blocked_idx" not in {
        index["name"] for index in sa.inspect(engine).get_indexes("projects")
    }
    print(
        f"Alembic {alembic.__version__}: schema + index composition PASS; plan is "
        + " | ".join(planned_sql)
        + "; DROP INDEX projects_blocked_idx | ALTER TABLE projects DROP COLUMN blocked"
    )
    engine.dispose()


def main() -> None:
    environment = dict(
        os.environ,
        CARGO_TARGET_DIR=str(TARGET),
        NETBADB_POSTGRES_TRACE="1",
    )
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
        cwd=ROOT,
        env=environment,
        check=True,
    )
    for probe in (psql_probe, psycopg_probe, sqlalchemy_probe, alembic_probe):
        with tempfile.TemporaryFile(mode="w+") as trace:
            process = subprocess.Popen(
                [str(TARGET / "debug/examples/sql_alter_table_fixture")],
                cwd=ROOT,
                env=environment,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=trace,
                text=True,
            )
            try:
                address = process.stdout.readline().strip()
                assert address, "fixture did not start"
                probe(f"postgresql://netbadb@{address}/netbadb")
            finally:
                output, _ = process.communicate(timeout=60)
                trace.seek(0)
                diagnostics = trace.read()
                assert process.returncode == 0, diagnostics
                assert "REOPEN PASS" in output, output
                print(output.strip())


if __name__ == "__main__":
    main()
