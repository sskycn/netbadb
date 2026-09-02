#!/usr/bin/env python3
"""Round 26 real psql, psycopg, SQLAlchemy and Alembic ALTER probes."""
from __future__ import annotations

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
    script = """
BEGIN;
ALTER TABLE projects ADD COLUMN active BOOLEAN;
SELECT id, name, active FROM projects;
INSERT INTO projects VALUES (2, 'two', true);
ROLLBACK;
ALTER TABLE projects ADD COLUMN active BOOLEAN;
ALTER TABLE projects RENAME COLUMN name TO title;
ALTER TABLE projects ALTER COLUMN title SET NOT NULL;
ALTER TABLE projects ALTER COLUMN title DROP NOT NULL;
ALTER TABLE projects DROP COLUMN active;
ALTER TABLE projects RENAME TO work;
SELECT id, title FROM work;
"""
    result = subprocess.run(
        [PSQL, "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", dsn],
        input=script,
        text=True,
        capture_output=True,
        check=True,
    )
    assert "1|one|" in result.stdout and "1|one" in result.stdout, result.stdout
    print(f"{version}: six ALTER operations, post-ALTER DML and rollback PASS")


def psycopg_probe(dsn: str) -> None:
    assert psycopg.__version__ == "3.2.13", psycopg.__version__
    with psycopg.connect(dsn) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(
                    "ALTER TABLE projects ADD COLUMN active BOOLEAN", prepare=True
                )
                cursor.execute("SELECT id, name, active FROM projects")
                assert cursor.fetchall() == [(1, "one", None)]
                cursor.execute(
                    "INSERT INTO projects VALUES (%s, %s, %s)",
                    (2, "two", True),
                    prepare=True,
                )
        with connection.cursor() as cursor:
            cursor.execute("SELECT id, active FROM projects ORDER BY id", prepare=True)
            assert cursor.fetchall() == [(1, None), (2, True)]
        connection.commit()
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("ALTER TABLE projects RENAME TO work")
                cursor.execute("SELECT id, active FROM work ORDER BY id")
                assert cursor.fetchall() == [(1, None), (2, True)]
    print(
        f"psycopg {psycopg.__version__}: default and prepare=True ALTER plus "
        "same-transaction DML PASS"
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
    with engine.begin() as connection:
        connection.exec_driver_sql("ALTER TABLE projects ADD COLUMN active BOOLEAN")
    add_oid = table_oid("projects")
    inspector = sa.inspect(engine)
    assert [(c["name"], c["nullable"]) for c in inspector.get_columns("projects")] == [
        ("id", False),
        ("name", True),
        ("active", True),
    ]
    with engine.begin() as connection:
        connection.exec_driver_sql("ALTER TABLE projects RENAME TO work")
    rename_oid = table_oid("work")
    assert "work" in sa.inspect(engine).get_table_names()
    assert [(c["name"], c["nullable"]) for c in sa.inspect(engine).get_columns("work")] == [
        ("id", False),
        ("name", True),
        ("active", True),
    ]
    assert [
        (index["name"], index["column_names"])
        for index in sa.inspect(engine).get_indexes("work")
    ] == [("projects_name_idx", ["name"])]
    assert len({before_oid, add_oid, rename_oid}) == 3
    engine.dispose()
    print(
        f"SQLAlchemy {sa.__version__}: exec_driver_sql and reflection PASS; "
        f"fingerprint-keyed table OIDs {before_oid} -> {add_oid} -> {rename_oid}"
    )


def alembic_probe(dsn: str) -> None:
    assert alembic.__version__ == "1.16.5", alembic.__version__
    engine = sa.create_engine(dsn.replace("postgresql://", "postgresql+psycopg://", 1))
    generated: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def capture(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("ALTER TABLE"):
            generated.append(" ".join(statement.split()))

    def apply(operation) -> None:
        with engine.begin() as connection:
            operation(Operations(MigrationContext.configure(connection)))

    apply(lambda op: op.add_column("projects", sa.Column("active", sa.Boolean(), nullable=True)))
    apply(lambda op: op.alter_column("projects", "name", nullable=False))
    apply(lambda op: op.alter_column("projects", "name", nullable=True))
    apply(lambda op: op.alter_column("projects", "name", new_column_name="title"))
    apply(lambda op: op.drop_column("projects", "active"))
    apply(lambda op: op.rename_table("projects", "work"))
    assert [(c["name"], c["nullable"]) for c in sa.inspect(engine).get_columns("work")] == [
        ("id", False),
        ("title", True),
    ]
    assert [
        (index["name"], index["column_names"])
        for index in sa.inspect(engine).get_indexes("work")
    ] == [("projects_name_idx", ["title"])]
    assert len(generated) == 6, generated
    print(
        f"Alembic {alembic.__version__}: six independent single-operation transactions PASS; generated SQL: "
        + " | ".join(generated)
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
