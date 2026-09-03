#!/usr/bin/env python3
"""Round 31 real-client audit of the current DDL-DML-DDL seal boundary."""
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

ROOT = Path(__file__).resolve().parents[1]
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "/private/tmp/netbadb-round31-target"))
PSQL = os.environ.get("PSQL", "/opt/local/lib/pgsql/bin/psql")

ADD = "ALTER TABLE projects ADD COLUMN normalized_name TEXT"
BACKFILL = "UPDATE projects SET normalized_name = 'filled'"
REFINE = "ALTER TABLE projects ALTER COLUMN normalized_name SET NOT NULL"


def verify_base(dsn: str) -> None:
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id, name FROM projects ORDER BY id")
            assert cursor.fetchall() == [(1, "one")]


def psql_probe(dsn: str) -> None:
    version = subprocess.check_output([PSQL, "--version"], text=True).strip()
    assert "17.11" in version, version
    result = subprocess.run(
        [PSQL, "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", dsn],
        input=f"\\set VERBOSITY verbose\nBEGIN;\n{ADD};\n{BACKFILL};\n{REFINE};\nCOMMIT;\n",
        text=True,
        capture_output=True,
    )
    assert result.returncode != 0, result.stdout
    assert "25000" in result.stderr, result.stderr
    verify_base(dsn)
    print(f"{version}: current final refinement rejected with 25000; disconnect rollback PASS")


def psycopg_probe(dsn: str) -> None:
    assert psycopg.__version__ == "3.2.13", psycopg.__version__
    with psycopg.connect(dsn) as connection:
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(ADD, prepare=True)
                    cursor.execute(BACKFILL, prepare=True)
                    cursor.execute(REFINE, prepare=True)
        except psycopg.errors.InvalidTransactionState as error:
            assert error.sqlstate == "25000"
        else:
            raise AssertionError("current post-DML refinement unexpectedly succeeded")
    verify_base(dsn)
    print(f"psycopg {psycopg.__version__}: prepare=True 25000 + rollback PASS")


def sqlalchemy_probe(dsn: str) -> None:
    assert sa.__version__ == "2.0.52", sa.__version__
    engine = sa.create_engine(dsn.replace("postgresql://", "postgresql+psycopg://", 1))
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(ADD)
            connection.execute(sa.text(BACKFILL))
            connection.exec_driver_sql(REFINE)
    except sa.exc.DBAPIError as error:
        assert error.orig.sqlstate == "25000", error
    else:
        raise AssertionError("current post-DML refinement unexpectedly succeeded")
    verify_base(dsn)
    engine.dispose()
    print(f"SQLAlchemy {sa.__version__}: text UPDATE, 25000, context rollback PASS")


def alembic_probe(dsn: str) -> None:
    assert alembic.__version__ == "1.16.5", alembic.__version__
    offline = io.StringIO()
    offline_ops = Operations(MigrationContext.configure(
        dialect_name="postgresql", opts={"as_sql": True, "output_buffer": offline}
    ))
    offline_ops.add_column("projects", sa.Column("normalized_name", sa.Text(), nullable=True))
    offline_ops.execute(sa.text(BACKFILL))
    offline_ops.alter_column("projects", "normalized_name", nullable=False)
    planned = [" ".join(sql.split()) for sql in offline.getvalue().split(";") if sql.strip()]
    assert planned == [ADD, BACKFILL, REFINE], planned

    engine = sa.create_engine(dsn.replace("postgresql://", "postgresql+psycopg://", 1))
    generated: list[str] = []
    boundaries: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def capture(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith(("ALTER TABLE", "UPDATE")):
            generated.append(" ".join(statement.split()))

    for name in ("begin", "commit", "rollback"):
        event.listen(engine, name, lambda _connection, name=name: boundaries.append(name.upper()))

    try:
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            operations.add_column(
                "projects", sa.Column("normalized_name", sa.Text(), nullable=True)
            )
            operations.execute(sa.text(BACKFILL))
            operations.alter_column("projects", "normalized_name", nullable=False)
    except sa.exc.DBAPIError as error:
        assert error.orig.sqlstate == "25000", error
    else:
        raise AssertionError("current Alembic post-DML refinement unexpectedly succeeded")
    assert generated == planned, generated
    assert boundaries == ["BEGIN", "ROLLBACK"], boundaries
    verify_base(dsn)
    engine.dispose()
    print(f"Alembic {alembic.__version__}: exact SQL + BEGIN/ROLLBACK + 25000 PASS")


def main() -> None:
    environment = dict(os.environ, CARGO_TARGET_DIR=str(TARGET), NETBADB_POSTGRES_TRACE="1")
    subprocess.run(
        ["cargo", "build", "--offline", "-p", "netbadb-server", "--example", "sql_alter_table_fixture"],
        cwd=ROOT, env=environment, check=True,
    )
    for probe in (psql_probe, psycopg_probe, sqlalchemy_probe, alembic_probe):
        probe_environment = dict(environment, NETBADB_ROUND31_PROBE=probe.__name__)
        with tempfile.TemporaryFile(mode="w+") as trace:
            process = subprocess.Popen(
                [str(TARGET / "debug/examples/sql_alter_table_fixture")],
                cwd=ROOT, env=probe_environment, stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=trace, text=True,
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
