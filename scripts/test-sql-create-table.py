#!/usr/bin/env python3
"""Round 19: fresh databases, real psql/psycopg/SQLAlchemy, no protocol workaround."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile

import psycopg
import sqlalchemy as sa

ROOT = Path(__file__).resolve().parents[1]
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "/private/tmp/netbadb-round19-target"))
PSQL = os.environ.get("PSQL", "/opt/local/lib/pgsql/bin/psql")
COLUMNS = "(id BIGINT NOT NULL, name TEXT, active BOOLEAN NOT NULL)"
ROW = (10, "demo", True)


def psql_probe(dsn: str) -> None:
    version = subprocess.check_output([PSQL, "--version"], text=True).strip()
    assert "17.11" in version, version
    statements = []
    for table, finish in [("rolled_back", "ROLLBACK"), ("committed", "COMMIT")]:
        statements.extend(["BEGIN;", f"CREATE TABLE {table} {COLUMNS};",
                           f"INSERT INTO {table} VALUES (10, 'demo', true);",
                           f"SELECT * FROM {table};", f"{finish};"])
    completed = subprocess.run([PSQL, "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", dsn],
                               input="\n".join(statements), text=True, capture_output=True, check=True)
    assert completed.stdout.count("10|demo|t") == 2, completed.stdout
    denied = subprocess.run([PSQL, "-X", "-w", "-c", "SELECT * FROM committed", dsn], text=True, capture_output=True)
    assert denied.returncode != 0 and "permission denied" in denied.stderr, denied
    catalog = subprocess.run([PSQL, "-X", "-w", "-c", r"\dt", dsn], text=True, capture_output=True, check=True)
    assert "committed" not in catalog.stdout
    print(f"{version}: transactional CREATE/DML/rollback/commit and post-commit denial PASS")


def psycopg_probe(dsn: str) -> None:
    assert psycopg.__version__ == "3.2.13", psycopg.__version__
    class RollbackProbe(Exception):
        pass
    with psycopg.connect(dsn) as connection:
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(f"CREATE TABLE rolled_back {COLUMNS}")
                    cursor.execute("INSERT INTO rolled_back VALUES (%s, %s, %s)", ROW)
                    cursor.execute("SELECT * FROM rolled_back")
                    assert cursor.fetchall() == [ROW]
                    raise RollbackProbe()
        except RollbackProbe:
            pass
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE TABLE committed {COLUMNS}", prepare=True)
                cursor.execute("INSERT INTO committed VALUES (%s, %s, %s)", ROW)
                cursor.execute("SELECT * FROM committed")
                assert cursor.fetchall() == [ROW]
        try:
            connection.execute("SELECT * FROM committed")
        except psycopg.errors.InsufficientPrivilege:
            connection.rollback()
        else:
            raise AssertionError("creator gained durable access")
    print(f"psycopg {psycopg.__version__}: default CREATE, named Extended CREATE and parameterized DML PASS")


def sqlalchemy_probe(dsn: str) -> None:
    assert sa.__version__ == "2.0.52", sa.__version__
    engine = sa.create_engine(dsn.replace("postgresql://", "postgresql+psycopg://", 1))
    metadata = sa.MetaData()
    class RollbackProbe(Exception):
        pass
    for name, rollback in [("rolled_back", True), ("committed", False)]:
        table = sa.Table(name, metadata, sa.Column("id", sa.BigInteger, nullable=False),
                         sa.Column("name", sa.Text, nullable=True),
                         sa.Column("active", sa.Boolean, nullable=False))
        try:
            with engine.begin() as connection:
                table.create(connection, checkfirst=False)
                connection.execute(table.insert().values(id=10, name="demo", active=True))
                assert tuple(connection.execute(sa.select(table)).one()) == ROW
                if rollback:
                    raise RollbackProbe()
        except RollbackProbe:
            pass
    try:
        with engine.connect() as connection:
            connection.execute(sa.select(metadata.tables["committed"]))
    except sa.exc.DBAPIError as error:
        assert isinstance(error.orig, psycopg.errors.InsufficientPrivilege), error
    else:
        raise AssertionError("SQLAlchemy creator gained durable access")
    engine.dispose()
    print(f"SQLAlchemy {sa.__version__}: Table.create(checkfirst=False), insert/select, rollback/commit PASS")


def main() -> None:
    environment = dict(os.environ, CARGO_TARGET_DIR=str(TARGET), NETBADB_POSTGRES_TRACE="1")
    subprocess.run(["cargo", "build", "--offline", "-p", "netbadb-server", "--example", "sql_create_table_fixture"],
                   cwd=ROOT, env=environment, check=True)
    for probe in (psql_probe, psycopg_probe, sqlalchemy_probe):
        with tempfile.TemporaryFile(mode="w+") as trace:
            process = subprocess.Popen([str(TARGET / "debug/examples/sql_create_table_fixture")],
                                       cwd=ROOT, env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=trace, text=True)
            try:
                address = process.stdout.readline().strip()
                assert address, "fixture did not start"
                probe(f"postgresql://netbadb@{address}/netbadb")
            finally:
                output, _ = process.communicate(timeout=30)
                trace.seek(0)
                diagnostics = trace.read()
                assert process.returncode == 0, diagnostics
                assert "REOPEN PASS" in output, output
                if probe is not psql_probe:
                    assert "Bind portal=" in diagnostics and "Execute portal=" in diagnostics, diagnostics
                    if probe is psycopg_probe:
                        assert any("Parse statement=" in line and "CREATE TABLE committed" in line for line in diagnostics.splitlines()), diagnostics
                    else:
                        assert any("Query sql=" in line and "CREATE TABLE" in line for line in diagnostics.splitlines()), diagnostics
                print(output.strip())


if __name__ == "__main__":
    main()
