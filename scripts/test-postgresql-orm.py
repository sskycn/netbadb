#!/usr/bin/env python3
"""Run the real psycopg 3 and SQLAlchemy PostgreSQL compatibility smoke test."""

from __future__ import annotations

import argparse

import psycopg
import sqlalchemy
from sqlalchemy import BigInteger, Boolean, Column, MetaData, Table, Text, delete, insert
from sqlalchemy import inspect, select, update
from sqlalchemy.orm import Session, registry


def psycopg_smoke(dsn: str) -> None:
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)
            cursor.execute("SELECT %s::BIGINT", (7,), prepare=True)
            assert cursor.fetchone() == (7,)
            cursor.execute(
                "INSERT INTO users (id, name, active) VALUES (%s, %s, %s)",
                (1, None, True),
                prepare=True,
            )
            cursor.execute(
                "UPDATE users SET name = %s WHERE id = %s",
                ("Ada", 1),
                prepare=True,
            )
            for expected in ("Ada", "Ada"):
                cursor.execute(
                    "SELECT name FROM users WHERE id = %s",
                    (1,),
                    prepare=True,
                )
                assert cursor.fetchone() == (expected,)
            cursor.execute(
                "INSERT INTO users (id, name, active) VALUES (%s, %s, %s)",
                (99, "temporary", False),
            )
            cursor.execute("DELETE FROM users WHERE id = %s", (99,), prepare=True)
        connection.commit()

        with connection.cursor(binary=True) as cursor:
            cursor.execute(
                "SELECT id, name, active FROM users WHERE id = %s",
                (1,),
                prepare=True,
            )
            assert cursor.fetchone() == (1, "Ada", True)

        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO users (id, name, active) VALUES (%s, %s, %s)",
                    (98, "committed", True),
                )
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM users WHERE id = %s", (98,))
        connection.commit()

        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT definitely_missing FROM users")
        except psycopg.Error:
            connection.rollback()
        else:
            raise AssertionError("expected failed statement")
        with connection.cursor() as cursor:
            cursor.execute("SELECT %s::BIGINT", (8,))
            assert cursor.fetchone() == (8,)
        connection.rollback()


def sqlalchemy_smoke(dsn: str) -> None:
    engine = sqlalchemy.create_engine(dsn)
    metadata = MetaData()
    users = Table(
        "users",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("name", Text, nullable=True),
        Column("active", Boolean, nullable=False),
    )

    with engine.connect() as connection:
        assert connection.execute(select(1)).scalar_one() == 1
        assert connection.execute(select(users.c.name).where(users.c.id == 1)).scalar_one() == "Ada"
        connection.execute(insert(users).values(id=2, name="Grace", active=True))
        connection.execute(
            update(users).where(users.c.id == 2).values(name="Grace Hopper")
        )
        assert (
            connection.execute(select(users.c.name).where(users.c.id == 2)).scalar_one()
            == "Grace Hopper"
        )
        connection.execute(delete(users).where(users.c.id == 2))
        connection.commit()

        transaction = connection.begin()
        connection.execute(insert(users).values(id=3, name="rollback", active=False))
        transaction.rollback()
        assert connection.execute(select(users.c.id).where(users.c.id == 3)).first() is None
        connection.rollback()

    inspector = inspect(engine)
    assert inspector.get_schema_names() == ["public"]
    assert inspector.get_table_names() == ["teams", "users"]
    assert inspector.has_table("users")
    assert inspector.has_table("users", schema="public")
    assert not inspector.has_table("missing")
    columns = inspector.get_columns("users")
    assert [(column["name"], column["nullable"]) for column in columns] == [
        ("id", False),
        ("name", True),
        ("active", False),
    ]
    assert inspector.get_pk_constraint("users")["constrained_columns"] == ["id"]
    assert inspector.get_indexes("users") == []

    reflected = Table("users", MetaData(), autoload_with=engine)
    assert list(reflected.columns) == [
        reflected.c.id,
        reflected.c.name,
        reflected.c.active,
    ]
    assert list(reflected.primary_key.columns) == [reflected.c.id]
    for _ in range(3):
        repeated = Table("users", MetaData(), autoload_with=engine)
        assert repeated.c.id.primary_key
    teams = Table("teams", MetaData(), autoload_with=engine)
    assert list(teams.columns.keys()) == ["id", "name"]

    with engine.connect() as connection:
        row = connection.execute(select(reflected).where(reflected.c.id == 1)).one()
        assert tuple(row) == (1, "Ada", True)

    mapper_registry = registry()

    class User:
        pass

    mapper_registry.map_imperatively(User, reflected)
    with Session(engine) as session:
        user = session.scalars(select(User).where(reflected.c.id == 1)).one()
        assert user.name == "Ada"
        user.name = "ORM Ada"
        session.commit()
    with Session(engine) as session:
        user = session.get(User, 1)
        assert user is not None and user.name == "ORM Ada"
        created = User()
        created.id = 4
        created.name = "ORM write"
        created.active = True
        session.add(created)
        session.commit()
        session.delete(created)
        session.commit()

    with engine.begin() as connection:
        connection.execute(update(users).where(users.c.id == 1).values(name="Ada"))
    engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dsn",
        required=True,
        help="postgresql+psycopg URL for SQLAlchemy; the psycopg URL is derived from it",
    )
    args = parser.parse_args()
    sqlalchemy_dsn = args.dsn
    psycopg_dsn = sqlalchemy_dsn.replace("postgresql+psycopg://", "postgresql://", 1)
    assert psycopg_dsn != sqlalchemy_dsn, "expected a postgresql+psycopg:// URL"
    psycopg_smoke(psycopg_dsn)
    sqlalchemy_smoke(sqlalchemy_dsn)
    print(
        "PostgreSQL ORM compatibility passed: "
        f"Python, psycopg {psycopg.__version__}, SQLAlchemy {sqlalchemy.__version__}"
    )


if __name__ == "__main__":
    main()
