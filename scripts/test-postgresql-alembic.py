#!/usr/bin/env python3
"""Run Alembic's schema comparison without executing migration DDL."""

from __future__ import annotations

import argparse

import alembic
import sqlalchemy
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import BigInteger, Boolean, Column, Index, MetaData, Table, Text, inspect


def reflected_index_names(engine: sqlalchemy.Engine) -> dict[str, str]:
    indexes = inspect(engine).get_indexes("users", schema="public")
    by_column = {
        index["column_names"][0]: index["name"]
        for index in indexes
        if len(index["column_names"]) == 1 and not index["unique"]
    }
    assert set(by_column) == {"name", "active"}
    return by_column


def existing_schema(index_names: dict[str, str], *, with_indexes: bool) -> MetaData:
    metadata = MetaData()
    users = Table(
        "users",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("name", Text, nullable=True),
        Column("active", Boolean, nullable=False),
    )
    Table(
        "teams",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("name", Text, nullable=False),
    )
    if with_indexes:
        Index(index_names["name"], users.c.name, unique=False)
        Index(index_names["active"], users.c.active, unique=False)
    return metadata


def compare(engine: sqlalchemy.Engine, metadata: MetaData) -> list[object]:
    with engine.connect() as connection:
        context = MigrationContext.configure(
            connection,
            opts={"compare_type": True, "include_schemas": False},
        )
        differences = compare_metadata(context, metadata)
        connection.rollback()
        return differences


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    args = parser.parse_args()

    engine = sqlalchemy.create_engine(args.dsn)
    names = reflected_index_names(engine)

    matching = compare(engine, existing_schema(names, with_indexes=True))
    assert matching == [], f"matching schema produced differences: {matching!r}"

    without_indexes = compare(engine, existing_schema(names, with_indexes=False))
    remove_indexes = [
        difference
        for difference in without_indexes
        if isinstance(difference, tuple) and difference[0] == "remove_index"
    ]
    assert len(remove_indexes) == 2, without_indexes
    assert not any(
        isinstance(difference, tuple) and difference[0] == "add_index"
        for difference in without_indexes
    )
    engine.dispose()
    print(
        "Alembic read-only comparison passed: "
        f"Python, psycopg, SQLAlchemy {sqlalchemy.__version__}, "
        f"Alembic {alembic.__version__}; baseline differences=0, "
        f"remove_index differences={len(remove_indexes)}"
    )


if __name__ == "__main__":
    main()
