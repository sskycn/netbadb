#!/usr/bin/env python3
"""Compare schemas and apply strictly guarded CreateIndexOp/DropIndexOp changes."""

from __future__ import annotations

import argparse

import alembic
import sqlalchemy
from alembic.autogenerate import compare_metadata, produce_migrations
from alembic.migration import MigrationContext
from alembic.operations import Operations, ops
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


def apply_index_changes(engine: sqlalchemy.Engine, metadata: MetaData) -> list[str]:
    with engine.begin() as connection:
        context = MigrationContext.configure(connection, opts={"compare_type": True, "include_schemas": False})
        migration = produce_migrations(context, metadata)
        pending = []

        def guard(container: object) -> None:
            for operation in container.ops:
                if isinstance(operation, ops.ModifyTableOps):
                    guard(operation)
                elif isinstance(operation, (ops.CreateIndexOp, ops.DropIndexOp)):
                    pending.append(operation)
                else:
                    raise AssertionError(f"refusing non-index migration: {operation!r}")

        guard(migration.upgrade_ops)
        # Validate the entire proposal before invoking any mutation.
        operations = Operations(context)
        for operation in pending:
            operations.invoke(operation)
        return [type(operation).__name__ for operation in pending]


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

    target = existing_schema(names, with_indexes=True)
    Index("users_id_alembic_idx", target.tables["users"].c.id, unique=False)
    add_index = compare(engine, target)
    assert len(add_index) == 1, add_index
    difference = add_index[0]
    assert isinstance(difference, tuple) and difference[0] == "add_index", add_index
    proposed = difference[1]
    assert proposed.name == "users_id_alembic_idx"
    assert proposed.table.name == "users"
    assert [column.name for column in proposed.columns] == ["id"]
    assert not proposed.unique

    assert apply_index_changes(engine, target) == ["CreateIndexOp"]
    assert compare(engine, target) == []
    baseline = existing_schema(names, with_indexes=True)
    named_removal = compare(engine, baseline)
    assert len(named_removal) == 1 and named_removal[0][0] == "remove_index", named_removal
    assert apply_index_changes(engine, baseline) == ["DropIndexOp"]
    assert compare(engine, baseline) == []
    # Legacy synthetic aliases must resolve in the PG adapter to generic IDs.
    no_indexes = existing_schema(names, with_indexes=False)
    assert apply_index_changes(engine, no_indexes) == ["DropIndexOp", "DropIndexOp"]
    assert compare(engine, no_indexes) == []
    engine.dispose()
    print(
        "Alembic guarded index-only mutation passed: "
        f"Python, psycopg, SQLAlchemy {sqlalchemy.__version__}, "
        f"Alembic {alembic.__version__}; baseline differences=0, "
        f"remove_index differences={len(remove_indexes)}, add_index applied=1, named remove_index applied=1, legacy remove_index applied=2, final differences=0"
    )


if __name__ == "__main__":
    main()
