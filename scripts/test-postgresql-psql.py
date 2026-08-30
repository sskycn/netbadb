#!/usr/bin/env python3
"""Exercise the supported psql 17.11 describe surface against the real client."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PSQL = Path("/opt/local/lib/pgsql/bin/psql")
TARGET_DIR = Path(
    os.environ.get("NETBADB_PSQL_TARGET_DIR", "/private/tmp/netbadb-round5-psql-target")
)


def psql_binary() -> str:
    configured = os.environ.get("PSQL")
    if configured:
        return configured
    if DEFAULT_PSQL.is_file():
        return str(DEFAULT_PSQL)
    discovered = shutil.which("psql")
    if discovered:
        return discovered
    raise RuntimeError("psql was not found; set PSQL or install PostgreSQL 17.11")


def require(output: str, *needles: str) -> None:
    missing = [needle for needle in needles if needle not in output]
    if missing:
        raise AssertionError(f"missing {missing!r} in psql output:\n{output}")


def main() -> int:
    psql = psql_binary()
    version = subprocess.run(
        [psql, "--version"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    ).stdout.strip()
    if "17.11" not in version:
        raise RuntimeError(f"this compatibility probe requires psql 17.11, found: {version}")

    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(TARGET_DIR)
    subprocess.run(
        [
            "cargo",
            "build",
            "--offline",
            "-p",
            "netbadb-server",
            "--example",
            "postgres_driver_fixture",
        ],
        cwd=ROOT,
        env=environment,
        check=True,
    )
    fixture = subprocess.Popen(
        [str(TARGET_DIR / "debug/examples/postgres_driver_fixture")],
        cwd=ROOT,
        env=environment,
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        if fixture.stdout is None:
            raise RuntimeError("fixture stdout pipe was not created")
        address = fixture.stdout.readline().strip()
        if not address:
            if fixture.stderr is None:
                raise RuntimeError("fixture stopped before publishing its address")
            raise RuntimeError(f"fixture failed to start: {fixture.stderr.read()}")
        host, port = address.rsplit(":", 1)

        def describe(command: str, expected_returncodes: tuple[int, ...] = (0,)) -> str:
            arguments = [
                psql,
                "-X",
                "-w",
                "-h",
                host,
                "-p",
                port,
                "-U",
                "netbadb",
                "-d",
                "test",
            ]
            if os.environ.get("NETBADB_PSQL_ECHO_HIDDEN"):
                arguments.append("-E")
            completed = subprocess.run(
                [*arguments, "-c", command],
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            if completed.returncode not in expected_returncodes:
                raise RuntimeError(
                    f"psql {command!r} failed with {completed.returncode}:\n{completed.stdout}"
                )
            return completed.stdout

        table = describe(r"\d users")
        require(
            table,
            'Table "public.users"',
            "id",
            "bigint",
            "name",
            "text",
            "active",
            "boolean",
            '"users_pkey" PRIMARY KEY',
            "nb_users_active_8c133803f178_idx",
            "nb_users_name_e8563df82c02_idx",
        )
        require(describe(r"\d public.users"), 'Table "public.users"')
        require(describe(r"\d user*"), 'Table "public.users"')
        require(
            describe(r"\d missing", expected_returncodes=(0, 1)),
            'Did not find any relation named "missing".',
        )

        tables = describe(r"\dt")
        require(tables, "public", "users", "teams", "table", "netbadb")
        user_tables = describe(r"\dt user*")
        require(user_tables, "users")
        if "teams" in user_tables:
            raise AssertionError(f"table pattern leaked teams:\n{user_tables}")
        require(describe(r"\dt public.*"), "users", "teams")

        indexes = describe(r"\di")
        require(
            indexes,
            "users_pkey",
            "teams_pkey",
            "nb_users_active_8c133803f178_idx",
            "nb_users_name_e8563df82c02_idx",
            "users",
        )
        user_indexes = describe(r"\di *users*")
        require(
            user_indexes,
            "users_pkey",
            "nb_users_active_8c133803f178_idx",
            "nb_users_name_e8563df82c02_idx",
        )
        if "teams_pkey" in user_indexes:
            raise AssertionError(f"index pattern leaked teams_pkey:\n{user_indexes}")

        rolled_back = describe(
            "BEGIN; CREATE INDEX users_id_rolled_back_idx ON users (id); ROLLBACK;"
        )
        require(rolled_back, "BEGIN", "CREATE INDEX", "ROLLBACK")
        absent = describe(r"\di *rolled_back*", expected_returncodes=(0, 1))
        if "users_id_rolled_back_idx" in absent:
            raise AssertionError(f"rolled-back index remained visible:\n{absent}")

        require(describe("CREATE INDEX users_id_round6_idx ON users (id);"), "CREATE INDEX")
        require(describe(r"\di *round6*"), "users_id_round6_idx", "users")
        require(describe(r"\d users"), "users_id_round6_idx")
        require(describe("BEGIN; DROP INDEX users_id_round6_idx; ROLLBACK;"), "DROP INDEX", "ROLLBACK")
        require(describe(r"\di *round6*"), "users_id_round6_idx")
        require(describe("BEGIN; DROP INDEX public.users_id_round6_idx; COMMIT;"), "DROP INDEX", "COMMIT")
        for command in [r"\di", r"\d users"]:
            assert "users_id_round6_idx" not in describe(command)
        require(describe("DROP INDEX IF EXISTS users_id_round6_idx;"), "DROP INDEX")
        require(describe("CREATE INDEX users_id_round7_idx ON users (id);"), "CREATE INDEX")
        require(describe(r"\di *round7*"), "users_id_round7_idx")
        require(describe(r"\d users"), "users_id_round7_idx")
        require(describe("DROP INDEX users_id_round7_idx;"), "DROP INDEX")
        for command in [r"\di", r"\d users"]:
            assert "users_id_round7_idx" not in describe(command)

    finally:
        if fixture.stdin is not None:
            fixture.stdin.close()
        try:
            fixture.wait(timeout=5)
        except subprocess.TimeoutExpired:
            fixture.terminate()
            fixture.wait(timeout=5)

    print(f"psql compatibility passed ({version})")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"psql compatibility failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
