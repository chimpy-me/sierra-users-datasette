#!/usr/bin/env python3
"""
extract_sierra_users_to_sqlite.py

Extract Sierra "users-related" data from Postgres (schema: sierra_view) into a SQLite DB.

Defaults:
- Reads SIERRA_PG_URL from environment (supports .env)
- Writes SQLite to ./users.db unless --sqlite-path or USERS_SQLITE_PATH is set

Env (suggested in .env):
  SIERRA_PG_URL=postgresql+psycopg://user:pass@host:5432/dbname
  USERS_SQLITE_PATH=./users.db        (optional)
  SIERRA_SCHEMA=sierra_view           (optional)

Usage:
  python extract_sierra_users_to_sqlite.py
  python extract_sierra_users_to_sqlite.py --sqlite-path ./data/users.db --drop
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, List, Tuple

import sqlite3
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from dotenv import load_dotenv


# Load .env from the current working directory (and optionally .env.local)
load_dotenv(dotenv_path=Path(".env"), override=False)
load_dotenv(dotenv_path=Path(".env.local"), override=False)


DEFAULT_SCHEMA = "sierra_view"

DEFAULT_OBJECTS = [
    "iii_user",
    "iii_user_location",
    "iii_user_iii_role",
    "iii_role",
    "iii_role_name",
    "iii_role_category",
    "iii_role_category_name",
    "iii_user_permission_myuser",
    "iii_user_application_myuser",
    "iii_user_workflow_myuser",
    "iii_user_printer_myuser",
    "iii_user_desktop_option",
    "iii_user_fund_master",
    "statistic_group",
    "statistic_group_name",
    "location",
    "location_name",
    "branch",
    "branch_name",
]

PGTYPE_TO_SQLITE = {
    "smallint": "INTEGER",
    "integer": "INTEGER",
    "bigint": "INTEGER",
    "numeric": "NUMERIC",
    "decimal": "NUMERIC",
    "real": "REAL",
    "double precision": "REAL",
    "character varying": "TEXT",
    "character": "TEXT",
    "text": "TEXT",
    "uuid": "TEXT",
    "date": "TEXT",
    "timestamp without time zone": "TEXT",
    "timestamp with time zone": "TEXT",
    "time without time zone": "TEXT",
    "time with time zone": "TEXT",
    "boolean": "INTEGER",
    "json": "TEXT",
    "jsonb": "TEXT",
    "bytea": "BLOB",
}


def normalize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, bool):
        return 1 if v else 0
    return v


def sqlite_fast_settings(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")  # ~200MB if available
    conn.execute("PRAGMA foreign_keys=OFF;")


def fetch_columns(pg: Engine, schema: str, obj: str) -> List[Tuple[str, str, bool]]:
    sql = text(
        """
        SELECT
            c.column_name,
            c.data_type,
            (c.is_nullable = 'YES') AS is_nullable
        FROM information_schema.columns c
        WHERE c.table_schema = :schema
          AND c.table_name = :obj
        ORDER BY c.ordinal_position
        """
    )
    with pg.connect() as conn:
        rows = conn.execute(sql, {"schema": schema, "obj": obj}).fetchall()
    return [(r[0], r[1], bool(r[2])) for r in rows]


def sqlite_type_for(pg_data_type: str) -> str:
    return PGTYPE_TO_SQLITE.get(pg_data_type.lower(), "TEXT")


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def create_sqlite_table(
    sq: sqlite3.Connection, table: str, columns: List[Tuple[str, str, bool]], drop: bool
) -> None:
    if drop:
        sq.execute(f"DROP TABLE IF EXISTS {quote_ident(table)};")

    cols_sql: List[str] = []
    for col_name, pg_type, is_nullable in columns:
        sqlite_type = sqlite_type_for(pg_type)
        null_sql = "" if is_nullable else " NOT NULL"
        cols_sql.append(f"{quote_ident(col_name)} {sqlite_type}{null_sql}")

    ddl = (
        f"CREATE TABLE IF NOT EXISTS {quote_ident(table)} (\n  "
        + ",\n  ".join(cols_sql)
        + "\n);"
    )
    sq.execute(ddl)


def stream_rows(
    pg: Engine, schema: str, obj: str, col_names: List[str], batch_size: int
) -> Iterable[List[Tuple[Any, ...]]]:
    select_sql = (
        "SELECT "
        + ", ".join(quote_ident(c) for c in col_names)
        + f" FROM {quote_ident(schema)}.{quote_ident(obj)}"
    )
    with pg.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(text(select_sql))
        while True:
            batch = result.fetchmany(batch_size)
            if not batch:
                break
            out: List[Tuple[Any, ...]] = []
            for row in batch:
                out.append(tuple(normalize_value(v) for v in row))
            yield out


def insert_batches(
    sq: sqlite3.Connection, table: str, col_names: List[str], batches: Iterable[List[Tuple[Any, ...]]]
) -> int:
    placeholders = ", ".join(["?"] * len(col_names))
    insert_sql = (
        f"INSERT INTO {quote_ident(table)} ("
        + ", ".join(quote_ident(c) for c in col_names)
        + f") VALUES ({placeholders});"
    )

    total = 0
    for batch in batches:
        sq.executemany(insert_sql, batch)
        total += len(batch)
    return total


def sqlite_table_columns(sq: sqlite3.Connection, table: str) -> List[str]:
    rows = sq.execute(f"PRAGMA table_info({quote_ident(table)});").fetchall()
    return [r[1] for r in rows]


def create_index_if_cols_exist(sq: sqlite3.Connection, table: str, cols: List[str], index_name: str) -> None:
    existing = set(sqlite_table_columns(sq, table))
    if all(c in existing for c in cols):
        cols_sql = ", ".join(quote_ident(c) for c in cols)
        sq.execute(
            f"CREATE INDEX IF NOT EXISTS {quote_ident(index_name)} "
            f"ON {quote_ident(table)} ({cols_sql});"
        )


def create_user_dim_view(sq: sqlite3.Connection) -> None:
    required = {
        "iii_user": {"id", "name", "full_name", "iii_user_group_code", "account_unit", "statistic_group_code_num", "is_suspended"},
        "statistic_group": {"code_num", "location_code"},
        "location": {"code", "branch_code_num"},
        "branch": {"code_num", "id"},
        "branch_name": {"branch_id", "name"},
    }

    for t, cols in required.items():
        try:
            existing = set(sqlite_table_columns(sq, t))
        except sqlite3.OperationalError:
            return
        if not cols.issubset(existing):
            return

    sq.execute("DROP VIEW IF EXISTS user_dim;")
    sq.execute(
        """
        CREATE VIEW user_dim AS
        SELECT
            iu.id                      AS user_id,
            iu.name                    AS user_name,
            iu.full_name               AS full_name,
            iu.iii_user_group_code     AS user_group_code,
            iu.account_unit            AS account_unit,
            iu.statistic_group_code_num AS statistic_group_code_num,
            sg.location_code           AS stats_location_code,
            bn.name                    AS branch_name,
            iu.is_suspended            AS is_suspended
        FROM iii_user iu
        LEFT JOIN statistic_group sg
            ON sg.code_num = iu.statistic_group_code_num
        LEFT JOIN location l
            ON l.code = sg.location_code
        LEFT JOIN branch b
            ON b.code_num = l.branch_code_num
        LEFT JOIN branch_name bn
            ON bn.branch_id = b.id
        ;
        """
    )


def resolve_sqlite_path(cli_value: str | None) -> Path:
    # Priority: CLI > ENV > default
    env_value = os.getenv("USERS_SQLITE_PATH")
    path_str = cli_value or env_value or "./users.db"
    p = Path(path_str).expanduser()
    return p


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg-url", default=os.getenv("SIERRA_PG_URL"), help="SQLAlchemy URL for Sierra Postgres (or SIERRA_PG_URL)")
    ap.add_argument("--schema", default=os.getenv("SIERRA_SCHEMA", DEFAULT_SCHEMA), help="Schema name (default: sierra_view)")
    ap.add_argument("--sqlite-path", default=None, help="Output SQLite path (or USERS_SQLITE_PATH). Default: ./users.db")
    ap.add_argument("--batch-size", type=int, default=5000, help="Fetch/insert batch size (default: 5000)")
    ap.add_argument("--drop", action="store_true", help="Drop and recreate SQLite tables")
    ap.add_argument("--strict", action="store_true", help="Fail if a listed object does not exist")
    ap.add_argument("--objects", nargs="*", default=DEFAULT_OBJECTS, help="Override the list of objects to extract")
    ap.add_argument("--no-vacuum", action="store_true", help="Skip VACUUM at the end (faster runs during iteration)")
    args = ap.parse_args()

    if not args.pg_url:
        print(
            "ERROR: Postgres URL not provided.\n"
            "Set SIERRA_PG_URL in your environment or .env, or pass --pg-url.",
            file=sys.stderr,
        )
        return 2

    sqlite_path = resolve_sqlite_path(args.sqlite_path)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    pg = create_engine(args.pg_url, future=True)

    with sqlite3.connect(str(sqlite_path)) as sq:
        sqlite_fast_settings(sq)

        for obj in args.objects:
            cols = fetch_columns(pg, args.schema, obj)
            if not cols:
                msg = f"Missing or inaccessible {args.schema}.{obj} (no columns found)"
                if args.strict:
                    raise RuntimeError(msg)
                print(f"[skip] {msg}")
                continue

            col_names = [c[0] for c in cols]
            print(f"[load] {args.schema}.{obj} -> sqlite:{obj} ({len(col_names)} cols) ... ", end="", flush=True)

            sq.execute("BEGIN;")
            try:
                create_sqlite_table(sq, obj, cols, drop=args.drop)
                batches = stream_rows(pg, args.schema, obj, col_names, args.batch_size)
                n = insert_batches(sq, obj, col_names, batches)
                sq.execute("COMMIT;")
                print(f"{n} rows")
            except Exception:
                sq.execute("ROLLBACK;")
                raise

        # Indexes and view
        sq.execute("BEGIN;")
        try:
            create_index_if_cols_exist(sq, "iii_user", ["name"], "idx_iii_user_name")
            create_index_if_cols_exist(sq, "iii_user", ["statistic_group_code_num"], "idx_iii_user_statgrp")
            create_index_if_cols_exist(sq, "iii_user", ["iii_user_group_code"], "idx_iii_user_group")

            create_index_if_cols_exist(sq, "iii_user_permission_myuser", ["user_name"], "idx_perm_user_name")
            create_index_if_cols_exist(sq, "iii_user_permission_myuser", ["permission_code"], "idx_perm_code")
            create_index_if_cols_exist(sq, "iii_user_application_myuser", ["user_name"], "idx_app_user_name")
            create_index_if_cols_exist(sq, "iii_user_workflow_myuser", ["user_name"], "idx_workflow_user_name")
            create_index_if_cols_exist(sq, "iii_user_location", ["user_name"], "idx_user_location_user")

            create_user_dim_view(sq)
            sq.execute("COMMIT;")
        except Exception:
            sq.execute("ROLLBACK;")
            raise

        if not args.no_vacuum:
            # Must not be in a transaction
            sq.execute("VACUUM;")

    print("[done] SQLite written:", str(sqlite_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
