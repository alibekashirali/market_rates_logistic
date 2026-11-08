"""Работа с PostgreSQL: подключения, чтение в DataFrame, пакетная запись через COPY."""
from __future__ import annotations

import io
from collections.abc import Sequence
from typing import Any

import pandas as pd
import psycopg
from psycopg import sql


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, application_name="smart-logistics-etl")


def read_frame(conn: psycopg.Connection, query: str | sql.Composable, params: Any = None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def copy_frame(cur: psycopg.Cursor, table: str, df: pd.DataFrame, columns: Sequence[str]) -> int:
    """Векторизованная запись DataFrame одним COPY (CSV в памяти), без построчных INSERT."""
    if df.empty:
        return 0
    buf = io.StringIO()
    # Пустое неэкранированное поле в CSV-режиме COPY = NULL.
    df.loc[:, list(columns)].to_csv(buf, index=False, header=False, na_rep="")
    stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
        sql.Identifier(*table.split(".")),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )
    with cur.copy(stmt) as copy:
        copy.write(buf.getvalue())
    return len(df)


def stream_copy(
    src: psycopg.Connection,
    dst_cur: psycopg.Cursor,
    select: sql.Composable,
    target: str,
    columns: Sequence[str],
) -> None:
    """Потоковая перекачка результата SELECT из одной БД в таблицу другой (COPY TO -> COPY FROM)."""
    out_stmt = sql.SQL("COPY ({}) TO STDOUT").format(select)
    in_stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(*target.split(".")),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )
    with src.cursor() as src_cur, src_cur.copy(out_stmt) as out, dst_cur.copy(in_stmt) as inp:
        for chunk in out:
            inp.write(chunk)
