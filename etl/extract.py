"""Extract: источник deals_bigdata -> DWH raw.* потоковым COPY.

Справочники перезаписываются целиком, prices/price_body_types/price_dates — окном loading_date.
Источник читается в одной REPEATABLE READ транзакции, поэтому все таблицы берутся из одного снимка.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import psycopg
from psycopg import sql

from etl.db import stream_copy
from etl.log import log_event

logger = logging.getLogger(__name__)

PRICE_COLUMNS: tuple[str, ...] = (
    "id", "created_at", "updated_at", "subscriber_id", "organization_id", "loading_date", "end_loading_date",
    "loading_city_gcid", "unloading_city_gcid", "distance", "total_weight", "total_volume", "body_type", "price",
    "currency_code", "complex", "price_type", "is_fact", "nearest_rate_date", "outlier_mark",
    "weight_category", "volume_category", "source",
)


@dataclass(frozen=True)
class Window:
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError(f"start_date {self.start} > end_date {self.end}")


def source_date_range(src: psycopg.Connection) -> Window:
    row = src.execute("SELECT min(loading_date), max(loading_date) FROM prices").fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("Source table prices is empty")
    return Window(row[0], row[1])


def _cols(columns: tuple[str, ...]) -> sql.Composable:
    return sql.SQL(", ").join(sql.Identifier(c) for c in columns)


def extract_to_raw(src: psycopg.Connection, dwh: psycopg.Connection, window: Window) -> dict:
    start, end = sql.Literal(window.start), sql.Literal(window.end)
    window_filter = sql.SQL("p.loading_date BETWEEN {} AND {}").format(start, end)
    price_cols = _cols(PRICE_COLUMNS)

    src.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    with src.transaction(), dwh.transaction(), dwh.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('raw_extract'))")

        # --- справочники: полная перезапись ---
        cur.execute("TRUNCATE raw.cities, raw.body_types, raw.exchange_rates")
        stream_copy(src, cur, sql.SQL("SELECT id, full_name_local, gc_id, name, longitude, latitude FROM cities"),
                    "raw.cities", ("id", "full_name_local", "gc_id", "name", "longitude", "latitude"))
        stream_copy(src, cur, sql.SQL('SELECT id, name, "id_from_1C", std_id FROM body_types'),
                    "raw.body_types", ("id", "name", "id_from_1c", "std_id"))
        stream_copy(src, cur, sql.SQL("SELECT id, num_code, value, setting_date FROM exchange_rates"),
                    "raw.exchange_rates", ("id", "num_code", "value", "setting_date"))

        # --- prices: окно через staging (ловит и сделки, у которых сдвинулся loading_date) ---
        cur.execute("CREATE TEMP TABLE stg_raw_prices (LIKE raw.prices INCLUDING DEFAULTS) ON COMMIT DROP")
        stream_copy(src, cur, sql.SQL("SELECT {} FROM prices p WHERE {}").format(price_cols, window_filter),
                    "stg_raw_prices", PRICE_COLUMNS)
        cur.execute(
            "DELETE FROM raw.prices WHERE loading_date BETWEEN %s AND %s OR id IN (SELECT id FROM stg_raw_prices)",
            (window.start, window.end),
        )
        replaced = cur.rowcount
        cur.execute(sql.SQL("INSERT INTO raw.prices ({0}) SELECT {0} FROM stg_raw_prices").format(price_cols))
        prices = cur.rowcount

        # Дочерние строки старой версии окна уже удалены каскадом.
        stream_copy(src, cur, sql.SQL(
            "SELECT pbt.price_id, pbt.body_type_id FROM price_body_types pbt JOIN prices p ON p.id = pbt.price_id "
            "WHERE {}").format(window_filter), "raw.price_body_types", ("price_id", "body_type_id"))
        stream_copy(src, cur, sql.SQL(
            "SELECT pd.price_id, pd.price_date FROM price_dates pd JOIN prices p ON p.id = pd.price_id "
            "WHERE {}").format(window_filter), "raw.price_dates", ("price_id", "price_date"))

        counts = {}
        for table in ("raw.cities", "raw.body_types", "raw.exchange_rates"):
            counts[table] = cur.execute(sql.SQL("SELECT count(*) FROM {}").format(
                sql.Identifier(*table.split(".")))).fetchone()[0]
        for table in ("raw.price_body_types", "raw.price_dates"):
            counts[table] = cur.execute(sql.SQL(
                "SELECT count(*) FROM {} c JOIN stg_raw_prices p ON p.id = c.price_id").format(
                sql.Identifier(*table.split(".")))).fetchone()[0]

    stats = {"raw.prices": prices, "raw.prices_replaced": replaced, **counts}
    log_event(logger, "extract.done", window_start=window.start, window_end=window.end, **stats)
    return stats
