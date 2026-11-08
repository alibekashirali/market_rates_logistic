"""Transform + Load: raw.* -> DQ (etl.validate) -> core.* атомарной перезаписью окна loading_date."""
from __future__ import annotations

import logging
import uuid

import pandas as pd
import psycopg

from etl.db import copy_frame, read_frame
from etl.extract import Window
from etl.log import log_event
from etl.validate import ValidationResult, validate_prices

logger = logging.getLogger(__name__)

FACT_COLUMNS: tuple[str, ...] = (
    "price_id", "created_at", "subscriber_id", "organization_id", "loading_date", "end_loading_date",
    "loading_city_gcid", "unloading_city_gcid", "distance", "total_weight", "total_volume", "body_type",
    "price", "currency_code", "nearest_rate_date", "fx_rate", "price_rub", "price_type", "is_fact", "complex",
    "outlier_mark", "weight_category", "volume_category", "weight_category_filled", "volume_category_filled",
    "etl_run_id",
)
QUARANTINE_COLUMNS: tuple[str, ...] = (
    "price_id", "loading_date", "reason_code", "reason_codes", "payload", "etl_run_id",
)
_INT_COLUMNS = ("subscriber_id", "organization_id", "body_type", "price_type", "loading_city_gcid", "unloading_city_gcid")

RAW_PRICES_SQL = """
SELECT id, created_at, subscriber_id, organization_id, loading_date, end_loading_date,
       loading_city_gcid, unloading_city_gcid, distance, total_weight, total_volume, body_type, price,
       currency_code, complex, price_type, is_fact, nearest_rate_date, outlier_mark,
       weight_category, volume_category
FROM raw.prices
WHERE loading_date BETWEEN %s AND %s
"""
RAW_RATES_SQL = """
SELECT DISTINCT ON (num_code, setting_date) num_code, setting_date, value
FROM raw.exchange_rates
ORDER BY num_code, setting_date, id DESC
"""

# Дедупликация городов: одна строка на gc_id с минимальным cities.id; «Город, Регион» -> city_name / region.
REFRESH_DIM_CITY_SQL = """
DELETE FROM core.dim_city;
INSERT INTO core.dim_city (gc_id, city_id, city_name, region, full_name_local, latitude, longitude)
SELECT DISTINCT ON (gc_id)
       gc_id,
       id,
       COALESCE(NULLIF(btrim(split_part(full_name_local, ',', 1)), ''), name),
       NULLIF(btrim(CASE WHEN strpos(full_name_local, ',') > 0
                         THEN substr(full_name_local, strpos(full_name_local, ',') + 1) END), ''),
       full_name_local, latitude, longitude
FROM raw.cities
ORDER BY gc_id, id;
"""

# Основная группа кузова сделки: prices.body_type, при его отсутствии — минимальный код из price_body_types.
SET_PRIMARY_BODY_GROUP_SQL = """
UPDATE stg_fact f
SET body_group = core.body_group_of(COALESCE(
        f.body_type,
        (SELECT min(pbt.body_type_id) FROM raw.price_body_types pbt WHERE pbt.price_id = f.price_id)))
"""

# Все группы кузова сделки без дублей внутри группы; без строк price_body_types -> prices.body_type -> «Не указан».
INSERT_BODY_GROUPS_SQL = """
INSERT INTO core.fact_price_body_group (price_id, body_group)
SELECT DISTINCT s.price_id, core.body_group_of(s.body_type_id)
FROM (
    SELECT f.price_id, pbt.body_type_id
    FROM stg_fact f
    JOIN raw.price_body_types pbt ON pbt.price_id = f.price_id
    UNION ALL
    SELECT f.price_id, f.body_type
    FROM stg_fact f
    WHERE NOT EXISTS (SELECT 1 FROM raw.price_body_types pbt WHERE pbt.price_id = f.price_id)
) s
"""

INSERT_PRICE_MONTHS_SQL = """
INSERT INTO core.fact_price_month (price_id, price_month, price_date)
SELECT DISTINCT ON (pd.price_id, date_trunc('month', pd.price_date))
       pd.price_id, date_trunc('month', pd.price_date)::date, pd.price_date
FROM raw.price_dates pd
JOIN stg_fact f ON f.price_id = pd.price_id
ORDER BY pd.price_id, date_trunc('month', pd.price_date), pd.price_date
"""

WINDOW_OR_BATCH = "loading_date BETWEEN %(start)s AND %(end)s " \
                  "OR price_id IN (SELECT price_id FROM stg_fact) OR price_id IN (SELECT price_id FROM stg_quarantine)"


def read_raw_window(dwh: psycopg.Connection, window: Window) -> tuple[pd.DataFrame, set[int], pd.DataFrame]:
    prices = read_frame(dwh, RAW_PRICES_SQL, (window.start, window.end))
    for col in _INT_COLUMNS:
        prices[col] = prices[col].astype("Int64")
    for col in ("distance", "total_weight", "total_volume", "price"):
        prices[col] = prices[col].astype(float)
    city_gcids = {int(r[0]) for r in dwh.execute("SELECT DISTINCT gc_id FROM raw.cities").fetchall()}
    rates = read_frame(dwh, RAW_RATES_SQL)
    return prices, city_gcids, rates


def transform(dwh: psycopg.Connection, window: Window) -> ValidationResult:
    prices, city_gcids, rates = read_raw_window(dwh, window)
    result = validate_prices(prices, city_gcids, rates)
    log_event(logger, "dq.done", window_start=window.start, window_end=window.end, **result.stats)
    return result


def load_core(dwh: psycopg.Connection, window: Window, result: ValidationResult, run_id: uuid.UUID) -> dict:
    clean = result.clean.assign(etl_run_id=str(run_id))
    quarantine = result.quarantine.assign(etl_run_id=str(run_id))
    params = {"start": window.start, "end": window.end}

    with dwh.transaction(), dwh.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('core_load'))")
        cur.execute(REFRESH_DIM_CITY_SQL)

        cur.execute("CREATE TEMP TABLE stg_fact (LIKE core.fact_prices INCLUDING DEFAULTS) ON COMMIT DROP")
        cur.execute("CREATE TEMP TABLE stg_quarantine (LIKE core.dq_quarantine_prices INCLUDING DEFAULTS) ON COMMIT DROP")
        copy_frame(cur, "stg_fact", clean, FACT_COLUMNS)
        copy_frame(cur, "stg_quarantine", quarantine, QUARANTINE_COLUMNS)
        cur.execute("ANALYZE stg_fact")
        cur.execute(SET_PRIMARY_BODY_GROUP_SQL)

        # Атомарная перезапись окна: старые строки окна (и мосты каскадом) удаляются, новые вставляются.
        cur.execute(f"DELETE FROM core.fact_prices WHERE {WINDOW_OR_BATCH}", params)
        facts_deleted = cur.rowcount
        cur.execute(f"DELETE FROM core.dq_quarantine_prices WHERE {WINDOW_OR_BATCH}", params)
        quarantine_deleted = cur.rowcount

        cur.execute("INSERT INTO core.fact_prices SELECT * FROM stg_fact")
        facts_inserted = cur.rowcount
        cur.execute("INSERT INTO core.dq_quarantine_prices SELECT * FROM stg_quarantine")
        quarantine_inserted = cur.rowcount
        cur.execute(INSERT_BODY_GROUPS_SQL)
        body_groups = cur.rowcount
        cur.execute(INSERT_PRICE_MONTHS_SQL)
        price_months = cur.rowcount
        cur.execute("SELECT count(*) FROM stg_fact f WHERE NOT EXISTS "
                    "(SELECT 1 FROM raw.price_dates pd WHERE pd.price_id = f.price_id)")
        without_price_dates = cur.fetchone()[0]
        dim_city = cur.execute("SELECT count(*) FROM core.dim_city").fetchone()[0]

    stats = {
        "core.fact_prices_deleted": facts_deleted,
        "core.fact_prices_inserted": facts_inserted,
        "core.dq_quarantine_deleted": quarantine_deleted,
        "core.dq_quarantine_inserted": quarantine_inserted,
        "core.fact_price_body_group": body_groups,
        "core.fact_price_month": price_months,
        "facts_without_price_dates": without_price_dates,
        "core.dim_city": dim_city,
    }
    log_event(logger, "load.done", window_start=window.start, window_end=window.end, **stats)
    return stats
