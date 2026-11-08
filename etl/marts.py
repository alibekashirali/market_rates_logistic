"""Пересчёт витрин mart.* (Вопрос 2). Логика витрины — в SQL-функции mart.refresh_route_card."""
from __future__ import annotations

import logging
from datetime import date

import psycopg

from etl.log import log_event

logger = logging.getLogger(__name__)


def refresh_route_card(dwh: psycopg.Connection, month_from: date, as_of: date) -> dict:
    with dwh.transaction(), dwh.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('mart_refresh'))")
        inserted = cur.execute("SELECT mart.refresh_route_card(%s, %s)", (month_from, as_of)).fetchone()[0]
        by_level = dict(cur.execute(
            "SELECT reliability_level, count(*) FROM mart.mart_route_card "
            "WHERE month >= date_trunc('month', %s::date) GROUP BY 1 ORDER BY 1", (month_from,)).fetchall())
        total = cur.execute("SELECT count(*) FROM mart.mart_route_card").fetchone()[0]

    stats = {"rows_refreshed": inserted, "rows_total": total, "refreshed_by_reliability": by_level}
    log_event(logger, "mart.route_card.done", month_from=month_from, as_of=as_of, **stats)
    return stats
