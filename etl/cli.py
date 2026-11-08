"""CLI пайплайна.

    python -m etl.cli migrate
    python -m etl.cli run [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--skip-marts]
    python -m etl.cli marts --month-from YYYY-MM-DD

Окно задаётся по loading_date (включительно). Без параметров берутся ETL_START_DATE/ETL_END_DATE,
а если и они пусты — полный диапазон источника. Повторный запуск за то же окно идемпотентен.
"""
from __future__ import annotations

import argparse
import json
import logging
import uuid
from datetime import date, datetime, timezone

import psycopg

from etl.config import Settings
from etl.db import connect
from etl.extract import Window, extract_to_raw, source_date_range
from etl.load import load_core, transform
from etl.log import log_event, setup_logging
from etl.marts import refresh_route_card
from etl.migrate import apply_migrations

logger = logging.getLogger("etl")


def _connect(dsn: str) -> psycopg.Connection:
    conn = connect(dsn)
    conn.autocommit = True  # транзакции открываются явно через conn.transaction()
    return conn


def resolve_window(settings: Settings, src: psycopg.Connection,
                   start: date | None, end: date | None) -> Window:
    start = start or settings.etl_start_date
    end = end or settings.etl_end_date
    if start is None or end is None:
        full = source_date_range(src)
        start, end = start or full.start, end or full.end
    return Window(start, end)


def _record_run(dwh: psycopg.Connection, run_id: uuid.UUID, step: str, window: Window | None,
                status: str, stats: dict, started_at: datetime) -> None:
    dwh.execute(
        """
        INSERT INTO core.etl_runs (run_id, step, window_start, window_end, status, stats, started_at, finished_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (run_id) DO UPDATE
        SET status = EXCLUDED.status, stats = EXCLUDED.stats, finished_at = EXCLUDED.finished_at
        """,
        (run_id, step, window.start if window else None, window.end if window else None,
         status, json.dumps(stats, default=str), started_at),
    )


def cmd_migrate(settings: Settings) -> None:
    with _connect(settings.dwh_dsn) as dwh:
        applied = apply_migrations(dwh)
    log_event(logger, "migrate.done", files=applied)


def cmd_run(settings: Settings, start: date | None, end: date | None, skip_marts: bool) -> dict:
    run_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)
    with _connect(settings.source_dsn) as src, _connect(settings.dwh_dsn) as dwh:
        window = resolve_window(settings, src, start, end)
        log_event(logger, "run.start", run_id=run_id, window_start=window.start, window_end=window.end,
                  as_of=settings.as_of)
        stats: dict = {}
        try:
            stats["extract"] = extract_to_raw(src, dwh, window)
            result = transform(dwh, window)
            stats["dq"] = result.stats
            stats["load"] = load_core(dwh, window, result, run_id)
            if not skip_marts:
                stats["mart"] = refresh_route_card(dwh, window.start, settings.as_of)
        except Exception:
            _record_run(dwh, run_id, "etl", window, "failed", stats, started_at)
            logger.exception("run.failed", extra={"run_id": str(run_id)})
            raise
        _record_run(dwh, run_id, "etl", window, "success", stats, started_at)
    log_event(logger, "run.done", run_id=run_id)
    return stats


def cmd_marts(settings: Settings, month_from: date) -> None:
    with _connect(settings.dwh_dsn) as dwh:
        refresh_route_card(dwh, month_from, settings.as_of)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m etl.cli", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate", help="применить DDL sql/dwh/*.sql")
    run = sub.add_parser("run", help="extract -> DQ -> load core -> refresh marts за окно loading_date")
    run.add_argument("--start-date", type=date.fromisoformat)
    run.add_argument("--end-date", type=date.fromisoformat)
    run.add_argument("--skip-marts", action="store_true")
    marts = sub.add_parser("marts", help="пересчитать витрины начиная с месяца")
    marts.add_argument("--month-from", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    setup_logging(settings.log_level)
    if args.command == "migrate":
        cmd_migrate(settings)
    elif args.command == "run":
        cmd_run(settings, args.start_date, args.end_date, args.skip_marts)
    else:
        cmd_marts(settings, args.month_from)


if __name__ == "__main__":
    main()
