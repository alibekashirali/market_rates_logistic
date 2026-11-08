"""Применение идемпотентных миграций DWH из sql/dwh/*.sql в лексикографическом порядке."""
from __future__ import annotations

import logging
from pathlib import Path

import psycopg

from etl.log import log_event

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "sql" / "dwh"


def apply_migrations(dwh: psycopg.Connection, directory: Path = MIGRATIONS_DIR) -> list[str]:
    files = sorted(directory.glob("*.sql"))
    with dwh.transaction(), dwh.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('dwh_migrations'))")
        for path in files:
            cur.execute(path.read_text(encoding="utf-8"))
            log_event(logger, "migration.applied", file=path.name)
    return [p.name for p in files]
