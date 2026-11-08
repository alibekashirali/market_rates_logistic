"""Конфигурация из переменных окружения (см. .env.example). Паролей и путей в коде нет."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from urllib.parse import quote


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        if default is None:
            raise RuntimeError(f"Environment variable {name} is required")
        return default
    return value


def _optional_date(name: str) -> date | None:
    value = os.getenv(name)
    return date.fromisoformat(value) if value else None


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    source_db: str
    dwh_db: str
    as_of: date
    etl_start_date: date | None
    etl_end_date: date | None
    random_seed: int
    log_level: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            pg_host=_env("POSTGRES_HOST", "localhost"),
            pg_port=int(_env("POSTGRES_PORT", "5432")),
            pg_user=_env("POSTGRES_USER"),
            pg_password=_env("POSTGRES_PASSWORD"),
            source_db=_env("SOURCE_DB", "deals_bigdata"),
            dwh_db=_env("DWH_DB", "dwh"),
            as_of=date.fromisoformat(_env("AS_OF", "2026-10-26")),
            etl_start_date=_optional_date("ETL_START_DATE"),
            etl_end_date=_optional_date("ETL_END_DATE"),
            random_seed=int(_env("RANDOM_SEED", "42")),
            log_level=_env("LOG_LEVEL", "INFO"),
        )

    def _dsn(self, dbname: str) -> str:
        return (
            f"postgresql://{quote(self.pg_user)}:{quote(self.pg_password)}"
            f"@{self.pg_host}:{self.pg_port}/{dbname}"
        )

    @property
    def source_dsn(self) -> str:
        return self._dsn(self.source_db)

    @property
    def dwh_dsn(self) -> str:
        return self._dsn(self.dwh_db)
