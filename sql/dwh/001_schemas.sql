-- Слои хранилища: raw (сырой слепок источника) -> core (очищенные факты) -> mart (витрины для BI и ML).
-- Все миграции идемпотентны и применяются при каждом запуске (python -m etl.cli migrate).
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
