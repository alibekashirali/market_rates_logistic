#!/usr/bin/env bash
# Инициализация при первом старте контейнера (пустой volume):
#   1) создаём БД источника и БД хранилища;
#   2) разворачиваем эталонную схему источника и детерминированные синтетические данные.
set -euo pipefail

: "${SOURCE_DB:?SOURCE_DB is required}"
: "${DWH_DB:?DWH_DB is required}"
N_PRICES="${N_PRICES:-300000}"

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
     -c "CREATE DATABASE \"${SOURCE_DB}\"" \
     -c "CREATE DATABASE \"${DWH_DB}\""

echo "[init] loading source schema into ${SOURCE_DB}"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$SOURCE_DB" -f /sql/schema.sql

echo "[init] generating ${N_PRICES} synthetic prices (deterministic seed)"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$SOURCE_DB" -v n_prices="$N_PRICES" -f /sql/synthetic.sql

echo "[init] source ready"
