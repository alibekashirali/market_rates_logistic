#!/usr/bin/env bash
# Обёртка над штатным /entrypoint образа Airflow:
#   1) собирает строку подключения к метабазе из POSTGRES_* (без дублирования пароля в конфиге);
#   2) создаёт БД метаданных AIRFLOW_DB, если её нет (на существующем volume initdb не перезапускается).
set -euo pipefail

: "${POSTGRES_HOST:?}" "${POSTGRES_PORT:?}" "${POSTGRES_USER:?}" "${POSTGRES_PASSWORD:?}"
export AIRFLOW_DB="${AIRFLOW_DB:-airflow}"

quote() { python -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"; }
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$(quote "$POSTGRES_USER"):$(quote "$POSTGRES_PASSWORD")@${POSTGRES_HOST}:${POSTGRES_PORT}/${AIRFLOW_DB}"

python - <<'PY'
import os
import psycopg2
from psycopg2 import errors, sql

conn = psycopg2.connect(host=os.environ["POSTGRES_HOST"], port=os.environ["POSTGRES_PORT"],
                        user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
                        dbname=os.environ.get("POSTGRES_DB", "postgres"))
conn.autocommit = True
db = os.environ["AIRFLOW_DB"]
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
    if cur.fetchone() is None:
        try:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db)))
            print(f"[airflow-entrypoint] created database {db}")
        except errors.DuplicateDatabase:
            pass
conn.close()
PY

exec /entrypoint "$@"
