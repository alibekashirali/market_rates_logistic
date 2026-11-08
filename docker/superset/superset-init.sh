#!/usr/bin/env bash
# Инициализация Superset: схема метаданных, админ, роли, подключение к DWH, датасеты и дашборд.
# Идемпотентно: повторный запуск пересоздаёт дашборд из кода и не плодит дубли.
set -euo pipefail

superset db upgrade
if ! superset fab list-users 2>/dev/null | grep -qw "username:${SUPERSET_ADMIN_USER}"; then
  superset fab create-admin --username "$SUPERSET_ADMIN_USER" --password "$SUPERSET_ADMIN_PASSWORD" \
    --firstname Admin --lastname Superset --email admin@example.com
fi
superset init
python /app/docker/bootstrap_dashboard.py
