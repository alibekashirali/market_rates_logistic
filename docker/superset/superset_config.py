"""Конфигурация Superset для локального демо. Секреты — только из окружения."""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Метаданные Superset (дашборды, чарты, пользователи) — SQLite в volume: для локального демо достаточно.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True}

# Локальный http без TLS.
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True

BABEL_DEFAULT_LOCALE = "ru"
LANGUAGES = {"ru": {"flag": "ru", "name": "Русский"}, "en": {"flag": "us", "name": "English"}}
