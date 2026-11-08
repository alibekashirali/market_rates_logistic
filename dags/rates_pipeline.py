"""DAG «Smart Logistics: рыночные ставки» — ежемесячный ETL -> витрины -> ML.

Тонкая обёртка над CLI пайплайна (etl/, ml/): вся логика и идемпотентность живут там.
Задачи запускаются в отдельном venv (PIPELINE_PYTHON), чтобы зависимости пайплайна
(pandas, catboost, psycopg 3) не конфликтовали с зависимостями самого Airflow.

Окно одного запуска = data interval: loading_date ∈ [data_interval_start; data_interval_end).
catchup=True догружает историю помесячно; повторный запуск/clear любой задачи безопасен
(DELETE окна + INSERT в транзакции). Будущие плановые заявки и ML выполняются только
в последнем запуске (LatestOnly), чтобы backfill истории не переобучал модель 37 раз.

Дата снимка as_of берётся из AS_OF (2026-10-26): датасет — фиксированный снимок, поэтому
системная дата запуска для витрин и split не используется.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.latest_only import LatestOnlyOperator

PIPELINE_HOME = os.environ.get("PIPELINE_HOME", "/opt/airflow/pipeline")
PIPELINE_PYTHON = os.environ.get("PIPELINE_PYTHON", "/opt/airflow/pipeline-venv/bin/python")

WINDOW_START = "{{ data_interval_start | ds }}"
WINDOW_END = "{{ macros.ds_add(data_interval_end | ds, -1) }}"
# Горизонт плановых заявок после окна: в датасете они есть до ~110 дней после as_of.
FUTURE_START = "{{ data_interval_end | ds }}"
FUTURE_END = "{{ macros.ds_add(data_interval_end | ds, 183) }}"


def _cli(args: str) -> str:
    return f"{PIPELINE_PYTHON} -m {args}"


with DAG(
    dag_id="smart_logistics_rates",
    description="ETL сделок -> DQ -> core -> mart_route_card -> CatBoost batch scoring",
    doc_md=__doc__,
    schedule="@monthly",
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),  # начало истории в источнике
    catchup=True,
    max_active_runs=1,  # окна загружаются последовательно; хвост витрины пересчитывается по порядку
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "cwd": PIPELINE_HOME,
    },
    tags=["etl", "mart", "ml"],
) as dag:
    migrate = BashOperator(
        task_id="migrate_dwh",
        bash_command=_cli("etl.cli migrate"),
    )

    load_month = BashOperator(
        task_id="load_month_to_core",
        doc_md="Extract источник -> raw, DQ + FX + категории -> core, карантин. Окно = data interval.",
        bash_command=_cli(f"etl.cli run --start-date {WINDOW_START} --end-date {WINDOW_END} --skip-marts"),
    )

    refresh_marts = BashOperator(
        task_id="refresh_route_card",
        doc_md="Пересчёт mart.mart_route_card с месяца окна (включая MoM/YoY следующих месяцев).",
        bash_command=_cli(f"etl.cli marts --month-from {WINDOW_START}"),
    )

    latest_only = LatestOnlyOperator(task_id="latest_only")

    load_future = BashOperator(
        task_id="load_future_plans",
        doc_md="Плановые заявки с loading_date после окна — нужны для batch-скоринга будущего.",
        bash_command=_cli(f"etl.cli run --start-date {FUTURE_START} --end-date {FUTURE_END} --skip-marts"),
    )

    train_and_score = BashOperator(
        task_id="train_and_score",
        doc_md="Time split от AS_OF, CatBoost + Ridge, метрики и прогнозы в mart.ml_*.",
        bash_command=_cli("ml.cli"),
    )

    migrate >> load_month >> [refresh_marts, latest_only]
    latest_only >> load_future
    [refresh_marts, load_future] >> train_and_score
