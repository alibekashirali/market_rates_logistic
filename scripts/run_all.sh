#!/usr/bin/env bash
# Сквозной прогон: миграции DWH -> ETL (raw/core + DQ) -> витрины -> ML обучение и batch-скоринг.
# Каждый шаг идемпотентен, повторный запуск безопасен.
set -euo pipefail

python -m etl.cli migrate
python -m etl.cli run ${ETL_START_DATE:+--start-date "$ETL_START_DATE"} ${ETL_END_DATE:+--end-date "$ETL_END_DATE"}
python -m ml.cli
