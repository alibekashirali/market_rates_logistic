"""Обучение на хронологическом split и batch-скоринг test + будущих заявок в mart.ml_price_predictions.

    python -m ml.cli

Повторный запуск идемпотентен: прогнозы модели перезаписываются (DELETE + COPY в транзакции),
метрики — UPSERT по (model_name, as_of).
"""
from __future__ import annotations

import json
import logging
import random

import numpy as np
import pandas as pd
import psycopg

from etl.config import Settings
from etl.db import connect, copy_frame, read_frame
from etl.log import log_event, setup_logging
from ml.features import TARGET, TimeSplit, build_features
from ml.models import CatBoostModel, RidgeModel, regression_metrics

logger = logging.getLogger("ml")

# Популяция модели = исторические фактические прямые перевозки из core (DQ уже пройден, price_rub рассчитан).
# Географию берём из дедуплицированного dim_city (min cities.id на gc_id).
DATASET_SQL = """
SELECT f.price_id, f.loading_date, f.loading_city_gcid, f.unloading_city_gcid,
       oc.city_name AS origin_city, oc.region AS origin_region,
       oc.latitude  AS origin_lat,  oc.longitude AS origin_lon,
       dc.city_name AS destination_city, dc.region AS destination_region,
       dc.latitude  AS destination_lat,  dc.longitude AS destination_lon,
       f.distance, f.total_weight, f.total_volume,
       f.body_group, f.weight_category, f.volume_category,
       f.outlier_mark,
       f.price_rub::float8 AS price_rub
FROM core.fact_prices f
LEFT JOIN core.dim_city oc ON oc.gc_id = f.loading_city_gcid
LEFT JOIN core.dim_city dc ON dc.gc_id = f.unloading_city_gcid
WHERE f.is_fact AND f.price_type IN (1, 2, 3, 4, 5, 9)
  AND NOT f.complex
  AND f.distance > 0
  AND f.loading_date >= %(train_start)s
ORDER BY f.price_id
"""

PREDICTION_COLUMNS = (
    "price_id", "model_name", "split", "loading_date", "loading_city_gcid", "unloading_city_gcid",
    "body_group", "weight_category", "distance", "actual_price_rub", "predicted_price_rub",
    "abs_error_rub", "ape_pct", "outlier_mark", "as_of",
)


def _seed_everything(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)


def load_dataset(dwh: psycopg.Connection, split: TimeSplit) -> pd.DataFrame:
    df = read_frame(dwh, DATASET_SQL, {"train_start": split.train_start})
    df["split"] = split.assign(df["loading_date"])
    return df[df["split"].notna()].reset_index(drop=True)


def build_predictions(scoring: pd.DataFrame, y_pred: np.ndarray, model_name: str, as_of) -> pd.DataFrame:
    is_test = scoring["split"] == "test"
    actual = scoring[TARGET].where(is_test)  # для будущих заявок факт не раскрываем, даже если price заполнен
    predicted = pd.Series(np.round(y_pred, 2), index=scoring.index)
    abs_err = (actual - predicted).abs()
    ape = (abs_err / actual.where(actual != 0) * 100)
    return pd.DataFrame({
        "price_id": scoring["price_id"],
        "model_name": model_name,
        "split": scoring["split"],
        "loading_date": scoring["loading_date"],
        "loading_city_gcid": scoring["loading_city_gcid"],
        "unloading_city_gcid": scoring["unloading_city_gcid"],
        "body_group": scoring["body_group"],
        "weight_category": scoring["weight_category"],
        "distance": scoring["distance"],
        "actual_price_rub": actual.round(2),
        "predicted_price_rub": predicted,
        "abs_error_rub": abs_err.round(2),
        "ape_pct": ape.round(2),
        "outlier_mark": scoring["outlier_mark"],
        "as_of": as_of,
    })


def save_results(dwh: psycopg.Connection, predictions: pd.DataFrame, metrics_rows: list[dict]) -> None:
    model_names = sorted(predictions["model_name"].unique())
    with dwh.transaction(), dwh.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('ml_scoring'))")
        cur.execute("DELETE FROM mart.ml_price_predictions WHERE model_name = ANY(%s)", (model_names,))
        copy_frame(cur, "mart.ml_price_predictions", predictions, PREDICTION_COLUMNS)
        for m in metrics_rows:
            cur.execute(
                """
                INSERT INTO mart.ml_model_metrics (model_name, as_of, train_start, test_start, n_train, n_test,
                                                   r2, mape_pct, rmse_rub, mae_rub, params, feature_importance)
                VALUES (%(model_name)s, %(as_of)s, %(train_start)s, %(test_start)s, %(n_train)s, %(n_test)s,
                        %(r2)s, %(mape_pct)s, %(rmse_rub)s, %(mae_rub)s, %(params)s, %(feature_importance)s)
                ON CONFLICT (model_name, as_of) DO UPDATE SET
                    train_start = EXCLUDED.train_start, test_start = EXCLUDED.test_start,
                    n_train = EXCLUDED.n_train, n_test = EXCLUDED.n_test, r2 = EXCLUDED.r2,
                    mape_pct = EXCLUDED.mape_pct, rmse_rub = EXCLUDED.rmse_rub, mae_rub = EXCLUDED.mae_rub,
                    params = EXCLUDED.params, feature_importance = EXCLUDED.feature_importance,
                    trained_at = now()
                """,
                {**m, "params": json.dumps(m["params"]),
                 "feature_importance": json.dumps(m["feature_importance"]) if m["feature_importance"] else None},
            )


def run(settings: Settings) -> list[dict]:
    _seed_everything(settings.random_seed)
    split = TimeSplit.from_as_of(settings.as_of)
    with connect(settings.dwh_dsn) as dwh:
        dwh.autocommit = True
        df = load_dataset(dwh, split)
        counts = df.groupby(["split", "outlier_mark"]).size().rename("n").reset_index().to_dict("records")
        log_event(logger, "ml.dataset", as_of=settings.as_of, train_start=split.train_start,
                  test_start=split.test_start, rows=len(df), by_split=counts)

        # Таргет очищаем по предоставленному outlier_mark (сам флаг в признаки не идёт).
        labelled = df[df["split"].isin(["train", "test"]) & ~df["outlier_mark"]]
        train, test = labelled[labelled["split"] == "train"], labelled[labelled["split"] == "test"]
        scoring = df[df["split"].isin(["test", "future"])]
        X_train, X_test, X_score = build_features(train), build_features(test), build_features(scoring)

        predictions, metrics_rows = [], []
        for model in (CatBoostModel(settings.random_seed), RidgeModel(settings.random_seed)):
            model.fit(X_train, train[TARGET])
            metrics = regression_metrics(test[TARGET], model.predict(X_test))
            importance = model.feature_importance()
            metrics_rows.append({
                "model_name": model.name, "as_of": settings.as_of,
                "train_start": split.train_start, "test_start": split.test_start,
                "n_train": len(train), "n_test": len(test), **metrics,
                "params": model.params, "feature_importance": importance,
            })
            log_event(logger, "ml.model.evaluated", model=model.name, n_train=len(train), n_test=len(test),
                      **{k: round(v, 4) for k, v in metrics.items()},
                      top_features=dict(list(importance.items())[:8]) if importance else None)
            predictions.append(build_predictions(scoring, model.predict(X_score), model.name, settings.as_of))

        all_predictions = pd.concat(predictions, ignore_index=True)
        save_results(dwh, all_predictions, metrics_rows)
        log_event(logger, "ml.scoring.done",
                  rows=len(all_predictions),
                  by_split=all_predictions.groupby(["model_name", "split"]).size().rename("n")
                  .reset_index().to_dict("records"))
    return metrics_rows


def main() -> None:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    run(settings)


if __name__ == "__main__":
    main()
