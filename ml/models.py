"""Модели: CatBoostRegressor (основная) и Ridge (линейный бейзлайн). Все seed зафиксированы."""
from __future__ import annotations

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from ml.features import CAT_FEATURES, NUM_FEATURES

CATBOOST_PARAMS = {
    "iterations": 1500,
    "learning_rate": 0.05,
    "depth": 8,
    "loss_function": "RMSE",
    "l2_leaf_reg": 3.0,
}


class CatBoostModel:
    name = "catboost_v1"

    def __init__(self, seed: int):
        self.params = {**CATBOOST_PARAMS, "random_seed": seed}
        self.model = CatBoostRegressor(**self.params, verbose=0, allow_writing_files=False,
                                       cat_features=list(CAT_FEATURES))

    def fit(self, X: pd.DataFrame, y: pd.Series) -> CatBoostModel:
        self.model.fit(X, y)
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return self.model.predict(X)

    def feature_importance(self) -> dict[str, float]:
        values = self.model.get_feature_importance()
        pairs = sorted(zip(self.model.feature_names_, values), key=lambda kv: -kv[1])
        return {k: round(float(v), 3) for k, v in pairs}


class RidgeModel:
    """One-hot + импутация + масштабирование; все трансформеры фитятся только на train внутри Pipeline."""
    name = "ridge_baseline"

    def __init__(self, seed: int):
        self.params = {"alpha": 1.0, "random_state": seed}
        preprocess = ColumnTransformer([
            ("cat", make_pipeline(SimpleImputer(strategy="most_frequent"),
                                  OneHotEncoder(handle_unknown="ignore", min_frequency=20)), list(CAT_FEATURES)),
            ("num", make_pipeline(SimpleImputer(strategy="median"), StandardScaler()), list(NUM_FEATURES)),
        ])
        self.model = Pipeline([("prep", preprocess), ("ridge", Ridge(**self.params))])

    def fit(self, X: pd.DataFrame, y: pd.Series) -> RidgeModel:
        self.model.fit(X, y)
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return self.model.predict(X)

    def feature_importance(self) -> dict[str, float] | None:
        return None


def regression_metrics(y_true: pd.Series, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "r2": float(r2_score(y_true, y_pred)),
        "mape_pct": float(mean_absolute_percentage_error(y_true, y_pred) * 100),
        "rmse_rub": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae_rub": float(mean_absolute_error(y_true, y_pred)),
    }
