"""Feature pipeline: гео + груз/транспорт + циклическое кодирование даты, защита от утечек и time split.

Все преобразования здесь построчные и детерминированные (без статистик по выборке), поэтому
их можно применять к train/test/future одинаково. Обучаемые трансформеры (импутеры, скейлеры,
энкодеры) живут внутри моделей (ml/models.py) и фитятся только на train.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

TARGET = "price_rub"

CAT_FEATURES: tuple[str, ...] = (
    "origin_region", "destination_region", "origin_city", "destination_city",
    "body_group", "weight_category", "volume_category",
)
NUM_FEATURES: tuple[str, ...] = (
    "distance", "total_weight", "total_volume",
    "origin_lat", "origin_lon", "destination_lat", "destination_lon",
    "year", "month", "day_of_week", "day_of_year",
    "month_sin", "month_cos", "dow_sin", "dow_cos", "doy_sin", "doy_cos",
)
FEATURES: tuple[str, ...] = CAT_FEATURES + NUM_FEATURES

# Прямые/косвенные производные таргета и постфактум-агрегаты — в признаки не попадают никогда.
FORBIDDEN_FEATURES = frozenset({
    "price", "price_rub", "price_with_vat", "fx_rate",
    "price_km", "price_per_km", "price_per_ton",
    "customer_cost", "transportation_cost", "profit", "carrier_cost",
    "price_group_id", "avg_price", "min_km_price", "max_km_price", "outliers_amount", "clear_items_amount",
    "outlier_mark", "price_id",
})

UNKNOWN = "UNKNOWN"


def assert_no_leakage(features: tuple[str, ...] | list[str]) -> None:
    leaked = sorted(set(features) & FORBIDDEN_FEATURES)
    if leaked:
        raise ValueError(f"Target leakage: forbidden features {leaked}")


def cyclical(values: pd.Series, period: float) -> tuple[pd.Series, pd.Series]:
    angle = 2 * np.pi * values.astype(float) / period
    return np.sin(angle), np.cos(angle)


def add_calendar_features(df: pd.DataFrame, date_col: str = "loading_date") -> pd.DataFrame:
    out = df.copy()
    d = pd.to_datetime(out[date_col])
    out["year"] = d.dt.year
    out["month"] = d.dt.month
    out["day_of_week"] = d.dt.dayofweek + 1  # 1 = понедельник ... 7 = воскресенье
    out["day_of_year"] = d.dt.dayofyear
    out["month_sin"], out["month_cos"] = cyclical(out["month"], 12)
    out["dow_sin"], out["dow_cos"] = cyclical(out["day_of_week"], 7)
    out["doy_sin"], out["doy_cos"] = cyclical(out["day_of_year"], 365.25)
    return out


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    assert_no_leakage(FEATURES)
    out = add_calendar_features(df)
    for col in CAT_FEATURES:
        out[col] = out[col].fillna(UNKNOWN).astype(str)
    for col in NUM_FEATURES:
        out[col] = out[col].astype(float)
    return out.loc[:, list(FEATURES)]


@dataclass(frozen=True)
class TimeSplit:
    """train = [as_of - 3 мес; as_of - 3 нед), test = [as_of - 3 нед; as_of], future = (as_of; ...)."""
    as_of: date
    train_start: date
    test_start: date

    @classmethod
    def from_as_of(cls, as_of: date) -> TimeSplit:
        ts = pd.Timestamp(as_of)
        return cls(
            as_of=as_of,
            train_start=(ts - pd.DateOffset(months=3)).date(),
            test_start=(ts - pd.Timedelta(weeks=3)).date(),
        )

    def assign(self, loading_date: pd.Series) -> pd.Series:
        d = pd.to_datetime(loading_date)
        train_start, test_start, as_of = map(pd.Timestamp, (self.train_start, self.test_start, self.as_of))
        labels = np.select(
            [(d >= train_start) & (d < test_start), (d >= test_start) & (d <= as_of), d > as_of],
            ["train", "test", "future"],
            default="",
        )
        return pd.Series(labels, index=loading_date.index).replace("", None)
