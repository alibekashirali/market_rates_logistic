from datetime import date

import numpy as np
import pandas as pd
import pytest

from ml.features import (
    FEATURES, FORBIDDEN_FEATURES, TimeSplit, add_calendar_features, assert_no_leakage, build_features,
)

AS_OF = date(2026, 10, 26)


def test_split_bounds_match_spec():
    split = TimeSplit.from_as_of(AS_OF)
    assert split.train_start == date(2026, 7, 26)
    assert split.test_start == date(2026, 10, 5)


@pytest.mark.parametrize("day, expected", [
    (date(2026, 7, 25), None),
    (date(2026, 7, 26), "train"),
    (date(2026, 10, 4), "train"),
    (date(2026, 10, 5), "test"),   # граница входит только в test
    (date(2026, 10, 26), "test"),
    (date(2026, 10, 27), "future"),
])
def test_split_assignment(day, expected):
    assert TimeSplit.from_as_of(AS_OF).assign(pd.Series([day])).iloc[0] == expected


def test_feature_set_has_no_leakage():
    assert_no_leakage(FEATURES)
    with pytest.raises(ValueError):
        assert_no_leakage(list(FEATURES) + ["price_with_vat"])
    assert "outlier_mark" in FORBIDDEN_FEATURES and "avg_price" in FORBIDDEN_FEATURES


def test_cyclical_encoding_is_continuous_across_year_end():
    df = add_calendar_features(pd.DataFrame({"loading_date": [date(2025, 12, 31), date(2026, 1, 1)]}))
    dec, jan = df.iloc[0], df.iloc[1]
    # 31.12 и 01.01 соседи на окружности дня года, хотя day_of_year отличается на 364
    assert np.hypot(dec.doy_sin - jan.doy_sin, dec.doy_cos - jan.doy_cos) < 0.05
    assert abs(dec.month_sin - np.sin(2 * np.pi * 12 / 12)) < 1e-9
    assert np.allclose(df[["month_sin", "month_cos"]].pow(2).sum(axis=1), 1.0)


def test_day_of_week_is_iso():
    df = add_calendar_features(pd.DataFrame({"loading_date": [date(2026, 10, 26), date(2026, 10, 25)]}))
    assert df["day_of_week"].tolist() == [1, 7]  # понедельник, воскресенье


def test_build_features_fills_missing_categories_and_drops_target():
    raw = pd.DataFrame([{
        "loading_date": date(2026, 9, 1), "origin_region": None, "destination_region": "Республика Татарстан",
        "origin_city": "Москва", "destination_city": "Казань", "body_group": "Тент", "weight_category": "FTL",
        "volume_category": "FTL", "distance": 897.0, "total_weight": 20.0, "total_volume": 82.0,
        "origin_lat": 55.75, "origin_lon": 37.61, "destination_lat": 55.79, "destination_lon": 49.10,
        "price_rub": 84500.0, "outlier_mark": False,
    }])
    X = build_features(raw)
    assert list(X.columns) == list(FEATURES)
    assert X.loc[0, "origin_region"] == "UNKNOWN"
    assert "price_rub" not in X.columns and "outlier_mark" not in X.columns
