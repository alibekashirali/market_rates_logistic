from datetime import date, datetime

import pandas as pd
import pytest

from etl.validate import derive_volume_category, derive_weight_category, validate_prices

RATE_DAY = date(2026, 9, 12)
CITIES = {1001, 1003, 1004}
RATES = pd.DataFrame({
    "num_code": ["643", "840", "978"],
    "setting_date": [RATE_DAY, RATE_DAY, RATE_DAY],
    "value": [1.0, 90.0, 100.0],
})


def _row(**overrides):
    base = {
        "id": 1, "created_at": datetime(2026, 9, 12, 10, 0), "subscriber_id": 101, "organization_id": 100001,
        "loading_date": date(2026, 9, 15), "end_loading_date": date(2026, 9, 15),
        "loading_city_gcid": 1001, "unloading_city_gcid": 1003, "distance": 897.0,
        "total_weight": 20.0, "total_volume": 82.0, "body_type": 56, "price": 84500.0,
        "currency_code": "643", "complex": False, "price_type": 3, "is_fact": True,
        "nearest_rate_date": RATE_DAY, "outlier_mark": False, "weight_category": "FTL", "volume_category": "FTL",
    }
    return {**base, **overrides}


def _validate(*rows):
    return validate_prices(pd.DataFrame([_row(id=i + 1, **r) for i, r in enumerate(rows)]), CITIES, RATES)


def _reason(result, price_id):
    return result.quarantine.set_index("price_id").loc[price_id, "reason_code"]


@pytest.mark.parametrize("overrides, reason", [
    ({"price": 0.0}, "PRICE_NON_POSITIVE"),
    ({"price": -5.0}, "PRICE_NON_POSITIVE"),
    ({"price": 1000.0}, "RUB_PRICE_BELOW_MIN"),
    ({"distance": 0.0}, "DISTANCE_NON_POSITIVE"),
    ({"total_weight": -1.0}, "WEIGHT_NEGATIVE"),
    ({"total_volume": -0.1}, "VOLUME_NEGATIVE"),
    ({"loading_city_gcid": 9999}, "LOADING_CITY_UNKNOWN"),
    ({"unloading_city_gcid": 9999}, "UNLOADING_CITY_UNKNOWN"),
    ({"currency_code": "156", "price": 5000.0}, "CURRENCY_UNSUPPORTED"),
    ({"currency_code": "840", "price": 900.0, "nearest_rate_date": None}, "FX_RATE_DATE_MISSING"),
    ({"currency_code": "978", "price": 900.0, "nearest_rate_date": date(2026, 9, 13)}, "FX_RATE_NOT_FOUND"),
])
def test_invalid_rows_go_to_quarantine_with_reason(overrides, reason):
    result = _validate(overrides)
    assert result.clean.empty
    assert _reason(result, 1) == reason


def test_valid_rows_and_fx_conversion():
    result = _validate({}, {"currency_code": "840", "price": 900.0}, {"currency_code": "978", "price": 800.0})
    assert result.quarantine.empty
    rub = result.clean.set_index("price_id")["price_rub"]
    assert rub.to_dict() == {1: 84500.0, 2: 81000.0, 3: 80000.0}


def test_foreign_price_below_1000_is_not_rub_rule():
    # Порог 1000 — source-validation только для RUB; 900 USD — нормальная ставка.
    result = _validate({"currency_code": "840", "price": 900.0})
    assert len(result.clean) == 1


def test_rub_without_rate_date_is_valid():
    result = _validate({"nearest_rate_date": None})
    assert len(result.clean) == 1 and result.clean["fx_rate"].iloc[0] == 1.0


def test_same_city_pair_is_not_rejected():
    result = _validate({"unloading_city_gcid": 1001})
    assert len(result.clean) == 1


def test_outlier_and_complex_are_not_dq_failures():
    result = _validate({"outlier_mark": True}, {"complex": True}, {"price_type": 6, "is_fact": False})
    assert len(result.clean) == 3


def test_all_reasons_are_recorded_and_primary_is_first():
    result = _validate({"price": 0.0, "distance": 0.0, "loading_city_gcid": 9999})
    row = result.quarantine.iloc[0]
    assert row["reason_code"] == "PRICE_NON_POSITIVE"
    assert row["reason_codes"] == "{PRICE_NON_POSITIVE,RUB_PRICE_BELOW_MIN,DISTANCE_NON_POSITIVE,LOADING_CITY_UNKNOWN}"
    assert '"id":1' in row["payload"]


def test_stats_are_consistent():
    result = _validate({}, {"price": 0.0}, {"distance": -1.0})
    s = result.stats
    assert s["rows_read"] == s["rows_clean"] + s["rows_quarantined"] == 3
    assert s["quarantine_by_reason"] == {"DISTANCE_NON_POSITIVE": 1, "PRICE_NON_POSITIVE": 1}


@pytest.mark.parametrize("weight, expected", [
    (38.0, "G20T"), (30.0, "G20T"), (29.96, "G20T"), (29.94, "FTL"), (10.0, "FTL"), (9.95, "FTL"),
    (9.9, "10T"), (5.0, "10T"), (4.9, "5T"), (2.5, "5T"), (2.45, "5T"), (2.4, "L5T"),
    (0.5, "L5T"), (0.45, "L5T"), (0.44, "OTH"), (0.0, "OTH"),
])
def test_weight_category_boundaries(weight, expected):
    assert derive_weight_category(pd.Series([weight])).iloc[0] == expected


@pytest.mark.parametrize("volume, expected", [(0.0, "OTH"), (0.1, "FTL"), (120.0, "FTL"), (120.01, "OTH")])
def test_volume_category_boundaries(volume, expected):
    assert derive_volume_category(pd.Series([volume])).iloc[0] == expected


def test_missing_categories_are_filled_existing_kept():
    result = _validate({"weight_category": None, "volume_category": None, "total_weight": 4.0, "total_volume": 130.0},
                       {"weight_category": "FTL"})
    clean = result.clean.set_index("price_id")
    assert clean.loc[1, "weight_category"] == "5T" and clean.loc[1, "volume_category"] == "OTH"
    assert bool(clean.loc[1, "weight_category_filled"]) and not bool(clean.loc[2, "weight_category_filled"])
