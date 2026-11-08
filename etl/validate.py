"""Data Quality, конвертация валют и заполнение категорий (Вопрос 1). Чистые функции над DataFrame.

Правила отбраковки (код причины -> условие). Порядок задаёт приоритет основной причины `reason_code`,
полный список сработавших правил сохраняется в `reason_codes`:

    PRICE_NON_POSITIVE      price <= 0
    RUB_PRICE_BELOW_MIN     currency_code = 643 и price <= 1000 (source-validation диапазон)
    DISTANCE_NON_POSITIVE   distance <= 0
    WEIGHT_NEGATIVE         total_weight < 0
    VOLUME_NEGATIVE         total_volume < 0
    LOADING_CITY_UNKNOWN    loading_city_gcid отсутствует в cities
    UNLOADING_CITY_UNKNOWN  unloading_city_gcid отсутствует в cities
    CURRENCY_UNSUPPORTED    код валюты не 643/840/978
    FX_RATE_DATE_MISSING    валюта 840/978 и nearest_rate_date IS NULL
    FX_RATE_NOT_FOUND       валюта 840/978 и нет курса ЦБ ровно на nearest_rate_date (fallback по loading_date не делаем)

Совпадение города погрузки и выгрузки браком не считается (production-job сохраняет такие пары).
outlier_mark, complex, is_fact — не DQ-правила, а фильтры витрины/ML; строки сохраняются в core.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

RUB, USD, EUR = "643", "840", "978"
SUPPORTED_CURRENCIES = frozenset({RUB, USD, EUR})
MIN_RUB_PRICE = 1000.0

REASON_CODES: tuple[str, ...] = (
    "PRICE_NON_POSITIVE",
    "RUB_PRICE_BELOW_MIN",
    "DISTANCE_NON_POSITIVE",
    "WEIGHT_NEGATIVE",
    "VOLUME_NEGATIVE",
    "LOADING_CITY_UNKNOWN",
    "UNLOADING_CITY_UNKNOWN",
    "CURRENCY_UNSUPPORTED",
    "FX_RATE_DATE_MISSING",
    "FX_RATE_NOT_FOUND",
)

# Границы весовых категорий (т, после округления до 0.1): нижняя граница включительно.
WEIGHT_CATEGORIES: tuple[tuple[float, str], ...] = (
    (30.0, "G20T"),
    (10.0, "FTL"),
    (5.0, "10T"),
    (2.5, "5T"),
    (0.5, "L5T"),
)
MAX_FTL_VOLUME = 120.0


@dataclass
class ValidationResult:
    clean: pd.DataFrame
    quarantine: pd.DataFrame
    stats: dict = field(default_factory=dict)


def round_half_up(values: pd.Series, digits: int = 1) -> pd.Series:
    """Округление как у PostgreSQL round(numeric): половина — от нуля (а не банковское)."""
    factor = 10.0**digits
    return np.sign(values) * np.floor(np.abs(values) * factor + 0.5 + 1e-9) / factor


def derive_weight_category(total_weight: pd.Series) -> pd.Series:
    w = round_half_up(total_weight.astype(float), 1)
    conditions = [w >= bound for bound, _ in WEIGHT_CATEGORIES]
    labels = [label for _, label in WEIGHT_CATEGORIES]
    return pd.Series(np.select(conditions, labels, default="OTH"), index=total_weight.index)


def derive_volume_category(total_volume: pd.Series) -> pd.Series:
    v = total_volume.astype(float)
    return pd.Series(np.where((v > 0) & (v <= MAX_FTL_VOLUME), "FTL", "OTH"), index=total_volume.index)


def attach_fx_rate(prices: pd.DataFrame, rates: pd.DataFrame) -> pd.Series:
    """Курс к RUB: 1.0 для 643, иначе точный курс ЦБ на nearest_rate_date (NaN, если его нет)."""
    rates = rates.drop_duplicates(["num_code", "setting_date"], keep="last")
    keys = pd.MultiIndex.from_frame(rates[["num_code", "setting_date"]])
    lookup = pd.Series(rates["value"].to_numpy(dtype=float), index=keys)
    wanted = pd.MultiIndex.from_arrays([prices["currency_code"], prices["nearest_rate_date"]])
    fx = pd.Series(lookup.reindex(wanted).to_numpy(), index=prices.index, dtype=float)
    return fx.where(prices["currency_code"] != RUB, 1.0)


def _rule_masks(df: pd.DataFrame, city_gcids: set[int], fx_rate: pd.Series) -> dict[str, pd.Series]:
    is_rub = df["currency_code"] == RUB
    is_foreign = df["currency_code"].isin(SUPPORTED_CURRENCIES) & ~is_rub
    has_rate_date = df["nearest_rate_date"].notna()
    return {
        "PRICE_NON_POSITIVE": df["price"] <= 0,
        "RUB_PRICE_BELOW_MIN": is_rub & (df["price"] <= MIN_RUB_PRICE),
        "DISTANCE_NON_POSITIVE": df["distance"] <= 0,
        "WEIGHT_NEGATIVE": df["total_weight"] < 0,
        "VOLUME_NEGATIVE": df["total_volume"] < 0,
        "LOADING_CITY_UNKNOWN": ~df["loading_city_gcid"].isin(city_gcids),
        "UNLOADING_CITY_UNKNOWN": ~df["unloading_city_gcid"].isin(city_gcids),
        "CURRENCY_UNSUPPORTED": ~df["currency_code"].isin(SUPPORTED_CURRENCIES),
        "FX_RATE_DATE_MISSING": is_foreign & ~has_rate_date,
        "FX_RATE_NOT_FOUND": is_foreign & has_rate_date & fx_rate.isna(),
    }


def _json_payload(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], index=df.index, dtype=object)
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].map(lambda v: v.isoformat() if hasattr(v, "isoformat") else v)
        elif pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
    lines = out.to_json(orient="records", lines=True, force_ascii=False).splitlines()
    return pd.Series(lines, index=df.index)


def validate_prices(prices: pd.DataFrame, city_gcids: set[int], rates: pd.DataFrame) -> ValidationResult:
    """Разделяет сделки на очищенные (с price_rub и категориями) и карантин с кодами причин."""
    df = prices.copy()
    df["currency_code"] = df["currency_code"].astype(str).str.strip()
    fx_rate = attach_fx_rate(df, rates)
    masks = _rule_masks(df, city_gcids, fx_rate)

    failed = np.zeros(len(df), dtype=bool)
    codes = pd.Series("", index=df.index)
    for code in REASON_CODES:
        m = masks[code].to_numpy()
        failed |= m
        codes = codes + np.where(m, code + ",", "")
    failed = pd.Series(failed, index=df.index)

    # --- карантин ---
    q = df.loc[failed]
    quarantine = pd.DataFrame(
        {
            "price_id": q["id"],
            "loading_date": q["loading_date"],
            "reason_code": np.select(
                [masks[c][failed].to_numpy() for c in REASON_CODES], REASON_CODES, default="UNKNOWN"
            ),
            "reason_codes": "{" + codes[failed].str.rstrip(",") + "}",
            "payload": _json_payload(q),
        }
    )

    # --- очищенные факты ---
    clean = df.loc[~failed].copy()
    clean["fx_rate"] = fx_rate[~failed]
    clean["price_rub"] = (clean["price"] * clean["fx_rate"]).round(2)
    wc_missing = clean["weight_category"].isna() | (clean["weight_category"].astype(str).str.strip() == "")
    vc_missing = clean["volume_category"].isna() | (clean["volume_category"].astype(str).str.strip() == "")
    clean["weight_category"] = clean["weight_category"].where(~wc_missing, derive_weight_category(clean["total_weight"]))
    clean["volume_category"] = clean["volume_category"].where(~vc_missing, derive_volume_category(clean["total_volume"]))
    clean["weight_category_filled"] = wc_missing
    clean["volume_category_filled"] = vc_missing
    clean = clean.rename(columns={"id": "price_id"})

    stats = {
        "rows_read": int(len(df)),
        "rows_clean": int(len(clean)),
        "rows_quarantined": int(len(quarantine)),
        "quarantine_by_reason": {k: int(v) for k, v in quarantine["reason_code"].value_counts().sort_index().items()},
        "rule_hits": {code: int(masks[code].sum()) for code in REASON_CODES if masks[code].any()},
        "weight_category_filled": int(wc_missing.sum()),
        "volume_category_filled": int(vc_missing.sum()),
        "fx_converted_rows": int((clean["currency_code"] != RUB).sum()),
    }
    return ValidationResult(clean=clean, quarantine=quarantine, stats=stats)
