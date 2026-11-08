"""Интеграционные проверки на поднятом DWH (после docker compose up): docker compose run --rm tests."""
from datetime import date

import psycopg
import pytest

from etl.cli import cmd_run
from etl.config import Settings

pytestmark = pytest.mark.integration

WINDOW = (date(2026, 9, 1), date(2026, 9, 30))


@pytest.fixture(scope="module")
def settings():
    try:
        s = Settings.from_env()
        with psycopg.connect(s.dwh_dsn, connect_timeout=3) as conn:
            loaded = conn.execute("SELECT to_regclass('core.fact_prices') IS NOT NULL "
                                  "AND EXISTS (SELECT 1 FROM core.fact_prices)").fetchone()[0]
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"DWH недоступен: {exc}")
    if not loaded:
        pytest.skip("DWH пуст — сначала выполните пайплайн (docker compose up)")
    return s


@pytest.fixture
def dwh(settings):
    with psycopg.connect(settings.dwh_dsn, autocommit=True) as conn:
        yield conn


SNAPSHOT_SQL = {
    "facts": "SELECT count(*), sum(price_rub) FROM core.fact_prices",
    "quarantine": "SELECT count(*), count(DISTINCT price_id) FROM core.dq_quarantine_prices",
    "body_groups": "SELECT count(*) FROM core.fact_price_body_group",
    "months": "SELECT count(*) FROM core.fact_price_month",
    "mart": "SELECT count(*), sum(trips_count), sum(median_trip), sum(p75_km), sum(mom_pct), sum(yoy_pct) "
            "FROM mart.mart_route_card",
}


def _snapshot(conn):
    return {k: conn.execute(q).fetchone() for k, q in SNAPSHOT_SQL.items()}


def test_rerun_window_is_idempotent(settings, dwh):
    before = _snapshot(dwh)
    cmd_run(settings, *WINDOW, skip_marts=False)
    cmd_run(settings, *WINDOW, skip_marts=False)
    assert _snapshot(dwh) == before


def test_no_duplicates_and_disjoint_core_and_quarantine(dwh):
    assert dwh.execute("SELECT count(*) - count(DISTINCT gc_id) FROM core.dim_city").fetchone()[0] == 0
    assert dwh.execute("SELECT count(*) FROM core.fact_prices f JOIN core.dq_quarantine_prices q "
                       "USING (price_id)").fetchone()[0] == 0
    # всё, что прочитано в raw, оказалось либо в core, либо в карантине
    assert dwh.execute("""
        SELECT count(*) FROM raw.prices r
        WHERE NOT EXISTS (SELECT 1 FROM core.fact_prices f WHERE f.price_id = r.id)
          AND NOT EXISTS (SELECT 1 FROM core.dq_quarantine_prices q WHERE q.price_id = r.id)""").fetchone()[0] == 0


def test_dq_rules_hold_in_core(dwh):
    bad = dwh.execute("""
        SELECT count(*) FROM core.fact_prices
        WHERE price <= 0 OR distance <= 0 OR total_weight < 0 OR total_volume < 0
           OR (currency_code = '643' AND price <= 1000)
           OR (currency_code <> '643' AND nearest_rate_date IS NULL)
           OR weight_category IS NULL OR volume_category IS NULL""").fetchone()[0]
    assert bad == 0


def test_body_groups_follow_mapping(dwh):
    rows = dict(dwh.execute("""
        SELECT body_type_id, string_agg(DISTINCT core.body_group_of(body_type_id), ',')
        FROM raw.price_body_types GROUP BY 1""").fetchall())
    assert rows.get(57) == "Тент" and rows.get(46) == "Рефрижератор" and rows.get(1) == "Борт"
    assert dwh.execute("SELECT count(*) FROM core.fact_price_body_group WHERE body_group = 'Не указан'"
                       ).fetchone()[0] > 0


def test_mart_rules(settings, dwh):
    assert dwh.execute("SELECT count(*) FROM mart.mart_route_card WHERE last_trip_date > %s",
                       (settings.as_of,)).fetchone()[0] == 0
    assert dwh.execute("""
        SELECT count(*) FROM mart.mart_route_card
        WHERE (reliability_level = 'HIGH' AND NOT (trips_count >= 30 AND carriers_count >= 5 AND freshness_days <= 30))
           OR (reliability_level = 'MEDIUM' AND trips_count < 10)
           OR (trips_count < 2 AND cv_rate IS NOT NULL)
           OR NOT (p25_trip <= median_trip AND median_trip <= p75_trip)""").fetchone()[0] == 0
    # MoM считается только при наличии календарно предыдущего месяца
    assert dwh.execute("""
        SELECT count(*) FROM mart.mart_route_card c
        WHERE c.mom_pct IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM mart.mart_route_card p
            WHERE p.loading_city_gcid = c.loading_city_gcid AND p.unloading_city_gcid = c.unloading_city_gcid
              AND p.body_group = c.body_group AND p.weight_category = c.weight_category
              AND p.month = (c.month - interval '1 month')::date)""").fetchone()[0] == 0


def test_mart_trips_match_core(settings, dwh):
    """Сверка ячейки витрины с прямым расчётом по core для самого плотного направления."""
    cell = dwh.execute("SELECT month, loading_city_gcid, unloading_city_gcid, body_group, weight_category, "
                       "trips_count FROM mart.mart_route_card ORDER BY trips_count DESC, month LIMIT 1").fetchone()
    expected = dwh.execute("""
        SELECT count(DISTINCT f.price_id)
        FROM core.fact_prices f
        JOIN core.fact_price_month m ON m.price_id = f.price_id
        JOIN core.fact_price_body_group bg ON bg.price_id = f.price_id
        WHERE m.price_month = %s AND f.loading_city_gcid = %s AND f.unloading_city_gcid = %s
          AND bg.body_group = %s AND f.weight_category = %s AND m.price_date <= %s
          AND f.is_fact AND NOT f.complex AND NOT f.outlier_mark AND f.price_type IN (1,2,3,4,5,9)""",
                           (*cell[:5], settings.as_of)).fetchone()[0]
    assert cell[5] == expected


def test_predictions_future_have_no_actuals(dwh):
    if dwh.execute("SELECT count(*) FROM mart.ml_price_predictions").fetchone()[0] == 0:
        pytest.skip("ML-скоринг ещё не выполнен")
    assert dwh.execute("""
        SELECT count(*) FROM mart.ml_price_predictions
        WHERE split = 'future' AND (actual_price_rub IS NOT NULL OR abs_error_rub IS NOT NULL OR ape_pct IS NOT NULL)
    """).fetchone()[0] == 0
    assert dwh.execute("SELECT count(*) FROM mart.ml_price_predictions WHERE split = 'future' "
                       "AND loading_date <= as_of").fetchone()[0] == 0
    assert dwh.execute("SELECT count(*) - count(DISTINCT (price_id, model_name)) "
                       "FROM mart.ml_price_predictions").fetchone()[0] == 0
