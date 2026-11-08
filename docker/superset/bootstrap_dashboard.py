"""Дашборд «Рыночные ставки грузоперевозок» как код (ORM Superset).

Создаёт/обновляет подключение к DWH, датасеты поверх mart/core, чарты и дашборд с нативными
фильтрами карточки маршрута. Объекты ищутся по имени и пересоздаются — запуск идемпотентен.
"""
from __future__ import annotations

import hashlib
import json
import os
from urllib.parse import quote

from superset.app import create_app

app = create_app()

DATABASE_NAME = "DWH (PostgreSQL)"
DASHBOARD_TITLE = "Рыночные ставки грузоперевозок"
DASHBOARD_SLUG = "freight-rates"
DEFAULT_ROUTE = ("Москва → Ростов-на-Дону", "Тент", "FTL")

DATASETS = {
    # Карточка маршрута: вся помесячная история + человекочитаемый маршрут для фильтра.
    "v_route_card": """
        SELECT *, loading_city || ' → ' || unloading_city AS route
        FROM mart.mart_route_card
    """,
    # Сводка сети: последний месяц.
    "v_market_corridor": """
        SELECT *, loading_city || ' → ' || unloading_city AS route
        FROM mart.mart_market_corridor
    """,
    "v_price_predictions": """
        SELECT p.*, oc.city_name || ' → ' || dc.city_name AS route
        FROM mart.ml_price_predictions p
        LEFT JOIN core.dim_city oc ON oc.gc_id = p.loading_city_gcid
        LEFT JOIN core.dim_city dc ON dc.gc_id = p.unloading_city_gcid
    """,
    "v_model_metrics": "SELECT * FROM mart.ml_model_metrics",
    "v_dq_quarantine": "SELECT price_id, loading_date, reason_code, quarantined_at FROM core.dq_quarantine_prices",
}


def dwh_uri() -> str:
    env = os.environ
    return (f"postgresql+psycopg2://{quote(env['POSTGRES_USER'], safe='')}:{quote(env['POSTGRES_PASSWORD'], safe='')}"
            f"@{env['POSTGRES_HOST']}:{env['POSTGRES_PORT']}/{env['DWH_DB']}")


# ---------- конструкторы form_data ----------

def metric(sql: str, label: str) -> dict:
    option = hashlib.md5(f"{sql}|{label}".encode()).hexdigest()[:12]
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label, "optionName": f"metric_{option}"}


def where(sql: str) -> dict:
    return {"clause": "WHERE", "expressionType": "SQL", "sqlExpression": sql}


def big_number(metric_sql: str, label: str, filters: list[dict], fmt: str = "SMART_NUMBER") -> dict:
    return {"viz_type": "big_number_total", "metric": metric(metric_sql, label), "adhoc_filters": filters,
            "subheader": label, "header_font_size": 0.4, "subheader_font_size": 0.15, "y_axis_format": fmt}


def raw_table(columns: list[str], order_by: str, desc: bool = True, filters: list[dict] | None = None,
              limit: int = 500) -> dict:
    return {"viz_type": "table", "query_mode": "raw", "all_columns": columns,
            "order_by_cols": [json.dumps([order_by, not desc])], "adhoc_filters": filters or [],
            "row_limit": limit, "server_page_length": 20, "include_search": True, "table_timestamp_format": "%Y-%m-%d"}


def timeseries(viz: str, x_axis: str, grain: str, metrics: list[dict], filters: list[dict] | None = None,
               y_format: str = ",.0f") -> dict:
    return {"viz_type": viz, "x_axis": x_axis, "time_grain_sqla": grain, "metrics": metrics, "groupby": [],
            "adhoc_filters": filters or [], "time_range": "No filter", "row_limit": 10000,
            "show_legend": True, "legendOrientation": "top", "rich_tooltip": True, "y_axis_format": y_format,
            "markerEnabled": True, "markerSize": 4, "truncateYAxis": False}


CATBOOST = where("model_name = 'catboost_v1'")

# (ключ, название, датасет, form_data, ширина, высота, row)
CHARTS = [
    ("kpi_mape", "MAPE CatBoost на test, %", "v_model_metrics",
     big_number("MAX(mape_pct)", "MAPE CatBoost на test, %", [CATBOOST], ".2f"), 3, 20, 1),
    ("kpi_r2", "R² CatBoost на test", "v_model_metrics",
     big_number("MAX(r2)", "R² CatBoost на test", [CATBOOST], ".3f"), 3, 20, 1),
    ("kpi_high", "Направлений HIGH в текущем месяце", "v_market_corridor",
     big_number("COUNT(*)", "Направлений HIGH в текущем месяце", [where("reliability_level = 'HIGH'")]), 3, 20, 1),
    ("kpi_dq", "Сделок в карантине DQ", "v_dq_quarantine",
     big_number("COUNT(*)", "Сделок в карантине DQ", []), 3, 20, 1),

    ("network", "Коридор ставок по сети — текущий месяц", "v_market_corridor",
     raw_table(["route", "body_group", "weight_category", "p25_trip", "median_trip", "p75_trip", "median_km",
                "trips_count", "carriers_count", "cv_rate", "mom_pct", "yoy_pct", "reliability_level"],
               "trips_count"), 8, 60, 2),
    ("reliability", "Надёжность направлений — текущий месяц", "v_market_corridor",
     {"viz_type": "pie", "groupby": ["reliability_level"], "metric": metric("COUNT(*)", "Направлений"),
      "adhoc_filters": [], "row_limit": 100, "show_legend": True, "show_labels": True, "label_type": "key_percent",
      "donut": True, "number_format": "SMART_NUMBER"}, 4, 60, 2),

    ("route_trip", "Коридор ставки за рейс, ₽ (P25 / P50 / P75)", "v_route_card",
     timeseries("echarts_timeseries_line", "month", "P1M", [
         metric("MAX(p25_trip)", "P25"), metric("MAX(median_trip)", "P50 (медиана)"),
         metric("MAX(p75_trip)", "P75")]), 6, 50, 3),
    ("route_km", "Коридор ставки за км, ₽/км (P25 / P50 / P75)", "v_route_card",
     timeseries("echarts_timeseries_line", "month", "P1M", [
         metric("MAX(p25_km)", "P25"), metric("MAX(median_km)", "P50 (медиана)"),
         metric("MAX(p75_km)", "P75")], y_format=",.1f"), 6, 50, 3),
    ("route_history", "Карточка маршрута: история и доказательная база", "v_route_card",
     raw_table(["month", "trips_count", "carriers_count", "p25_trip", "median_trip", "p75_trip", "median_km",
                "cv_rate", "mom_pct", "yoy_pct", "freshness_days", "reliability_level"], "month", limit=200), 12, 50, 4),

    ("fact_vs_forecast", "Факт vs прогноз CatBoost, средняя ставка за рейс по дням, ₽", "v_price_predictions",
     timeseries("echarts_timeseries_line", "loading_date", "P1D", [
         metric("AVG(actual_price_rub)", "Факт"), metric("AVG(predicted_price_rub)", "Прогноз")],
         filters=[CATBOOST, where("split = 'future' OR NOT outlier_mark")]), 8, 50, 5),
    ("dq_reasons", "Карантин DQ по причинам", "v_dq_quarantine",
     {"viz_type": "echarts_timeseries_bar", "x_axis": "reason_code", "metrics": [metric("COUNT(*)", "Строк")],
      "groupby": [], "adhoc_filters": [], "time_range": "No filter", "row_limit": 100, "show_legend": False,
      "orientation": "vertical", "show_value": True, "y_axis_format": "SMART_NUMBER"}, 4, 50, 5),

    ("overpriced", "Рейсы, закрытые на 30%+ выше прогноза (test)", "v_price_predictions",
     raw_table(["loading_date", "route", "body_group", "weight_category", "distance", "actual_price_rub",
                "predicted_price_rub", "ape_pct", "outlier_mark"], "ape_pct",
               filters=[CATBOOST, where("split = 'test'"), where("actual_price_rub > predicted_price_rub * 1.3")],
               limit=200), 12, 50, 6),
]
ROUTE_CHARTS = ("route_trip", "route_km", "route_history")


def native_filter(fid: str, name: str, column: str, default: str, dataset_id: int,
                  in_scope: list[int], all_ids: list[int]) -> dict:
    return {
        "id": f"NATIVE_FILTER-{fid}",
        "name": name,
        "filterType": "filter_select",
        "type": "NATIVE_FILTER",
        "description": "",
        "targets": [{"datasetId": dataset_id, "column": {"name": column}}],
        "controlValues": {"enableEmptyFilter": True, "multiSelect": False, "defaultToFirstItem": False,
                          "searchAllOptions": True, "inverseSelection": False},
        "defaultDataMask": {
            "extraFormData": {"filters": [{"col": column, "op": "IN", "val": [default]}]},
            "filterState": {"value": [default], "label": default},
            "ownState": {},
        },
        "cascadeParentIds": [],
        "scope": {"rootPath": ["ROOT_ID"], "excluded": [i for i in all_ids if i not in in_scope]},
        "chartsInScope": in_scope,
        "tabsInScope": [],
    }


def build_position(slices: dict, intro: str) -> dict:
    pos: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASHBOARD_TITLE}},
    }

    def add_row(row_id: str, children: list[dict]) -> None:
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {"type": "ROW", "id": row_id, "children": [c["id"] for c in children],
                       "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for c in children:
            c["parents"] = ["ROOT_ID", "GRID_ID", row_id]
            pos[c["id"]] = c

    add_row("ROW-intro", [{"type": "MARKDOWN", "id": "MARKDOWN-intro", "children": [],
                           "meta": {"width": 12, "height": 18, "code": intro}}])
    rows: dict[int, list[dict]] = {}
    for key, name, _, _, width, height, row in CHARTS:
        s = slices[key]
        rows.setdefault(row, []).append({
            "type": "CHART", "id": f"CHART-{key}", "children": [],
            "meta": {"width": width, "height": height, "chartId": s.id, "sliceName": name, "uuid": str(s.uuid)},
        })
    for row, children in sorted(rows.items()):
        add_row(f"ROW-{row}", children)
    return pos


INTRO = """### Рыночный коридор ставок и прогноз тарифа
Данные: витрины `mart.*` DWH (RUB без НДС, снимок **as_of = 2026-10-26**).
Ряды 1–2 — сводка сети за текущий месяц; ряды 3–4 — **карточка маршрута** (выберите маршрут, кузов и
весовую категорию в фильтрах слева); ряды 5–6 — контроль ML-модели «факт vs прогноз» и качество данных.
Надёжность: `HIGH` — ≥30 рейсов, ≥5 перевозчиков, свежесть ≤30 дней; `MEDIUM` — ≥10 рейсов; иначе `LOW_DATA`."""


def main() -> None:
    from superset import db, security_manager
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    admin = security_manager.find_user(username=os.environ["SUPERSET_ADMIN_USER"])

    database = db.session.query(Database).filter_by(database_name=DATABASE_NAME).one_or_none()
    if database is None:
        database = Database(database_name=DATABASE_NAME)
        db.session.add(database)
    database.set_sqlalchemy_uri(dwh_uri())
    database.expose_in_sqllab = True
    db.session.flush()

    datasets = {}
    for name, sql in DATASETS.items():
        ds = db.session.query(SqlaTable).filter_by(table_name=name, database_id=database.id).one_or_none()
        if ds is None:
            ds = SqlaTable(table_name=name, database=database, schema="mart")
            db.session.add(ds)
        ds.sql = " ".join(sql.split())
        ds.owners = [admin]
        db.session.flush()
        ds.fetch_metadata()
        datasets[name] = ds

    # Пересоздаём дашборд и чарты из кода.
    old = db.session.query(Dashboard).filter_by(slug=DASHBOARD_SLUG).one_or_none()
    if old is not None:
        db.session.delete(old)
    names = [c[1] for c in CHARTS]
    for s in db.session.query(Slice).filter(Slice.slice_name.in_(names)).all():
        db.session.delete(s)
    db.session.flush()

    slices = {}
    for key, name, ds_name, form_data, *_ in CHARTS:
        ds = datasets[ds_name]
        params = {**form_data, "datasource": f"{ds.id}__table", "slice_name": name}
        s = Slice(slice_name=name, viz_type=form_data["viz_type"], datasource_type="table",
                  datasource_id=ds.id, params=json.dumps(params, ensure_ascii=False), owners=[admin])
        db.session.add(s)
        slices[key] = s
    db.session.flush()

    all_ids = [s.id for s in slices.values()]
    route_ids = [slices[k].id for k in ROUTE_CHARTS]
    route_ds = datasets["v_route_card"].id
    route, body, weight = DEFAULT_ROUTE
    metadata = {
        "native_filter_configuration": [
            native_filter("route", "Маршрут", "route", route, route_ds, route_ids, all_ids),
            native_filter("body", "Группа кузова", "body_group", body, route_ds, route_ids, all_ids),
            native_filter("weight", "Весовая категория", "weight_category", weight, route_ds, route_ids, all_ids),
        ],
        "color_scheme": "supersetColors",
        "refresh_frequency": 0,
        "cross_filters_enabled": False,
        "default_filters": "{}",
        "chart_configuration": {},
        "global_chart_configuration": {"scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "chartsInScope": all_ids},
    }
    dashboard = Dashboard(
        dashboard_title=DASHBOARD_TITLE, slug=DASHBOARD_SLUG, published=True, owners=[admin],
        slices=list(slices.values()),
        position_json=json.dumps(build_position(slices, INTRO), ensure_ascii=False),
        json_metadata=json.dumps(metadata, ensure_ascii=False),
    )
    db.session.add(dashboard)
    db.session.commit()
    print(f"[superset-bootstrap] dashboard '{DASHBOARD_TITLE}' (/superset/dashboard/{DASHBOARD_SLUG}/): "
          f"{len(slices)} charts, {len(datasets)} datasets")


if __name__ == "__main__":
    with app.app_context():
        main()
