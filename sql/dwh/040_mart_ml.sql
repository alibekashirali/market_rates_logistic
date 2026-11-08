-- MART: результаты ML-контура (Вопрос 3) — «Факт vs Прогноз» и метрики моделей.

CREATE TABLE IF NOT EXISTS mart.ml_price_predictions (
    price_id            bigint         NOT NULL,
    model_name          text           NOT NULL,
    split               text           NOT NULL CHECK (split IN ('test', 'future')),
    loading_date        date           NOT NULL,
    loading_city_gcid   integer        NOT NULL,
    unloading_city_gcid integer        NOT NULL,
    body_group          text,
    weight_category     varchar(4),
    distance            double precision,
    actual_price_rub    numeric(14, 2),               -- NULL для будущих заявок
    predicted_price_rub numeric(14, 2) NOT NULL,
    abs_error_rub       numeric(14, 2),               -- NULL для будущих заявок
    ape_pct             numeric(10, 2),               -- NULL для будущих заявок
    outlier_mark        boolean        NOT NULL,      -- только для анализа, в модель не подаётся
    as_of               date           NOT NULL,
    scored_at           timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (price_id, model_name),
    CHECK (split = 'test' OR (actual_price_rub IS NULL AND abs_error_rub IS NULL AND ape_pct IS NULL))
);
CREATE INDEX IF NOT EXISTS ml_price_predictions_route_idx
    ON mart.ml_price_predictions (loading_city_gcid, unloading_city_gcid, loading_date);

CREATE TABLE IF NOT EXISTS mart.ml_model_metrics (
    model_name         text        NOT NULL,
    as_of              date        NOT NULL,
    train_start        date        NOT NULL,
    test_start         date        NOT NULL,
    n_train            integer     NOT NULL,
    n_test             integer     NOT NULL,
    r2                 numeric(8, 4),
    mape_pct           numeric(8, 2),
    rmse_rub           numeric(14, 2),
    mae_rub            numeric(14, 2),
    params             jsonb       NOT NULL DEFAULT '{}'::jsonb,
    feature_importance jsonb,
    trained_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model_name, as_of)
);
