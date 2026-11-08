-- MART: «Карточка маршрута и коридор рыночных ставок» (Вопрос 2).
-- Grain: месяц (price_dates.price_date) × город отправления × город назначения × группа кузова × весовая категория.
-- volume_category не входит в grain, хранится как атрибут (мода по ячейке).

CREATE TABLE IF NOT EXISTS mart.mart_route_card (
    month                date        NOT NULL,
    loading_city_gcid    integer     NOT NULL,
    loading_city         text,
    loading_region       text,
    unloading_city_gcid  integer     NOT NULL,
    unloading_city       text,
    unloading_region     text,
    body_group           text        NOT NULL,
    weight_category      varchar(4)  NOT NULL,
    volume_category_mode varchar(4),
    -- коридор за рейс, RUB без НДС
    p25_trip             numeric(14, 2),
    median_trip          numeric(14, 2),
    p75_trip             numeric(14, 2),
    avg_trip             numeric(14, 2),
    -- коридор за км, RUB без НДС
    p25_km               numeric(12, 2),
    median_km            numeric(12, 2),
    p75_km               numeric(12, 2),
    -- доказательная база
    trips_count          integer     NOT NULL,
    carriers_count       integer     NOT NULL,
    cv_rate              numeric(10, 4),
    mom_pct              numeric(10, 2),
    yoy_pct              numeric(10, 2),
    last_trip_date       date        NOT NULL,
    freshness_days       integer     NOT NULL,
    reliability_level    text        NOT NULL CHECK (reliability_level IN ('HIGH', 'MEDIUM', 'LOW_DATA')),
    as_of                date        NOT NULL,
    refreshed_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (month, loading_city_gcid, unloading_city_gcid, body_group, weight_category)
);
-- Сценарий «детальная карточка маршрута»: история одного направления по месяцам.
CREATE INDEX IF NOT EXISTS mart_route_card_route_idx
    ON mart.mart_route_card (loading_city_gcid, unloading_city_gcid, body_group, weight_category, month);

-- Пересчёт витрины начиная с месяца p_month_from (включительно) по состоянию на p_as_of.
-- Все сделки окна loading_date >= start имеют price_date >= start, поэтому пересчёт хвоста
-- [month(start); ...] полностью покрывает затронутые месяцы. Идемпотентно: DELETE + INSERT
-- в транзакции вызывающего кода. Возвращает число вставленных строк.
CREATE OR REPLACE FUNCTION mart.refresh_route_card(p_month_from date, p_as_of date)
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    v_from date := date_trunc('month', p_month_from)::date;
    v_rows integer;
BEGIN
    DELETE FROM mart.mart_route_card WHERE month >= v_from;

    INSERT INTO mart.mart_route_card (
        month, loading_city_gcid, loading_city, loading_region, unloading_city_gcid, unloading_city, unloading_region,
        body_group, weight_category, volume_category_mode,
        p25_trip, median_trip, p75_trip, avg_trip, p25_km, median_km, p75_km,
        trips_count, carriers_count, cv_rate, mom_pct, yoy_pct,
        last_trip_date, freshness_days, reliability_level, as_of
    )
    WITH market_facts AS (
        SELECT m.price_month AS month,
               m.price_date,
               f.price_id,
               f.loading_city_gcid,
               f.unloading_city_gcid,
               bg.body_group,
               f.weight_category,
               f.volume_category,
               f.organization_id,
               f.price_type,
               f.price_rub,
               f.price_rub / NULLIF(f.distance, 0)::numeric AS price_km
        FROM core.fact_price_month m
        JOIN core.fact_prices f            ON f.price_id = m.price_id
        JOIN core.fact_price_body_group bg ON bg.price_id = m.price_id
        WHERE m.price_month >= v_from
          AND m.price_date <= p_as_of                       -- плановые будущие месяцы не входят в историю рынка
          AND f.is_fact AND f.price_type IN (1, 2, 3, 4, 5, 9)
          AND NOT f.complex
          AND f.price > 0 AND f.distance > 0
          AND f.price_rub IS NOT NULL
          AND NOT f.outlier_mark
    ), cells AS (
        SELECT month, loading_city_gcid, unloading_city_gcid, body_group, weight_category,
               mode() WITHIN GROUP (ORDER BY volume_category)                      AS volume_category_mode,
               percentile_cont(0.25) WITHIN GROUP (ORDER BY price_rub)             AS p25_trip,
               percentile_cont(0.50) WITHIN GROUP (ORDER BY price_rub)             AS median_trip,
               percentile_cont(0.75) WITHIN GROUP (ORDER BY price_rub)             AS p75_trip,
               avg(price_rub)                                                      AS avg_trip,
               stddev_samp(price_rub)                                              AS std_trip,
               percentile_cont(0.25) WITHIN GROUP (ORDER BY price_km)              AS p25_km,
               percentile_cont(0.50) WITHIN GROUP (ORDER BY price_km)              AS median_km,
               percentile_cont(0.75) WITHIN GROUP (ORDER BY price_km)              AS p75_km,
               count(DISTINCT price_id)                                            AS trips_count,
               count(DISTINCT organization_id) FILTER (WHERE price_type IN (3, 9)) AS carriers_count,
               max(price_date)                                                     AS last_trip_date
        FROM market_facts
        GROUP BY month, loading_city_gcid, unloading_city_gcid, body_group, weight_category
    )
    SELECT c.month,
           c.loading_city_gcid, lc.city_name, lc.region,
           c.unloading_city_gcid, uc.city_name, uc.region,
           c.body_group, c.weight_category, c.volume_category_mode,
           round(c.p25_trip::numeric, 2), round(c.median_trip::numeric, 2), round(c.p75_trip::numeric, 2),
           round(c.avg_trip, 2),
           round(c.p25_km::numeric, 2), round(c.median_km::numeric, 2), round(c.p75_km::numeric, 2),
           c.trips_count, c.carriers_count,
           CASE WHEN c.trips_count >= 2 AND c.avg_trip <> 0 THEN round(c.std_trip / c.avg_trip, 4) END,
           NULL, NULL,                                        -- mom/yoy считаются ниже, когда все месяцы на месте
           c.last_trip_date,
           p_as_of - c.last_trip_date,
           CASE
               WHEN c.trips_count >= 30 AND c.carriers_count >= 5 AND p_as_of - c.last_trip_date <= 30 THEN 'HIGH'
               WHEN c.trips_count >= 10 THEN 'MEDIUM'
               ELSE 'LOW_DATA'
           END,
           p_as_of
    FROM cells c
    LEFT JOIN core.dim_city lc ON lc.gc_id = c.loading_city_gcid
    LEFT JOIN core.dim_city uc ON uc.gc_id = c.unloading_city_gcid;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    -- MoM / YoY: строго календарный предыдущий месяц и тот же месяц прошлого года.
    -- Нет ячейки сравнения или знаменатель 0 -> NULL (без подстановки «последнего доступного»).
    UPDATE mart.mart_route_card cur
    SET mom_pct = CASE WHEN prev_m.median_trip <> 0
                       THEN round(100 * (cur.median_trip - prev_m.median_trip) / prev_m.median_trip, 2) END,
        yoy_pct = CASE WHEN prev_y.median_trip <> 0
                       THEN round(100 * (cur.median_trip - prev_y.median_trip) / prev_y.median_trip, 2) END
    FROM mart.mart_route_card self
    LEFT JOIN mart.mart_route_card prev_m
           ON prev_m.loading_city_gcid = self.loading_city_gcid
          AND prev_m.unloading_city_gcid = self.unloading_city_gcid
          AND prev_m.body_group = self.body_group
          AND prev_m.weight_category = self.weight_category
          AND prev_m.month = (self.month - interval '1 month')::date
    LEFT JOIN mart.mart_route_card prev_y
           ON prev_y.loading_city_gcid = self.loading_city_gcid
          AND prev_y.unloading_city_gcid = self.unloading_city_gcid
          AND prev_y.body_group = self.body_group
          AND prev_y.weight_category = self.weight_category
          AND prev_y.month = (self.month - interval '1 year')::date
    WHERE self.month >= v_from
      AND cur.month = self.month
      AND cur.loading_city_gcid = self.loading_city_gcid
      AND cur.unloading_city_gcid = self.unloading_city_gcid
      AND cur.body_group = self.body_group
      AND cur.weight_category = self.weight_category;

    RETURN v_rows;
END
$$;

-- Сценарий «сводный анализ маршрутной сети»: последний (текущий) месяц по всем направлениям.
CREATE OR REPLACE VIEW mart.mart_market_corridor AS
SELECT *
FROM mart.mart_route_card
WHERE month = (SELECT max(month) FROM mart.mart_route_card);
