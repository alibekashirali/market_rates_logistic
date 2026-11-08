-- CORE: очищенные и нормализованные данные (Вопрос 1) + дедуплицированные измерения (Вопрос 2).

-- Города: одна строка на gc_id (минимальный cities.id), город и регион разобраны из full_name_local.
CREATE TABLE IF NOT EXISTS core.dim_city (
    gc_id           integer PRIMARY KEY,
    city_id         bigint  NOT NULL,
    city_name       text    NOT NULL,
    region          text,
    full_name_local text    NOT NULL,
    latitude        double precision,
    longitude       double precision
);

-- Бизнес-группы кузова (std_id по постановке; для «Прочие» и «Не указан» std_id нет).
CREATE TABLE IF NOT EXISTS core.dim_body_group (
    body_group text PRIMARY KEY,
    std_id     integer,
    sort_order smallint NOT NULL
);
INSERT INTO core.dim_body_group (body_group, std_id, sort_order) VALUES
    ('Тент', 56, 1), ('Рефрижератор', 4, 2), ('Изотерм', 2, 3), ('Борт', 15, 4),
    ('Прочие', NULL, 5), ('Не указан', NULL, 6)
ON CONFLICT (body_group) DO UPDATE SET std_id = EXCLUDED.std_id, sort_order = EXCLUDED.sort_order;

-- Маппинг кода кузова 1С в бизнес-группу. NULL -> «Не указан», неизвестные коды -> «Прочие».
CREATE OR REPLACE FUNCTION core.body_group_of(p_body_type_id integer)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN p_body_type_id IS NULL                       THEN 'Не указан'
        WHEN p_body_type_id IN (56, 57, 58, 59, 60, 61)   THEN 'Тент'
        WHEN p_body_type_id IN (4, 46, 47, 48)            THEN 'Рефрижератор'
        WHEN p_body_type_id = 2                           THEN 'Изотерм'
        WHEN p_body_type_id IN (1, 15)                    THEN 'Борт'
        ELSE 'Прочие'
    END
$$;

-- Очищенные сделки: прошли DQ, цена приведена к RUB, категории заполнены.
-- outlier_mark / complex / is_fact сохраняются как есть — это фильтры витрины, а не DQ-брак.
CREATE TABLE IF NOT EXISTS core.fact_prices (
    price_id               bigint PRIMARY KEY,
    created_at             timestamp(0),
    subscriber_id          integer,
    organization_id        integer,
    loading_date           date             NOT NULL,
    end_loading_date       date,
    loading_city_gcid      integer          NOT NULL,
    unloading_city_gcid    integer          NOT NULL,
    distance               double precision NOT NULL,
    total_weight           double precision NOT NULL,
    total_volume           double precision NOT NULL,
    body_type              integer,
    body_group             text,            -- основная группа кузова (для ML): body_type, иначе min(price_body_types)
    price                  double precision NOT NULL,
    currency_code          varchar(3)       NOT NULL,
    nearest_rate_date      date,
    fx_rate                double precision NOT NULL,
    price_rub              numeric(14, 2)   NOT NULL,
    price_type             smallint         NOT NULL,
    is_fact                boolean          NOT NULL,
    complex                boolean          NOT NULL,
    outlier_mark           boolean          NOT NULL,
    weight_category        varchar(4)       NOT NULL,
    volume_category        varchar(4)       NOT NULL,
    weight_category_filled boolean          NOT NULL,
    volume_category_filled boolean          NOT NULL,
    etl_run_id             uuid             NOT NULL,
    loaded_at              timestamptz      NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fact_prices_loading_date_idx ON core.fact_prices (loading_date);

-- Мост «сделка × бизнес-группа кузова»: одна строка на пару, без дублей внутри группы.
CREATE TABLE IF NOT EXISTS core.fact_price_body_group (
    price_id   bigint NOT NULL REFERENCES core.fact_prices (price_id) ON DELETE CASCADE,
    body_group text   NOT NULL REFERENCES core.dim_body_group (body_group),
    PRIMARY KEY (price_id, body_group)
);

-- Помесячная развёртка сделки из price_dates (зерно месяца витрины).
CREATE TABLE IF NOT EXISTS core.fact_price_month (
    price_id    bigint NOT NULL REFERENCES core.fact_prices (price_id) ON DELETE CASCADE,
    price_month date   NOT NULL,
    price_date  date   NOT NULL,
    PRIMARY KEY (price_id, price_month)
);
CREATE INDEX IF NOT EXISTS fact_price_month_month_idx ON core.fact_price_month (price_month);

-- Карантин DQ: отбракованные строки с основной причиной, полным списком причин и исходной строкой.
CREATE TABLE IF NOT EXISTS core.dq_quarantine_prices (
    price_id       bigint PRIMARY KEY,
    loading_date   date        NOT NULL,
    reason_code    text        NOT NULL,
    reason_codes   text[]      NOT NULL,
    payload        jsonb       NOT NULL,
    etl_run_id     uuid        NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dq_quarantine_loading_date_idx ON core.dq_quarantine_prices (loading_date);
CREATE INDEX IF NOT EXISTS dq_quarantine_reason_idx ON core.dq_quarantine_prices (reason_code);

-- Журнал запусков (аудит, не аналитические данные).
CREATE TABLE IF NOT EXISTS core.etl_runs (
    run_id       uuid PRIMARY KEY,
    step         text        NOT NULL,
    window_start date,
    window_end   date,
    status       text        NOT NULL,
    stats        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz
);
