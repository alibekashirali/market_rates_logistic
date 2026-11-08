-- RAW: слепок нужного подмножества источника deals_bigdata без преобразований.
-- prices и дочерние таблицы перезаписываются окном loading_date, справочники — целиком.
-- price_group_stats / prices.price_group_id сознательно не извлекаются: для витрины они не нужны,
-- а для ML это прямая утечка таргета (агрегаты включают предсказываемую сделку).

CREATE TABLE IF NOT EXISTS raw.prices (
    id                  bigint PRIMARY KEY,
    created_at          timestamp(0),
    updated_at          timestamp(0),
    subscriber_id       integer,
    organization_id     integer,
    loading_date        date             NOT NULL,
    end_loading_date    date,
    loading_city_gcid   integer          NOT NULL,
    unloading_city_gcid integer          NOT NULL,
    distance            double precision NOT NULL,
    total_weight        double precision NOT NULL,
    total_volume        double precision NOT NULL,
    body_type           integer,
    price               double precision NOT NULL,
    currency_code       varchar(3)       NOT NULL,
    complex             boolean          NOT NULL,
    price_type          smallint         NOT NULL,
    is_fact             boolean          NOT NULL,
    nearest_rate_date   date,
    outlier_mark        boolean          NOT NULL,
    weight_category     varchar(4),
    volume_category     varchar(4),
    source              varchar(255),
    _extracted_at       timestamptz      NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS raw_prices_loading_date_idx ON raw.prices (loading_date);

-- Дочерние таблицы удаляются каскадом при перезаписи окна prices.
CREATE TABLE IF NOT EXISTS raw.price_body_types (
    price_id     bigint  NOT NULL REFERENCES raw.prices (id) ON DELETE CASCADE,
    body_type_id integer NOT NULL,
    PRIMARY KEY (price_id, body_type_id)
);

CREATE TABLE IF NOT EXISTS raw.price_dates (
    price_id   bigint NOT NULL REFERENCES raw.prices (id) ON DELETE CASCADE,
    price_date date   NOT NULL,
    PRIMARY KEY (price_id, price_date)
);

CREATE TABLE IF NOT EXISTS raw.cities (
    id              bigint PRIMARY KEY,
    full_name_local varchar(255) NOT NULL,
    gc_id           integer      NOT NULL,
    name            varchar(255) NOT NULL,
    longitude       double precision,
    latitude        double precision
);

CREATE TABLE IF NOT EXISTS raw.body_types (
    id         bigint PRIMARY KEY,
    name       varchar(255) NOT NULL,
    id_from_1c integer      NOT NULL,
    std_id     integer
);

CREATE TABLE IF NOT EXISTS raw.exchange_rates (
    id           bigint PRIMARY KEY,
    num_code     varchar(255)     NOT NULL,
    value        double precision NOT NULL,
    setting_date date             NOT NULL
);
