-- =====================================================================
-- STAGE: данные кладутся как пришли из API, без преобразований.
-- Одна строка = один ответ API по одному городу.
-- =====================================================================

-- Ответы Geocoding API (справочник городов)
CREATE TABLE IF NOT EXISTS stg.cities_raw (
    id            BIGSERIAL PRIMARY KEY,
    city_query    TEXT        NOT NULL,          -- что искали, например 'Moscow'
    country_code  TEXT        NOT NULL,          -- фильтр по стране, например 'RU'
    payload       JSONB       NOT NULL,          -- полный ответ API
    selected      JSONB       NOT NULL,          -- выбранный результат (город, координаты, часовой пояс...)
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_cities_raw_query ON stg.cities_raw (city_query, loaded_at DESC);

-- Ответы Forecast API (ежечасно) и Previous Runs API (бэкфилл)
CREATE TABLE IF NOT EXISTS stg.forecast_raw (
    id              BIGSERIAL PRIMARY KEY,
    batch_id        TEXT        NOT NULL,        -- run_id запуска DAG'а
    source          TEXT        NOT NULL         -- 'forecast' | 'previous_runs'
                    CHECK (source IN ('forecast', 'previous_runs')),
    city_query      TEXT        NOT NULL,        -- ссылка на stg.cities_raw.city_query
    latitude        DOUBLE PRECISION NOT NULL,   -- координаты, которые запрашивали
    longitude       DOUBLE PRECISION NOT NULL,
    request_params  JSONB       NOT NULL,        -- параметры запроса (переменные, даты и т.п.)
    payload         JSONB       NOT NULL,        -- полный ответ API по этому городу
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()  -- момент загрузки = версия прогноза
);

CREATE INDEX IF NOT EXISTS ix_forecast_raw_loaded ON stg.forecast_raw (loaded_at);
CREATE INDEX IF NOT EXISTS ix_forecast_raw_source ON stg.forecast_raw (source, id);
