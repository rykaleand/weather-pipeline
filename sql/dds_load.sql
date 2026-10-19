-- Перенос stg -> dds (+ SCD2). Выполняется каждый час одной транзакцией.
-- Идемпотентно: обрабатываются только строки stg.forecast_raw с id > watermark.

CREATE TABLE IF NOT EXISTS dds.dim_city (
    city_id      SERIAL PRIMARY KEY,
    city_query   TEXT NOT NULL UNIQUE,
    name         TEXT,
    country      TEXT,
    country_code TEXT,
    admin1       TEXT,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    elevation    DOUBLE PRECISION,
    timezone     TEXT,
    population   BIGINT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dds.dim_weather_code (
    weather_code SMALLINT PRIMARY KEY,
    description  TEXT NOT NULL
);

-- SCD2: естественный ключ версии (city_id, target_ts)
CREATE TABLE IF NOT EXISTS dds.forecast_hourly (
    id                        BIGSERIAL PRIMARY KEY,
    city_id                   INTEGER NOT NULL REFERENCES dds.dim_city (city_id),
    target_ts                 TIMESTAMP NOT NULL,
    temperature                NUMERIC,
    humidity                    NUMERIC,
    precipitation                NUMERIC,
    precipitation_probability     NUMERIC,
    wind_speed                     NUMERIC,
    cloud_cover                     NUMERIC,
    weather_code                    SMALLINT REFERENCES dds.dim_weather_code (weather_code),
    row_hash                        TEXT NOT NULL,
    valid_from                      TIMESTAMPTZ NOT NULL,
    valid_to                        TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current                      BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS ix_forecast_hourly_city_ts ON dds.forecast_hourly (city_id, target_ts);
CREATE INDEX IF NOT EXISTS ix_forecast_hourly_target_ts ON dds.forecast_hourly (target_ts);

-- не более одной активной версии на (city_id, target_ts)
CREATE UNIQUE INDEX IF NOT EXISTS ux_forecast_hourly_current
    ON dds.forecast_hourly (city_id, target_ts) WHERE is_current;

-- бэкфилл: здесь версия - lead_days, SCD2 не нужен
CREATE TABLE IF NOT EXISTS dds.forecast_by_lead (
    city_id      INTEGER NOT NULL REFERENCES dds.dim_city (city_id),
    target_ts    TIMESTAMP NOT NULL,
    lead_days    SMALLINT NOT NULL,
    temperature  NUMERIC,
    humidity     NUMERIC,
    precipitation NUMERIC,
    wind_speed   NUMERIC,
    cloud_cover  NUMERIC,
    weather_code SMALLINT REFERENCES dds.dim_weather_code (weather_code),
    loaded_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (city_id, target_ts, lead_days)
);

CREATE TABLE IF NOT EXISTS dds.etl_watermark (
    source      TEXT PRIMARY KEY,
    last_stg_id BIGINT NOT NULL DEFAULT 0
);

INSERT INTO dds.etl_watermark (source, last_stg_id)
VALUES ('forecast', 0), ('previous_runs', 0)
ON CONFLICT (source) DO NOTHING;

-- справочник кодов WMO
INSERT INTO dds.dim_weather_code (weather_code, description) VALUES
    (0,  'Ясно'),
    (1,  'Преимущественно ясно'),
    (2,  'Переменная облачность'),
    (3,  'Пасмурно'),
    (45, 'Туман'),
    (48, 'Изморозь'),
    (51, 'Слабая морось'),
    (53, 'Умеренная морось'),
    (55, 'Сильная морось'),
    (56, 'Слабая переохлаждённая морось'),
    (57, 'Сильная переохлаждённая морось'),
    (61, 'Небольшой дождь'),
    (63, 'Умеренный дождь'),
    (65, 'Сильный дождь'),
    (66, 'Слабый переохлаждённый дождь'),
    (67, 'Сильный переохлаждённый дождь'),
    (71, 'Небольшой снег'),
    (73, 'Умеренный снег'),
    (75, 'Сильный снег'),
    (77, 'Снежная крупа'),
    (80, 'Слабый ливень'),
    (81, 'Умеренный ливень'),
    (82, 'Сильный ливень'),
    (85, 'Слабый снегопад ливневого типа'),
    (86, 'Сильный снегопад ливневого типа'),
    (95, 'Гроза'),
    (96, 'Гроза с небольшим градом'),
    (99, 'Гроза с сильным градом')
ON CONFLICT (weather_code) DO NOTHING;

-- 1. dim_city: upsert по последней версии геокодинга
INSERT INTO dds.dim_city (
    city_query, name, country, country_code, admin1,
    latitude, longitude, elevation, timezone, population, updated_at
)
SELECT DISTINCT ON (city_query)
    city_query,
    selected ->> 'name',
    selected ->> 'country',
    selected ->> 'country_code',
    selected ->> 'admin1',
    (selected ->> 'latitude')::double precision,
    (selected ->> 'longitude')::double precision,
    (selected ->> 'elevation')::double precision,
    selected ->> 'timezone',
    NULLIF(selected ->> 'population', '')::bigint,
    loaded_at
FROM stg.cities_raw
ORDER BY city_query, loaded_at DESC
ON CONFLICT (city_query) DO UPDATE SET
    name         = EXCLUDED.name,
    country      = EXCLUDED.country,
    country_code = EXCLUDED.country_code,
    admin1       = EXCLUDED.admin1,
    latitude     = EXCLUDED.latitude,
    longitude    = EXCLUDED.longitude,
    elevation    = EXCLUDED.elevation,
    timezone     = EXCLUDED.timezone,
    population   = EXCLUDED.population,
    updated_at   = EXCLUDED.updated_at;

-- 2. forecast_hourly (SCD2) из source = 'forecast'.
-- row_hash сравнивается с prev_hash (LAG внутри пачки, иначе текущая активная
-- версия в dds) - непохожие хэши считаются изменением версии.
WITH new_stg AS (
    SELECT f.id AS stg_id, f.city_query, f.payload, f.loaded_at
    FROM stg.forecast_raw f
    WHERE f.source = 'forecast'
      AND f.id > (SELECT last_stg_id FROM dds.etl_watermark WHERE source = 'forecast')
),
unnested AS (
    SELECT
        c.city_id,
        n.stg_id,
        n.loaded_at,
        t.ts::timestamp AS target_ts,
        (n.payload -> 'hourly' -> 'temperature_2m' ->> (t.i - 1)::int)::numeric AS temperature,
        (n.payload -> 'hourly' -> 'relative_humidity_2m' ->> (t.i - 1)::int)::numeric AS humidity,
        (n.payload -> 'hourly' -> 'precipitation' ->> (t.i - 1)::int)::numeric AS precipitation,
        (n.payload -> 'hourly' -> 'precipitation_probability' ->> (t.i - 1)::int)::numeric AS precipitation_probability,
        (n.payload -> 'hourly' -> 'wind_speed_10m' ->> (t.i - 1)::int)::numeric AS wind_speed,
        (n.payload -> 'hourly' -> 'cloud_cover' ->> (t.i - 1)::int)::numeric AS cloud_cover,
        (n.payload -> 'hourly' -> 'weather_code' ->> (t.i - 1)::int)::smallint AS weather_code
    FROM new_stg n
    JOIN dds.dim_city c ON c.city_query = n.city_query
    CROSS JOIN LATERAL jsonb_array_elements_text(n.payload -> 'hourly' -> 'time')
        WITH ORDINALITY AS t(ts, i)
),
hashed AS (
    SELECT *,
        md5(concat_ws('|', temperature, humidity, precipitation,
                       precipitation_probability, wind_speed, cloud_cover, weather_code)) AS row_hash
    FROM unnested
),
baseline AS (
    SELECT city_id, target_ts, row_hash AS base_hash
    FROM dds.forecast_hourly
    WHERE is_current
),
diffed AS (
    SELECT h.*,
        COALESCE(
            LAG(h.row_hash) OVER (PARTITION BY h.city_id, h.target_ts ORDER BY h.loaded_at),
            b.base_hash
        ) AS prev_hash
    FROM hashed h
    LEFT JOIN baseline b ON b.city_id = h.city_id AND b.target_ts = h.target_ts
),
changes AS (
    SELECT *,
        LEAD(loaded_at) OVER (PARTITION BY city_id, target_ts ORDER BY loaded_at) AS next_change_loaded_at
    FROM diffed
    WHERE row_hash IS DISTINCT FROM prev_hash
),
-- закрываем старую активную версию только по первому изменению в пачке;
-- последующие изменения той же пачки уже учтены в INSERT через LEAD
first_changes AS (
    SELECT DISTINCT ON (city_id, target_ts) city_id, target_ts, loaded_at
    FROM changes
    ORDER BY city_id, target_ts, loaded_at
),
closed AS (
    UPDATE dds.forecast_hourly f
    SET valid_to = fc.loaded_at, is_current = FALSE
    FROM first_changes fc
    WHERE f.city_id = fc.city_id AND f.target_ts = fc.target_ts AND f.is_current
    RETURNING 1
),
inserted AS (
    INSERT INTO dds.forecast_hourly (
        city_id, target_ts, temperature, humidity, precipitation, precipitation_probability,
        wind_speed, cloud_cover, weather_code, row_hash, valid_from, valid_to, is_current
    )
    SELECT
        city_id, target_ts, temperature, humidity, precipitation, precipitation_probability,
        wind_speed, cloud_cover, weather_code, row_hash,
        loaded_at AS valid_from,
        COALESCE(next_change_loaded_at, 'infinity'::timestamptz) AS valid_to,
        (next_change_loaded_at IS NULL) AS is_current
    FROM changes
    -- форсирует выполнение UPDATE "closed" раньше этого INSERT (Postgres не
    -- гарантирует порядок независимых CTE, иначе ловим ux_forecast_hourly_current)
    WHERE (SELECT count(*) FROM closed) >= 0
    RETURNING 1
),
watermark_bump AS (
    UPDATE dds.etl_watermark w
    SET last_stg_id = s.max_id
    FROM (
        SELECT COALESCE(MAX(stg_id), (SELECT last_stg_id FROM dds.etl_watermark WHERE source = 'forecast')) AS max_id
        FROM new_stg
    ) s
    WHERE w.source = 'forecast'
    RETURNING 1
)
SELECT
    (SELECT count(*) FROM inserted) AS forecast_versions_inserted,
    (SELECT count(*) FROM closed) AS forecast_versions_closed;

-- 3. forecast_by_lead из source = 'previous_runs'
WITH new_stg AS (
    SELECT f.id AS stg_id, f.city_query, f.payload, f.loaded_at
    FROM stg.forecast_raw f
    WHERE f.source = 'previous_runs'
      AND f.id > (SELECT last_stg_id FROM dds.etl_watermark WHERE source = 'previous_runs')
),
unnested AS (
    SELECT
        c.city_id,
        n.stg_id,
        n.loaded_at,
        t.ts::timestamp AS target_ts,
        g.lead_days,
        (n.payload -> 'hourly' -> (CASE WHEN g.lead_days = 0 THEN 'temperature_2m' ELSE 'temperature_2m_previous_day' || g.lead_days END) ->> (t.i - 1)::int)::numeric AS temperature,
        (n.payload -> 'hourly' -> (CASE WHEN g.lead_days = 0 THEN 'relative_humidity_2m' ELSE 'relative_humidity_2m_previous_day' || g.lead_days END) ->> (t.i - 1)::int)::numeric AS humidity,
        (n.payload -> 'hourly' -> (CASE WHEN g.lead_days = 0 THEN 'precipitation' ELSE 'precipitation_previous_day' || g.lead_days END) ->> (t.i - 1)::int)::numeric AS precipitation,
        (n.payload -> 'hourly' -> (CASE WHEN g.lead_days = 0 THEN 'wind_speed_10m' ELSE 'wind_speed_10m_previous_day' || g.lead_days END) ->> (t.i - 1)::int)::numeric AS wind_speed,
        (n.payload -> 'hourly' -> (CASE WHEN g.lead_days = 0 THEN 'cloud_cover' ELSE 'cloud_cover_previous_day' || g.lead_days END) ->> (t.i - 1)::int)::numeric AS cloud_cover,
        (n.payload -> 'hourly' -> (CASE WHEN g.lead_days = 0 THEN 'weather_code' ELSE 'weather_code_previous_day' || g.lead_days END) ->> (t.i - 1)::int)::smallint AS weather_code
    FROM new_stg n
    JOIN dds.dim_city c ON c.city_query = n.city_query
    CROSS JOIN LATERAL jsonb_array_elements_text(n.payload -> 'hourly' -> 'time')
        WITH ORDINALITY AS t(ts, i)
    CROSS JOIN LATERAL generate_series(0, 7) AS g(lead_days)
),
upserted AS (
    INSERT INTO dds.forecast_by_lead (
        city_id, target_ts, lead_days, temperature, humidity, precipitation,
        wind_speed, cloud_cover, weather_code, loaded_at
    )
    SELECT city_id, target_ts, lead_days, temperature, humidity, precipitation,
           wind_speed, cloud_cover, weather_code, loaded_at
    FROM unnested
    WHERE temperature IS NOT NULL
    ON CONFLICT (city_id, target_ts, lead_days) DO UPDATE SET
        temperature  = EXCLUDED.temperature,
        humidity     = EXCLUDED.humidity,
        precipitation = EXCLUDED.precipitation,
        wind_speed   = EXCLUDED.wind_speed,
        cloud_cover  = EXCLUDED.cloud_cover,
        weather_code = EXCLUDED.weather_code,
        loaded_at    = EXCLUDED.loaded_at
    RETURNING 1
),
watermark_bump AS (
    UPDATE dds.etl_watermark w
    SET last_stg_id = s.max_id
    FROM (
        SELECT COALESCE(MAX(stg_id), (SELECT last_stg_id FROM dds.etl_watermark WHERE source = 'previous_runs')) AS max_id
        FROM new_stg
    ) s
    WHERE w.source = 'previous_runs'
    RETURNING 1
)
SELECT (SELECT count(*) FROM upserted) AS by_lead_rows_upserted;
