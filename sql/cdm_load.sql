-- =====================================================================
-- РОЛЬ 3: перенос dds → cdm (витрины)
-- Выполняется задачей dds_to_cdm каждый час, после stg_to_dds.
-- Идемпотентно: DDL → TRUNCATE → INSERT SELECT из dds.*.
-- Если dds-таблицы пусты (до backfill / первого часа) — вставляется 0 строк.
-- =====================================================================

-- ── DDL ──────────────────────────────────────────────────────────────

-- MAE и bias по городу и горизонту прогноза (главный график дашборда).
-- Источник: dds.forecast_by_lead (backfill, явный lead_days).
-- "Факт" = lead_days = 0 (_previous_day0 ≈ фактическое наблюдение).
CREATE TABLE IF NOT EXISTS cdm.error_by_lead_time (
    city_id             INTEGER      NOT NULL,
    city_name           TEXT         NOT NULL,
    country             TEXT,
    lead_days           SMALLINT     NOT NULL,
    mae_temperature     NUMERIC(8,3),
    bias_temperature    NUMERIC(8,3),
    mae_precipitation   NUMERIC(8,3),
    bias_precipitation  NUMERIC(8,3),
    mae_wind_speed      NUMERIC(8,3),
    bias_wind_speed     NUMERIC(8,3),
    sample_count        BIGINT       NOT NULL,
    refreshed_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, lead_days)
);

-- Почасовое сравнение прогноза с фактом на горизонтах 1, 3, 7 дней.
CREATE TABLE IF NOT EXISTS cdm.forecast_vs_actual (
    city_id                INTEGER     NOT NULL,
    city_name              TEXT        NOT NULL,
    target_ts              TIMESTAMP   NOT NULL,
    lead_days              SMALLINT    NOT NULL,
    forecast_temperature   NUMERIC,
    actual_temperature     NUMERIC,
    error_temperature      NUMERIC,
    forecast_precipitation NUMERIC,
    actual_precipitation   NUMERIC,
    error_precipitation    NUMERIC,
    forecast_wind_speed    NUMERIC,
    actual_wind_speed      NUMERIC,
    error_wind_speed       NUMERIC,
    refreshed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, target_ts, lead_days)
);

-- Ревизии почасового прогноза (SCD2): сколько раз и насколько менялся прогноз.
-- Источник: dds.forecast_hourly — все версии из живого конвейера.
CREATE TABLE IF NOT EXISTS cdm.forecast_revisions (
    city_id           INTEGER     NOT NULL,
    city_name         TEXT        NOT NULL,
    target_ts         TIMESTAMP   NOT NULL,
    revision_count    INTEGER     NOT NULL,
    first_temperature NUMERIC,
    last_temperature  NUMERIC,
    temp_range        NUMERIC,
    first_precip      NUMERIC,
    last_precip       NUMERIC,
    refreshed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, target_ts)
);

-- ── TRUNCATE (полное перестроение каждый час) ─────────────────────────

TRUNCATE TABLE cdm.error_by_lead_time;
TRUNCATE TABLE cdm.forecast_vs_actual;
TRUNCATE TABLE cdm.forecast_revisions;

-- ── INSERT: error_by_lead_time ────────────────────────────────────────

INSERT INTO cdm.error_by_lead_time (
    city_id, city_name, country, lead_days,
    mae_temperature,  bias_temperature,
    mae_precipitation, bias_precipitation,
    mae_wind_speed,   bias_wind_speed,
    sample_count, refreshed_at
)
SELECT
    f.city_id,
    c.name,
    c.country,
    f.lead_days,
    round(avg(abs(f.temperature   - a.temperature)),   3),
    round(avg(    f.temperature   - a.temperature ),   3),
    round(avg(abs(f.precipitation - a.precipitation)), 3),
    round(avg(    f.precipitation - a.precipitation),  3),
    round(avg(abs(f.wind_speed    - a.wind_speed)),    3),
    round(avg(    f.wind_speed    - a.wind_speed ),    3),
    count(*),
    now()
FROM dds.forecast_by_lead f
JOIN dds.forecast_by_lead a
    ON  a.city_id   = f.city_id
    AND a.target_ts = f.target_ts
    AND a.lead_days = 0
JOIN dds.dim_city c ON c.city_id = f.city_id
WHERE f.lead_days > 0
  AND f.target_ts  < now()
  AND f.temperature  IS NOT NULL
  AND a.temperature  IS NOT NULL
GROUP BY f.city_id, c.name, c.country, f.lead_days;

-- ── INSERT: forecast_vs_actual ────────────────────────────────────────

INSERT INTO cdm.forecast_vs_actual (
    city_id, city_name, target_ts, lead_days,
    forecast_temperature,   actual_temperature,   error_temperature,
    forecast_precipitation, actual_precipitation, error_precipitation,
    forecast_wind_speed,    actual_wind_speed,    error_wind_speed,
    refreshed_at
)
SELECT
    f.city_id,
    c.name,
    f.target_ts,
    f.lead_days,
    f.temperature,
    a.temperature,
    f.temperature   - a.temperature,
    f.precipitation,
    a.precipitation,
    f.precipitation - a.precipitation,
    f.wind_speed,
    a.wind_speed,
    f.wind_speed    - a.wind_speed,
    now()
FROM dds.forecast_by_lead f
JOIN dds.forecast_by_lead a
    ON  a.city_id   = f.city_id
    AND a.target_ts = f.target_ts
    AND a.lead_days = 0
JOIN dds.dim_city c ON c.city_id = f.city_id
WHERE f.lead_days IN (1, 3, 7)
  AND f.target_ts  < now()
  AND f.temperature IS NOT NULL;

-- ── INSERT: forecast_revisions ────────────────────────────────────────

INSERT INTO cdm.forecast_revisions (
    city_id, city_name, target_ts,
    revision_count,
    first_temperature, last_temperature, temp_range,
    first_precip, last_precip,
    refreshed_at
)
WITH ranked AS (
    SELECT
        h.city_id,
        h.target_ts,
        h.temperature,
        h.precipitation,
        count(*)    OVER (PARTITION BY h.city_id, h.target_ts)                                AS revision_count,
        row_number() OVER (PARTITION BY h.city_id, h.target_ts ORDER BY h.valid_from ASC)  AS rn_first,
        row_number() OVER (PARTITION BY h.city_id, h.target_ts ORDER BY h.valid_from DESC) AS rn_last
    FROM dds.forecast_hourly h
)
SELECT
    r.city_id,
    c.name,
    r.target_ts,
    max(r.revision_count),
    max(r.temperature)   FILTER (WHERE r.rn_first = 1),
    max(r.temperature)   FILTER (WHERE r.rn_last  = 1),
    max(r.temperature) - min(r.temperature),
    max(r.precipitation) FILTER (WHERE r.rn_first = 1),
    max(r.precipitation) FILTER (WHERE r.rn_last  = 1),
    now()
FROM ranked r
JOIN dds.dim_city c ON c.city_id = r.city_id
GROUP BY r.city_id, c.name, r.target_ts;
