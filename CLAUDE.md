# Контекст проекта: ППАД, задание 3 — конвейер данных о погоде

## Что это за задание

Курс «Прикладные пакеты для анализа данных», задание 3 «Конвейер обработки данных» (групповое, 30 баллов).
Дедлайн защиты — **21 октября 2026** (после него штраф 5 баллов). Консультации/защиты по средам.

Требования преподавателя:
- Пайплайн на **Airflow**, поднимается через **docker compose**.
- Слои хранилища в Postgres: **stg** (сырые данные) → **dds** (детальный слой) → **cdm** (витрины).
- Витрины в cdm формируются **только по данным из dds**.
- **Версионирование данных** — предпочтительно на dds (SCD2), допустимо на stg (тогда нужно решать очистку).
- Дашборд в **Metabase**, подключённый к cdm.
- Группа 3 человека, роли:
  1. stage + оркестрация + сбор данных + docker compose — **это моя роль**
  2. dds + перенос stg → dds (с SCD2)
  3. cdm + перенос dds → cdm + дашборд в Metabase
- Предметную область выбираем сами (исходная тема из репо преподавателя — учебные планы ИТМО — устарела, API сломано).
  Репо преподавателя для референса: https://github.com/kinomant/ppa-for-da/tree/master/airflow_workshop

## Наша тема: «Насколько можно верить прогнозу погоды?»

Каждый час забираем прогноз на 7 дней вперёд для 10 городов из **Open-Meteo** (бесплатно, без ключа).
Прогноз на один и тот же час пересчитывается много раз → храним все версии (SCD2 на dds) →
когда час наступил, знаем факт → считаем ошибку прогноза в зависимости от горизонта (за сколько дней сделан).

### Источник: Open-Meteo
- Forecast: `https://api.open-meteo.com/v1/forecast` — несколько городов в одном запросе
  (`latitude=a,b,c&longitude=x,y,z`, ответ — JSON-список по точкам). `forecast_days=7`, `past_days=2`
  (прошедшие часы = «факт»), `timezone=GMT` (все времена в UTC).
- Geocoding: `https://geocoding-api.open-meteo.com/v1/search?name=...&count=10&language=ru` — справочник городов.
- Previous Runs: `https://previous-runs-api.open-meteo.com/v1/forecast` — для бэкфилла истории.
  Переменные вида `temperature_2m_previous_dayN`, N=0..7: значение, предсказанное за N×24 ч до часа
  (`_previous_day0` ≈ текущий прогон/факт). Архив большинства моделей с января 2024.
  Параметры `start_date`, `end_date`. Нет `precipitation_probability`.
- Коды погоды WMO (0 ясно, 61 дождь, 71 снег, 95 гроза...) — маппинг есть в документации Forecast API.
- Ответ при ошибке параметров — HTTP 400 с JSON `{"error": true, "reason": "..."}`.

## Структура проекта (каркас уже написан, НЕ протестирован в docker)

```
docker-compose.yml          Airflow 2.10.5 (LocalExecutor) + airflow-db + warehouse (Postgres 16) + Metabase
sql/init/01_schemas.sql     CREATE SCHEMA stg, dds, cdm
sql/init/02_stg_ddl.sql     stg.cities_raw, stg.forecast_raw
sql/dds_load.sql            ЗАГЛУШКА для роли 2 (только комментарии → задача skipped)
sql/cdm_load.sql            ЗАГЛУШКА для роли 3
dags/weather_pipeline.py    ежечасный DAG (в :05)
dags/weather_backfill.py    ручной DAG, параметр days_back (по умолчанию 30)
dags/weather_common/
  config.py                 города, переменные, URL, ретеншн stage (30 дней)
  openmeteo.py              HTTP-клиент с ретраями: geocode, fetch_forecast, fetch_previous_runs
  stg.py                    запись в stage, чтение городов, cleanup, run_sql_file
```

### Окружение
- Airflow UI: http://localhost:8080 (admin / admin)
- Metabase: http://localhost:3333 (подключение к БД: host `warehouse`, port 5432, db `de`, jovyan/jovyan)
- Хранилище с хоста: `localhost:15432`, db `de`, jovyan/jovyan
- Подключение Airflow к хранилищу — conn id `pg_warehouse`, задаётся env-переменной `AIRFLOW_CONN_PG_WAREHOUSE`
  в compose (в UI создавать не нужно).
- DAG'и не на паузе после создания (`DAGS_ARE_PAUSED_AT_CREATION=false`) — сбор стартует сам.
- `sql/init/*` выполняется только при первом создании тома warehouse-data. Чтобы переприменить —
  `docker compose down -v` (удалит данные!) или выполнить скрипт вручную через psql.

### DAG weather_pipeline
```
ensure_cities → load_forecast → stg_to_dds → dds_to_cdm → cleanup_stg
```
- `ensure_cities`: геокодит города из config.CITIES, которых ещё нет в stg.cities_raw.
- `load_forecast`: один запрос по всем городам, по строке на город в stg.forecast_raw (source='forecast').
- `stg_to_dds` / `dds_to_cdm`: выполняют `sql/dds_load.sql` / `sql/cdm_load.sql` целиком одной транзакцией.
  Если в файле только комментарии — AirflowSkipException. trigger_rule=none_failed, поэтому skip не ломает цепочку.
- `cleanup_stg`: удаляет stg.forecast_raw старше 30 дней.

### DAG weather_backfill
Ручной запуск (Trigger DAG w/ config, `{"days_back": 30}`). Берёт города из stage (поэтому сначала должен
отработать weather_pipeline), режет период на куски по 14 дней, пишет в stg.forecast_raw с source='previous_runs'.

## Контракт stage (для ролей 2 и 3)

**stg.cities_raw**: `id, city_query, country_code, payload jsonb (полный ответ), selected jsonb (выбранный город), loaded_at`.
В `selected`: `name` (по-русски), `latitude`, `longitude`, `elevation`, `timezone`, `country`, `country_code`, `population`, `admin1`.

**stg.forecast_raw**: `id, batch_id (run_id DAG'а), source, city_query, latitude, longitude, request_params jsonb, payload jsonb, loaded_at`.

`source='forecast'` — payload:
```json
{"latitude": .., "longitude": .., "elevation": .., "utc_offset_seconds": 0,
 "hourly": {"time": ["2026-10-09T00:00", ...],
            "temperature_2m": [...], "relative_humidity_2m": [...], "precipitation": [...],
            "precipitation_probability": [...], "wind_speed_10m": [...], "cloud_cover": [...],
            "weather_code": [...]},
 "hourly_units": {...}}
```
Массивы параллельные: i-й элемент каждой переменной относится к `time[i]` (UTC). Около 216 часов (2 прошлых дня + 7 вперёд).
**Версия прогноза = `loaded_at`**. Горизонт = `target_ts − loaded_at`. Факт для часа — значение из загрузки, сделанной после него.
Разворачивание в строки в SQL: `jsonb_array_elements_text(payload->'hourly'->'time') WITH ORDINALITY` + индекс в остальные массивы.

`source='previous_runs'` — payload с ключами `hourly.temperature_2m_previous_day0 ... _previous_day7` и т. д.
Здесь версия задаётся не loaded_at, а lead-днём: прогноз выпущен ≈ `target_ts − N дней`.

## Задумка dds и cdm (договорённость, ещё не реализовано)

dds:
- `dds.dim_city` (из cities_raw.selected), `dds.dim_weather_code` (WMO-маппинг).
- `dds.forecast_hourly`: `city_id, target_ts, temperature, humidity, precipitation, precipitation_probability,
  wind_speed, cloud_cover, weather_code, valid_from, valid_to, is_current, row_hash` — SCD2 по (city_id, target_ts).
- Инкрементальность через watermark по `stg.forecast_raw.id`.
- Бэкфилл — либо отдельная таблица `dds.forecast_by_lead (city_id, target_ts, lead_days, ...)`, либо приведение к SCD2-виду.

cdm (витрины под дашборд):
- `cdm.error_by_lead_time` — MAE и bias по городу/переменной/горизонту (главный график).
- `cdm.forecast_vs_actual` — факт vs прогноз за 24/72/168 ч.
- `cdm.forecast_revisions` — сколько раз и насколько менялся прогноз на день.
- `cdm.precip_hit_rate` — попадания/пропуски/ложные тревоги по осадкам.
- `cdm.city_daily_summary`.

## Что делать дальше (моя роль)

1. `docker compose up -d`, убедиться, что все контейнеры живы (`docker compose ps`), Airflow открывается.
2. Проверить, что `weather_pipeline` отработал: в stg.cities_raw 10 городов, в stg.forecast_raw копятся строки.
   Если какой-то город не нашёлся в Geocoding — поправить имя в config.CITIES.
3. Запустить `weather_backfill` и проверить, что Previous Runs API принимает список переменных
   (если 400 — уменьшить набор переменных или chunk).
4. **Сбор запустить как можно раньше** — каждый день задержки = минус день истории.
5. Отдать ролям 2 и 3 контракт stage (раздел выше) и инструкцию по запуску.
6. Подготовиться к защите: уметь объяснить оркестрацию, расписание, ретраи, идемпотентность, очистку stage.

## Известные моменты
- Каркас не запускался: в облачной песочнице не было docker и доступа к open-meteo. Возможны мелкие баги.
- На Linux у томов могут быть проблемы с правами для ./dags — при необходимости `AIRFLOW_UID=$(id -u)` и `user:` в compose.
- Metabase образ `latest` — при проблемах закрепить версию.
- Код и комментарии на русском, это учебный проект.
