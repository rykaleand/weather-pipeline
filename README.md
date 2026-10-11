# Насколько можно верить прогнозу погоды?

ППАД, задание 3 «Конвейер обработки данных».

Каждый час забираем из [Open-Meteo](https://open-meteo.com/) прогноз на 7 дней вперёд для 10 городов.
Прогноз на один и тот же час пересчитывается много раз — храним все версии (SCD2 в dds),
а когда час наступил, сравниваем версии с фактом и считаем ошибку прогноза в зависимости от того,
за сколько дней он был сделан.

```
Open-Meteo ──► stg (сырой JSON) ──► dds (строки + SCD2) ──► cdm (витрины) ──► Metabase
   роль 1            роль 1              роль 2               роль 3          роль 3
```

| Роль | Что делает | Файлы |
|---|---|---|
| 1 | stage, оркестрация, сбор данных, docker compose | `dags/`, `sql/init/`, `docker-compose.yml` — **готово** |
| 2 | dds + перенос stg → dds с SCD2 | `sql/dds_load.sql` |
| 3 | cdm + перенос dds → cdm + дашборд в Metabase | `sql/cdm_load.sql` |

## Быстрый старт

Нужен запущенный Docker Desktop.

```bash
git clone https://github.com/rykaleand/weather-pipeline.git
cd weather-pipeline
docker compose up -d
```

Первый запуск качает образы — несколько минут. Дальше:

| Сервис | Адрес | Доступ |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3333 | при первом входе зарегистрироваться |
| Хранилище (DBeaver, psql) | `localhost:15432`, БД `de` | jovyan / jovyan |

Подключение Metabase к хранилищу: тип PostgreSQL, host `warehouse`, port `5432`, БД `de`, jovyan / jovyan
(именно `warehouse`, а не `localhost` — Metabase живёт внутри docker-сети).

Через пару минут после старта Airflow сам загрузит 10 городов и первый прогноз.

## Нужно ли запускать DAG'и

| DAG | Запуск | Что делает |
|---|---|---|
| `weather_pipeline` | **сам**, каждый час в :05 | `ensure_cities → load_forecast → stg_to_dds → dds_to_cdm → cleanup_stg` |
| `weather_backfill` | **вручную, один раз** после первого `docker compose up -d` | загружает 30 дней истории прогнозов (Previous Runs API) |

- `weather_pipeline` включается автоматически, ничего нажимать не нужно. Работает, пока запущен Docker.
  Пропущенные часы (ноутбук был выключен) не догоняются — `catchup=False`.
- `weather_backfill`: в Airflow → DAG `weather_backfill` → Trigger. Можно с конфигом `{"days_back": 90}`,
  по умолчанию 30 дней. Его же можно перезапустить, чтобы закрыть дыры после простоя.
- Кнопка Trigger у `weather_pipeline` нужна только для проверки своего SQL сразу, не дожидаясь :05.

## Как работать ролям 2 и 3

1. Поднять стенд (см. «Быстрый старт») и один раз запустить `weather_backfill`.
2. Писать свой SQL **только в своём файле**:
   - роль 2 — `sql/dds_load.sql`
   - роль 3 — `sql/cdm_load.sql`

   DDL держать в начале этого же файла (`CREATE TABLE IF NOT EXISTS ...`): скрипты `sql/init/`
   выполняются только при первом создании БД, а эти файлы — при каждом запуске DAG'а.
   Файл выполняется целиком одной транзакцией. Пока в нём только комментарии, задача помечается skipped
   и конвейер идёт дальше.
3. Проверка: Airflow → `weather_pipeline` → Trigger. Ваша задача (`stg_to_dds` или `dds_to_cdm`)
   должна стать зелёной (success). Логи — клик по задаче → Logs.
4. Готовый SQL — закоммитить и запушить. Python и DAG'и трогать не нужно.

Требования к SQL:
- **Повторный запуск не должен плодить дубли** (DAG крутится каждый час). Роль 2 — watermark по
  `stg.forecast_raw.id`; роль 3 — проще всего `TRUNCATE` витрины + `INSERT ... SELECT`.
- Витрины cdm строятся **только из dds**, не из stg (требование задания).

**Роль 3 зависит от таблиц роли 2.** Чтобы не ждать друг друга, договоритесь о названиях таблиц и
колонок dds заранее — за основу можно взять раздел «Задумка dds и cdm» ниже.

## Контракт stage

### stg.cities_raw — справочник городов (Geocoding API)

| Колонка | Тип | Описание |
|---|---|---|
| `id` | bigserial | |
| `city_query` | text | что искали, например `Moscow` — ключ для связи с forecast_raw |
| `country_code` | text | `RU`, `KZ` |
| `payload` | jsonb | полный ответ API |
| `selected` | jsonb | выбранный город |
| `loaded_at` | timestamptz | момент загрузки |

В `selected`: `name` (по-русски), `latitude`, `longitude`, `elevation`, `timezone`, `country`,
`country_code`, `population`, `admin1` (регион).
Нюанс: у Мурманска `name` = `Murmansk` (геокодер не отдаёт русское название).

### stg.forecast_raw — ответы Forecast API и Previous Runs API

| Колонка | Тип | Описание |
|---|---|---|
| `id` | bigserial | растёт с каждой загрузкой — для watermark |
| `batch_id` | text | run_id запуска DAG'а |
| `source` | text | `forecast` (ежечасно) или `previous_runs` (бэкфилл) |
| `city_query` | text | ссылка на `stg.cities_raw.city_query` |
| `latitude`, `longitude` | float | запрошенные координаты |
| `request_params` | jsonb | параметры запроса |
| `payload` | jsonb | ответ API по этому городу |
| `loaded_at` | timestamptz | момент загрузки |

Одна строка = один город в одной загрузке. Все времена в UTC.

**`source = 'forecast'`** — около 216 часов (2 прошедших дня + 7 вперёд):

```json
{"latitude": .., "longitude": .., "elevation": .., "utc_offset_seconds": 0,
 "hourly": {"time": ["2026-10-09T00:00", ...],
            "temperature_2m": [...], "relative_humidity_2m": [...], "precipitation": [...],
            "precipitation_probability": [...], "wind_speed_10m": [...], "cloud_cover": [...],
            "weather_code": [...]},
 "hourly_units": {...}}
```

- Массивы параллельные: i-й элемент каждой переменной относится к `time[i]`.
- **Версия прогноза = `loaded_at`**. Горизонт = `target_ts − loaded_at`.
- Факт для часа — значение из загрузки, сделанной после этого часа.

**`source = 'previous_runs'`** — бэкфилл, куски по 14 дней. Ключи вида
`hourly.temperature_2m_previous_day0 ... _previous_day7` (то же для `relative_humidity_2m`,
`precipitation`, `wind_speed_10m`, `cloud_cover`, `weather_code`; `precipitation_probability` нет).
`_previous_dayN` — значение, предсказанное за N суток до часа; `_previous_day0` ≈ факт.
Версия здесь задаётся не `loaded_at`, а lead-днём N.

Разворачивание JSON в строки:

```sql
SELECT f.city_query,
       t.ts::timestamp                                        AS target_ts,
       (f.payload->'hourly'->'temperature_2m'->>(t.i - 1)::int)::numeric AS temperature,
       f.loaded_at
FROM stg.forecast_raw f
CROSS JOIN LATERAL jsonb_array_elements_text(f.payload->'hourly'->'time')
     WITH ORDINALITY AS t(ts, i)
WHERE f.source = 'forecast';
```

## Задумка dds и cdm

dds (роль 2):
- `dds.dim_city` — из `stg.cities_raw.selected`.
- `dds.dim_weather_code` — расшифровка кодов WMO (0 ясно, 61 дождь, 71 снег, 95 гроза…).
- `dds.forecast_hourly` — `city_id, target_ts, temperature, humidity, precipitation,
  precipitation_probability, wind_speed, cloud_cover, weather_code, valid_from, valid_to,
  is_current, row_hash`. SCD2 по `(city_id, target_ts)`: значения изменились — старая версия
  закрывается (`valid_to`, `is_current = false`), вставляется новая.
- Бэкфилл — отдельная таблица `dds.forecast_by_lead (city_id, target_ts, lead_days, ...)`.

cdm (роль 3):
- `cdm.error_by_lead_time` — MAE и bias по городу / переменной / горизонту (главный график).
- `cdm.forecast_vs_actual` — факт vs прогноз за 24 / 72 / 168 ч.
- `cdm.forecast_revisions` — сколько раз и насколько менялся прогноз на час/день.
- `cdm.precip_hit_rate` — попадания / пропуски / ложные тревоги по осадкам.
- `cdm.city_daily_summary`.

Главный график «ошибка от горизонта» строится по бэкфиллу (30 дней × 8 горизонтов уже есть);
ежечасный сбор — для SCD2 и графика «как менялся прогноз».

## Перед защитой

Показываем на одном ноутбуке — у роли 1, там самая длинная история ежечасных версий.

1. Роли 2 и 3 запушили свой SQL.
2. На ноутбуке роли 1: `git pull`, затем Trigger у `weather_pipeline` (или дождаться :05) —
   проверить, что все задачи зелёные.
3. Дашборд Metabase хранится внутри Metabase, а не в репозитории — роль 3 пересобирает его
   на этом ноутбуке.

## Остановка и сброс

```bash
docker compose stop     # остановить, данные сохраняются
docker compose start    # продолжить
```

**Не запускайте `docker compose down -v`** — флаг `-v` удаляет тома вместе со всей собранной историей.
Без `-v` (`docker compose down`) данные сохраняются.
