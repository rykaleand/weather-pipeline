"""Настройки пайплайна. Всё, что может захотеться поменять, — здесь."""

# Подключение к хранилищу (создаётся из AIRFLOW_CONN_PG_WAREHOUSE в docker-compose)
PG_CONN_ID = "pg_warehouse"

# Папка с SQL внутри контейнера Airflow (смонтирована из ./sql)
SQL_DIR = "/opt/airflow/sql"

# Города: имя для поиска в Geocoding API (латиницей) + код страны для однозначности.
# Разные климаты, чтобы было что сравнивать на дашборде.
CITIES = [
    {"name": "Moscow", "country_code": "RU"},
    {"name": "Saint Petersburg", "country_code": "RU"},
    {"name": "Kazan", "country_code": "RU"},
    {"name": "Yekaterinburg", "country_code": "RU"},
    {"name": "Novosibirsk", "country_code": "RU"},
    {"name": "Vladivostok", "country_code": "RU"},
    {"name": "Sochi", "country_code": "RU"},
    {"name": "Murmansk", "country_code": "RU"},
    {"name": "Almaty", "country_code": "KZ"},
    {"name": "Astana", "country_code": "KZ"},
]

# Эндпоинты Open-Meteo (ключ не нужен)
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"

# Почасовые переменные ежечасного прогноза
FORECAST_HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "precipitation_probability",
    "wind_speed_10m",
    "cloud_cover",
    "weather_code",
]
FORECAST_DAYS = 7  # горизонт прогноза
PAST_DAYS = 2      # прошедшие дни — из них берётся «факт»

# Переменные для бэкфилла (Previous Runs API поддерживает не всё, например нет precipitation_probability)
BACKFILL_HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "cloud_cover",
    "weather_code",
]
BACKFILL_LEAD_DAYS = range(0, 8)  # _previous_day0 ... _previous_day7
BACKFILL_CHUNK_DAYS = 14          # размер одного запроса по датам

# Сколько дней хранить сырые ответы в stage (история версий живёт в dds)
STG_RETENTION_DAYS = 30
