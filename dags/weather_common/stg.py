"""Запись в stage-слой и чтение справочника городов."""

from __future__ import annotations

import json
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from weather_common import config


def hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=config.PG_CONN_ID)


def loaded_city_queries() -> set[str]:
    rows = hook().get_records("SELECT DISTINCT city_query FROM stg.cities_raw")
    return {r[0] for r in rows}


def insert_city(city_query: str, country_code: str, payload: dict, selected: dict) -> None:
    hook().run(
        """
        INSERT INTO stg.cities_raw (city_query, country_code, payload, selected)
        VALUES (%s, %s, %s::jsonb, %s::jsonb)
        """,
        parameters=(city_query, country_code, json.dumps(payload), json.dumps(selected)),
    )


def get_cities() -> list[dict]:
    """Последняя загруженная версия каждого города из конфига."""
    names = [c["name"] for c in config.CITIES]
    rows = hook().get_records(
        """
        SELECT DISTINCT ON (city_query)
               city_query,
               (selected->>'latitude')::float,
               (selected->>'longitude')::float
        FROM stg.cities_raw
        WHERE city_query = ANY(%s)
        ORDER BY city_query, loaded_at DESC
        """,
        parameters=(names,),
    )
    return [{"name": r[0], "latitude": r[1], "longitude": r[2]} for r in rows]


def insert_forecasts(
    batch_id: str, source: str, cities: list[dict], params: dict, payloads: list[dict]
) -> int:
    rows = [
        (
            batch_id,
            source,
            city["name"],
            city["latitude"],
            city["longitude"],
            json.dumps(params),
            json.dumps(payload),
        )
        for city, payload in zip(cities, payloads)
    ]
    conn = hook().get_conn()
    try:
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO stg.forecast_raw
                    (batch_id, source, city_query, latitude, longitude, request_params, payload)
                VALUES %s
                """,
                rows,
                template="(%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)",
            )
    finally:
        conn.close()
    return len(rows)


def cleanup(retention_days: int) -> int:
    conn = hook().get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM stg.forecast_raw WHERE loaded_at < now() - make_interval(days => %s)",
                (retention_days,),
            )
            return cur.rowcount
    finally:
        conn.close()


def has_sql(text: str) -> bool:
    """Есть ли в файле что-то кроме комментариев и пустых строк."""
    for line in text.splitlines():
        s = line.strip()
        if s and not s.startswith("--"):
            return True
    return False


def run_sql_file(filename: str) -> None:
    """Выполняет SQL-файл из папки sql/. Пустой файл (только комментарии) — пропуск задачи."""
    path = Path(config.SQL_DIR) / filename
    if not path.exists():
        raise AirflowSkipException(f"{filename} ещё не создан")
    text = path.read_text(encoding="utf-8")
    if not has_sql(text):
        raise AirflowSkipException(f"{filename} пока пустой — пропускаю")
    hook().run(text)  # весь файл одной транзакцией
