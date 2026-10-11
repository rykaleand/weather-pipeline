"""
Бэкфилл истории из Previous Runs API. Запускается вручную (Trigger DAG w/ config).

Для каждого часа в периоде API отдаёт значения, предсказанные за 0..7 суток до него:
temperature_2m_previous_day0 (≈ факт), ..._previous_day1 (прогноз за 24 ч), ... day7.
Так можно сразу получить данные для графика «ошибка от горизонта», не дожидаясь,
пока ежечасный сбор накопит неделю истории.
"""

from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

from weather_common import config


def date_chunks(start: date, end: date, size: int) -> list[tuple[str, str]]:
    chunks = []
    cur = start
    while cur <= end:
        stop = min(cur + timedelta(days=size - 1), end)
        chunks.append((cur.isoformat(), stop.isoformat()))
        cur = stop + timedelta(days=1)
    return chunks


@dag(
    dag_id="weather_backfill",
    description="Догрузка истории прогнозов (Previous Runs API) в stage",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "days_back": Param(30, type="integer", minimum=1, maximum=365,
                           description="Сколько дней истории загрузить (до вчера включительно)"),
    },
    tags=["weather", "ppad"],
)
def weather_backfill():

    @task
    def load_history(params=None, run_id=None) -> int:
        from weather_common import openmeteo, stg

        cities = stg.get_cities()
        if not cities:
            raise ValueError("Сначала должен отработать weather_pipeline (задача ensure_cities)")

        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=int(params["days_back"]) - 1)
        total = 0
        for chunk_start, chunk_end in date_chunks(start, end, config.BACKFILL_CHUNK_DAYS):
            req, payloads = openmeteo.fetch_previous_runs(cities, chunk_start, chunk_end)
            total += stg.insert_forecasts(run_id, "previous_runs", cities, req, payloads)
        return total

    load_history()


weather_backfill()
