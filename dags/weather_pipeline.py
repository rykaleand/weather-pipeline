"""
Основной конвейер: раз в час

    ensure_cities ─► load_forecast ─► stg_to_dds ─► dds_to_cdm ─► cleanup_stg
       (роль 1)        (роль 1)        (роль 2)      (роль 3)       (роль 1)

stg_to_dds и dds_to_cdm выполняют файлы sql/dds_load.sql и sql/cdm_load.sql.
Пока файлы пустые, задачи помечаются как skipped, а конвейер продолжает работать.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from weather_common import config


@dag(
    dag_id="weather_pipeline",
    description="Open-Meteo: прогноз → stg → dds → cdm",
    schedule="5 * * * *",  # каждый час в :05
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["weather", "ppad"],
)
def weather_pipeline():

    @task
    def ensure_cities() -> int:
        """Загружает в stage города из конфига, которых там ещё нет."""
        from weather_common import openmeteo, stg

        known = stg.loaded_city_queries()
        added = 0
        for city in config.CITIES:
            if city["name"] in known:
                continue
            payload, selected = openmeteo.geocode(city["name"], city["country_code"])
            stg.insert_city(city["name"], city["country_code"], payload, selected)
            added += 1
        return added

    @task
    def load_forecast(run_id=None) -> int:
        """Один запрос по всем городам, ответы — в stg.forecast_raw."""
        from weather_common import openmeteo, stg

        cities = stg.get_cities()
        if not cities:
            raise ValueError("В stg.cities_raw нет городов")
        params, payloads = openmeteo.fetch_forecast(cities)
        return stg.insert_forecasts(run_id, "forecast", cities, params, payloads)

    @task(trigger_rule="none_failed")
    def stg_to_dds():
        from weather_common import stg
        stg.run_sql_file("dds_load.sql")

    @task(trigger_rule="none_failed")
    def dds_to_cdm():
        from weather_common import stg
        stg.run_sql_file("cdm_load.sql")

    @task(trigger_rule="none_failed")
    def cleanup_stg() -> int:
        from weather_common import stg
        return stg.cleanup(config.STG_RETENTION_DAYS)

    ensure_cities() >> load_forecast() >> stg_to_dds() >> dds_to_cdm() >> cleanup_stg()


weather_pipeline()
