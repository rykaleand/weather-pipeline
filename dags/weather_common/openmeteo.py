"""Клиент Open-Meteo: геокодинг, прогноз, прошлые прогнозы."""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from weather_common import config

TIMEOUT = 60


def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def _get(url: str, params: dict) -> dict | list:
    resp = _session().get(url, params=params, timeout=TIMEOUT)
    if resp.status_code == 400:
        # Open-Meteo в ответе объясняет, что не так с параметрами
        raise ValueError(f"Open-Meteo 400: {resp.text}")
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------- geocoding

def geocode(name: str, country_code: str) -> tuple[dict, dict]:
    """Возвращает (полный ответ API, выбранный результат)."""
    params = {"name": name, "count": 10, "language": "ru", "format": "json"}
    payload = _get(config.GEOCODING_URL, params)
    selected = pick_city(payload, country_code)
    if selected is None:
        raise ValueError(f"Город {name} ({country_code}) не найден в Geocoding API")
    return payload, selected


def pick_city(payload: dict, country_code: str) -> dict | None:
    """Первый (самый релевантный) результат в нужной стране."""
    for r in payload.get("results") or []:
        if r.get("country_code", "").upper() == country_code.upper():
            return r
    return None


# ---------------------------------------------------------------- forecast

def _coords(cities: list[dict]) -> dict:
    return {
        "latitude": ",".join(f"{c['latitude']:.4f}" for c in cities),
        "longitude": ",".join(f"{c['longitude']:.4f}" for c in cities),
    }


def split_locations(payload: dict | list, n: int) -> list[dict]:
    """При нескольких координатах API возвращает список, при одной — объект."""
    items = payload if isinstance(payload, list) else [payload]
    if len(items) != n:
        raise ValueError(f"Ожидали ответ по {n} точкам, пришло {len(items)}")
    return items


def forecast_params() -> dict:
    return {
        "hourly": ",".join(config.FORECAST_HOURLY_VARS),
        "forecast_days": config.FORECAST_DAYS,
        "past_days": config.PAST_DAYS,
        "timezone": "GMT",  # все времена в UTC
    }


def fetch_forecast(cities: list[dict]) -> tuple[dict, list[dict]]:
    """Один запрос по всем городам. Возвращает (параметры, ответы по городам в том же порядке)."""
    params = forecast_params()
    payload = _get(config.FORECAST_URL, {**_coords(cities), **params})
    return params, split_locations(payload, len(cities))


def backfill_params(start_date: str, end_date: str) -> dict:
    hourly = [
        f"{v}_previous_day{d}"
        for v in config.BACKFILL_HOURLY_VARS
        for d in config.BACKFILL_LEAD_DAYS
    ]
    return {
        "hourly": ",".join(hourly),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "GMT",
    }


def fetch_previous_runs(cities: list[dict], start_date: str, end_date: str) -> tuple[dict, list[dict]]:
    """Прогнозы, сделанные за 0..7 дней до каждого часа, за период [start_date, end_date]."""
    params = backfill_params(start_date, end_date)
    payload = _get(config.PREVIOUS_RUNS_URL, {**_coords(cities), **params})
    return params, split_locations(payload, len(cities))
