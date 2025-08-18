import os

import openmeteo_requests
import requests_cache
from dotenv import load_dotenv
from retry_requests import retry

from src.classes.enums import (
    LondonSettings,
    WeatherConn,
    WeatherDailyVariables,
    WeatherParams,
)

load_dotenv()
api_key = os.getenv("WEATHER_API")


def connection() -> list:
    cache_session = requests_cache.CachedSession(
        ".cache", expire_after=WeatherConn.CACHEEXPIRY.value
    )
    retry_session = retry(
        cache_session, retries=WeatherConn.RETRIES.value, backoff_factor=0.2
    )
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = api_key

    params = {
        WeatherParams.LATITUDE.value: LondonSettings.LATITUDE.value,
        WeatherParams.LONGITUDE.value: LondonSettings.LONGITUDE.value,
        WeatherParams.DAILY.value: [v.value for v in WeatherDailyVariables],
        WeatherParams.HOURLY.value: LondonSettings.HOURLY.value,
        WeatherParams.CURRENT.value: LondonSettings.CURRENT.value,
        WeatherParams.TIMEZONE.value: LondonSettings.TIMEZONE.value,
    }

    responses = openmeteo.weather_api(url, params=params)

    return responses


if __name__ == "__main__":
    connection()
