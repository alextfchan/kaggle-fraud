from enum import Enum


class WeatherParams(Enum):
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    DAILY = "daily"
    HOURLY = "hourly"
    CURRENT = "current"
    TIMEZONE = "timezone"


class LondonSettings(Enum):
    LATITUDE = 51.5085
    LONGITUDE = -0.1257
    HOURLY = "temperature_2m"
    CURRENT = "temperature_2m"
    TIMEZONE = "Europe/London"


class WeatherDailyVariables(Enum):
    TEMPMAX = "temperature_2m_max"
    TEMPMIN = "temperature_2m_min"
    TEMPMEAN = "temperature_2m_mean"


class WeatherConn(Enum):
    CACHEEXPIRY = 3600
    RETRIES = 5


# params = {
#     WeatherParams.LATITUDE.value: LondonSettings.LATITUDE.value,
#     WeatherParams.LONGITUDE.value: LondonSettings.LONGITUDE.value,
#     WeatherParams.DAILY.value: [param.value for param in WeatherVariables],
#     WeatherParams.HOURLY.value: LondonSettings.HOURLY.value,
#     WeatherParams.CURRENT.value: LondonSettings.CURRENT.value,
#     WeatherParams.TIMEZONE.value: LondonSettings.TIMEZONE.value,
# }
