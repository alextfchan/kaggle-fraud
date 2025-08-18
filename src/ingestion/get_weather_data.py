import polars as pl

# from openmeteo_sdk.Variable import Variable
from src.ingestion.get_weather_connection import connection


def get_hourly_data(connection: list):
    locations = list(range(len(responses)))

    for i in locations:
        hourly = responses[i].Hourly()
        hourly_temperature_2m = hourly.Variables(i).ValuesAsNumpy()

        # Process too extra. Fix this.
        s = pl.Series([hourly.Time(), hourly.TimeEnd()])
        times = pl.from_epoch(s, time_unit="s")

        hourly_times = pl.datetime_range(
            start=times[0],
            end=times[1],
            interval=str(hourly.Interval()) + "s",
            closed="left",
            eager="True",
        ).alias("date")

        hourly_dataframe = pl.select(
            date=hourly_times,
            temperature_2m=hourly_temperature_2m,
        )

    return hourly_dataframe


def get_daily_data(connection: list):
    locations = list(range(len(responses)))

    for i in locations:
        daily = responses[i].Daily()

        s = pl.Series([daily.Time(), daily.TimeEnd()])
        times = pl.from_epoch(s, time_unit="s")

        daily_times = pl.datetime_range(
            start=times[0],
            end=times[1],
            interval=str(daily.Interval()) + "s",
            closed="left",
            eager="True",
        ).alias("date")

        daily_dataframe = pl.select(
            date=daily_times,
            temperature_2m_max=daily.Variables(0).ValuesAsNumpy(),
            temperature_2m_min=daily.Variables(1).ValuesAsNumpy(),
            temperature_2m_mean=daily.Variables(2).ValuesAsNumpy(),
        )

    return daily_dataframe


if __name__ == "__main__":
    responses = connection()
    # get_hourly_data(responses)
    result = get_daily_data(responses)
    print(result)
