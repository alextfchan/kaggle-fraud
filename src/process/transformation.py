from polars import DataFrame

from src.ingestion.get_weather_data import get_daily_data

# This will be to put weather data into parquet format.


def transform_weather_data(dataframe: DataFrame) -> None:
    print("hello")

    return None


if __name__ == "__main__":
    responses = connection()
    result = get_daily_data(responses)
    transform = transform_weather_data(result)
    print(result)
