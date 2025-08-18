import logging

import yaml
from polars import DataFrame

from src.ingestion.get_weather_connection import connection
from src.ingestion.get_weather_data import get_hourly_data
from src.utils.exporter import to_parquet

logger = logging.getLogger()


def load_raw_data(raw_data: DataFrame = None, config: str = "config.yaml"):
    """
    Function loads DataFrame with raw data into parquet format.
    """

    try:
        with open(config, "r") as f:
            file_path = yaml.safe_load(f)

        file_path = file_path["data"]["raw"] + "raw_data.parquet"
        to_parquet(raw_data, file_path)

    except Exception as e:
        logger.exception(f"Unexcepted Error: {e}")


if __name__ == "__main__":
    responses = connection()

    # print(config["file_path"])

    data_hourly = get_hourly_data(responses)
    # data_daily = get_daily_data(responses)
    result = load_raw_data(data_hourly)
    # result = load_raw_data(data_daily)
    print("result: ", result, data_hourly)
