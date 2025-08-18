import logging

from polars import DataFrame, col

from src.process.outlier_detection import outlier_detection
from src.utils.exporter import to_parquet
from src.utils.pathreader import pathreader

logger = logging.getLogger()


def analysis(filtered_data: DataFrame, file_path: dict) -> dict:
    """
    Function receives a DataFrame, filters out the V4 values for the top 25%, middle 50%, and bottom 25% interquartile range, then exports these 3 buckets into parquet files.

    Parameters
    ----------
    filtered_data: DataFrame
        DataFrame containing the filtered data
    file_path: dict
        File Paths for the output files (source: config.yaml)

    Output
    ------
    high_bucket.parquet: Parquet file
        Data with V4 values filtered for the top 25% interquartile range
    medium_bucket.parquet: Parquet file
        Data with V4 values filtered for the middle 50% interquartile range
    low_bucket.parquet: Parquet file
        Data with V4 values filtered for the bottom 25% interquartile range
    Updates the config file with the new paths in config.yaml

    Return
    ------
    dict
        A dictionary containing the high, medium, and low buckets as DataFrames.
    """

    return_analysis = {"high_bucket": None, "medium_bucket": None, "low_bucket": None}

    if not isinstance(filtered_data, DataFrame):
        logger.error("Filtered Data was not found.")
        return return_analysis

    try:
        analysis = filtered_data.select("Time", "V4", "Amount", "Class")

        # Separating results into buckets
        quantile = [
            analysis["V4"].quantile(0.25, "nearest"),
            analysis["V4"].quantile(0.75, "nearest"),
        ]

        return_analysis["high_bucket"] = analysis.filter(col("V4") > quantile[1])
        return_analysis["medium_bucket"] = analysis.filter(
            (col("V4") <= quantile[1]) & (col("V4") >= quantile[0])
        )
        return_analysis["low_bucket"] = analysis.filter(col("V4") < quantile[0])

        # Exporting buckets into parquet format
        output_path = file_path["file_path"]["paths"]["output"]
        for k, v in return_analysis.items():
            to_parquet(v, file_path=output_path + k + ".parquet")

    except Exception as e:
        logger.exception(f"Unexpected Error: {e}")

    logger.info("Data Analysis has been completed.")

    return return_analysis


if __name__ == "__main__":
    complete_data = pathreader("config.yaml", "complete_data")
    filtered_data = outlier_detection(complete_data)

    result = analysis(filtered_data["filtered_data"], complete_data)
    print(result)
