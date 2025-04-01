import logging
from polars import DataFrame, col
from src.process.utils.exporter import to_parquet
from src.process.outlier_detection import outlier_detection
from src.process.utils.pathreader import pathreader


logger = logging.getLogger()


def aggregate(filtered_data: DataFrame, file_path: dict) -> DataFrame:
    """
    Function gets data from a CSV file, aggregates it, then exports the results in a parquet file.

    Parameters
    ----------
    spark: SparkSession
        spark session required to run and process the data
    filtered_data: DataFrame
        DataFrame containing the filtered data
    file_path: dict
        File Paths for the output files (source: config.yaml)

    Output
    ------
    Parquet file:
        aggregated data from the CSV file
    Updates the config file with the new paths in config.yaml

    Return
    ------
    dict
        summary of the aggregated data as a DataFrame, and the path to the output file
    """

    return_aggregate = None

    if not isinstance(filtered_data, DataFrame):
        logger.error("Filtered Data was not found.")
        return return_aggregate

    try:
        # File Path for the aggregated data
        output_path = file_path["file_path"]["paths"]["anomalies"] + "aggregate.parquet"

        # DataFrame: Aggregated data
        return_aggregate = filtered_data.select("Time", "V4", "Amount", "Class").describe()

        # Exporting data into parquet format
        to_parquet(return_aggregate, output_path)

    except Exception as e:
        logger.exception(f"Error aggregating data: {e}")

    logger.info("Data aggregation has been completed.")

    return return_aggregate


if __name__ == "__main__":
    complete_data = pathreader("config.yaml", "complete_data")
    filtered_data = outlier_detection(complete_data)

    result = aggregate(filtered_data["filtered_data"],
                       complete_data)
    print(result)
