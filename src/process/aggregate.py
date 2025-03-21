from pyspark.sql import SparkSession, DataFrame
from src.process.utils.exporter import to_parquet
import logging
# from outlier_detection import outlier_detection  #just for testing


logger = logging.getLogger()


def aggregate(spark: SparkSession, filtered_data: DataFrame, file_path: dict) -> dict:
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
        file_path = file_path["file_path"]["paths"]["anomalies"] + "aggregate.parquet"

        # DataFrame: Aggregated data
        return_aggregate = filtered_data.select(
            "Time", "V4", "Amount", "Class"
        ).describe()

        # Exporting data into parquet format
        to_parquet(return_aggregate, file_path)

    except Exception as e:
        logger.exception(f"Error aggregating data: {e}")

    return return_aggregate


# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("Aggregate").getOrCreate()

#     complete_data = pathreader(spark, "config.yaml", "complete_data")
#     filtered_data = outlier_detection(spark, complete_data)

#     result = aggregate(spark,
#                        filtered_data["filtered_data"],
#                        complete_data)
#     spark.stop()
#     print(result)
