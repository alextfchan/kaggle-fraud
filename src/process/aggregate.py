from pyspark.sql import SparkSession, DataFrame
from src.process.utils.pathreader import pathreader
import pandas as pd
import yaml
# from outlier_detection import outlier_detection  #just for testing


def aggregate(spark: SparkSession, filtered_data: DataFrame, file_path: dict) -> dict:
    '''
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
    '''

    aggregate_path = file_path["file_path"]["paths"]["anomalies"]+"aggregate.parquet"

    summary = filtered_data.describe()

    export = summary.toPandas()
    export.to_parquet(aggregate_path)

    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    
    config["paths"]["aggregate_path"] = aggregate_path

    with open("config.yaml", "w") as file:
        yaml.safe_dump(config, file)
        
    return {"summary": summary, "aggregate_path": aggregate_path}


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Aggregate").getOrCreate()

    complete_data = pathreader(spark, "config.yaml", "complete_data")
    filtered_data = outlier_detection(spark, complete_data)

    result = aggregate(spark,
                       filtered_data["filtered_data"],
                       complete_data)
    spark.stop()
    print(result)
