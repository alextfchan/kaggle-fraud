from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from src.process.utils.pathreader import pathreader
import yaml


def analysis(spark: SparkSession, filtered_data: DataFrame, file_path: dict) -> dict:
    '''
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
    '''


    analysis=filtered_data.select("Time", "V4", "Amount", "Class")

    quantile = analysis.approxQuantile("V4", [0.25, 0.75], 0)

    high_bucket = analysis.filter(col("V4") > quantile[1])
    medium_bucket = analysis.filter((col("V4") <= quantile[1]) & (col("V4") >= quantile[0]))
    low_bucket = analysis.filter(col("V4") < quantile[0])
    
    return_dict = {"high_bucket": high_bucket, "medium_bucket": medium_bucket, "low_bucket": low_bucket}

    output_path = file_path["file_path"]["paths"]["output"]

    for k,v in return_dict.items():
        v.toPandas().to_parquet(output_path + k + ".parquet")

    with open("config.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)

    config["paths"]["high_bucket_path"] = output_path + "high_bucket.parquet"
    config["paths"]["medium_bucket_path"] = output_path + "medium_bucket.parquet"
    config["paths"]["low_bucket_path"] = output_path + "low_bucket.parquet"

    with open("config.yaml", "w") as config_file:
        yaml.safe_dump(config, config_file)

    return return_dict


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Analysis").getOrCreate()

    complete_data = pathreader(spark, "config.yaml", "complete_data")
    filtered_data = outlier_detection(spark, complete_data)

    result = analysis(spark,
                      filtered_data["filtered_data"],
                      complete_data) 
    spark.stop()
