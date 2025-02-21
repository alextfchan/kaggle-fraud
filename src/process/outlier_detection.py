from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, col
from src.process.utils.pathreader import pathreader
import yaml


def outlier_detection(spark: SparkSession, complete_data: dict) -> dict:
    '''
    Function gets data from a CSV file, filters out outliers (focusing on Col: V4), then exports the outliers and remaining data into two parquet files.
    
    Parameters
    ----------
    complete_data: Dict{Dataframe, file paths}
        DataFrame containing the data from the CSV file, using the pathreader util function
        File Paths for the output files (source: config.yaml)

    Output
    ------
    outliers.parquet: Parquet file
        Data with V4 values filtered for outliers for human processing
    remaining.csv: CSV file
        Data with V4 values filtered for non-outliers for further processing
    
    Return
    ------
    dict
        - A dictionary containing the outliers and remaining data as DataFrames, as well as the paths to the output files
    '''

    df = complete_data["dataframe"]
    file_path = complete_data["file_path"]

    # Remove NULL values
    df.na.drop(subset=["V4"])

    # Remove extreme outliers (3 standard deviations from the mean)
    m = df.agg(mean("V4")).collect()[0][0]
    s = df.agg(stddev("V4")).collect()[0][0]

    high = m + (3*s)
    low = m - (3*s)

    # Filter out the outliers for human processing
    outliers = df.filter((col("V4") >= high) | (col("V4") <= low))
    outliers_path = file_path["paths"]["anomalies"]+"outliers.parquet"
    outliers.toPandas().to_parquet(outliers_path)

    # Collect remaining data for futher processing
    filtered_data = df.filter((col("V4") < high) & (col("V4") > low))
    filtered_data_path = file_path["data"]["root"]+"filtered_data.csv"
    filtered_data.toPandas().to_csv(filtered_data_path)

    # Updating the config file with the paths of the new files
    with open(complete_data["config_file"], "r") as file:
        config = yaml.safe_load(file)
    
    config["data"]["filtered_data_path"] = filtered_data_path
    config["paths"]["outliers_path"] = outliers_path

    with open(complete_data["config_file"], "w") as file:
        yaml.safe_dump(config, file)


    return {"outliers_data": outliers,
            "outlier_path": outliers_path,
            "filtered_data": filtered_data,
            "filtered_path": filtered_data_path,}


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Outlier Detection").getOrCreate()
    complete_data = pathreader(spark, "config.yaml", "complete_data")
    result = outlier_detection(spark, complete_data)
    spark.stop()
  