from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

# process overwrites current file. would be good to fix that.

def analysis(spark: SparkSession, filtered_data: str) -> dict:
    '''
    Function gets data from a CSV file, splits the results into 3 buckets (High, Medium, Low).  Then exports the results in a parquet file.

    Parameters
    ----------
    filtered_data: str
        Path to the CSV file

    Output
    ------
    high_bucket.parquet: Parquet file
        Data with V4 values filtered for the top 25% interquartile range
    medium_bucket.parquet: Parquet file
        Data with V4 values filtered for the middle 50% interquartile range
    low_bucket.parquet: Parquet file
        Data with V4 values filtered for the bottom 25% interquartile range

    Return
    ------
    dict
        A dictionary containing the high, medium, and low buckets as DataFrames.
    '''

    df = spark.read.csv(filtered_data, header=True, inferSchema=True)

    analysis=df.select("Time", "V4", "Amount", "Class")

    quantile = analysis.approxQuantile("V4", [0.25, 0.75], 0)

    high_bucket = analysis.filter(col("V4") > quantile[1])
    medium_bucket = analysis.filter((col("V4") <= quantile[1]) & (col("V4") >= quantile[0]))
    low_bucket = analysis.filter(col("V4") < quantile[0])

    # export to CSV
    # high_bucket.toPandas().to_csv("data/output/high_bucket.csv")
    # medium_bucket.toPandas().to_csv("data/output/medium_bucket.csv")
    # low_bucket.toPandas().to_csv("data/output/low_bucket.csv")

    # export to parquet
    high_bucket.toPandas().to_parquet("data/output/high_bucket.parquet")
    medium_bucket.toPandas().to_parquet("data/output/medium_bucket.parquet")
    low_bucket.toPandas().to_parquet("data/output/low_bucket.parquet")

    return {"high_bucket": high_bucket, "medium_bucket": medium_bucket, "low_bucket": low_bucket}


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Analysis").getOrCreate()
    result = analysis(spark, "data/filtered_data.csv") 
    # result = analysis(spark, "data/creditcard.csv")
    spark.stop()