from pyspark.sql import SparkSession, DataFrame
import pandas as pd


def aggregate(spark: SparkSession, complete_data) -> DataFrame:
    '''
    Function gets data from a CSV file, aggregates it, then exports the results in a parquet file.

    Parameters
    ----------
    spark: SparkSession
        spark session required to run and process the data
    complete_data: str
        path to the CSV file

    Output
    ------
    Parquet file:
        aggregated data from the CSV file

    Return
    ------
    summary: DataFrame
        summary of the aggregated data
    '''
    # session = spark
    # spark = SparkSession.builder.appName("Aggregate").getOrCreate()

    df = spark.read.csv(complete_data, header=True, inferSchema=True)
    summary = df.describe()

    export = summary.toPandas()

    export.to_csv("data/output/aggregate.csv")
    # export.to_parquet("data/output/aggregate.parquet")
    
    # spark.stop()
    
    return summary


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Aggregate").getOrCreate()
    result = aggregate(spark, "data/creditcard.csv")
    spark.stop()
    print(result)