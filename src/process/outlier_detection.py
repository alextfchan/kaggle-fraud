from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, col
import pandas as pd



def outlier_detection(spark: SparkSession, complete_data: str) -> dict:
    '''
    Function gets data from a CSV file, filters out outliers (focusing on Col: V4), then exports the outliers and remaining data into two parquet files.
    
    Parameters
    ----------
    complete_data: str
        Path to the CSV file

    Output
    ------
    outliers.parquet: Parquet file
        Data with V4 values filtered for outliers for human processing
    remaining.csv: CSV file
        Data with V4 values filtered for non-outliers for further processing
    
    Return
    ------
    dict
        A dictionary containing the outliers and remaining data as DataFrames.
    '''

    df = spark.read.csv(complete_data, header=True, inferSchema=True) #<- remove
    # selection = df.select("Time", "V4", "Amount", "Class")

    # Remove NULL values
    df.na.drop(subset=["V4"])

    # Remove extreme outliers (2 standard deviations from the mean)
    m = df.agg(mean("V4")).collect()[0][0]
    s = df.agg(stddev("V4")).collect()[0][0]

    high = m + (2*s)
    low = m - (2*s)

    # Filter out the outliers for human processing
    outliers = df.filter((col("V4") >= high) | (col("V4") <= low)) #maybe this should only show column V4.
    outliers.toPandas().to_parquet("data/anomalies/outliers.parquet") #<- remove
    # print("Outliers exported to data/anomalies/outliers.parquet")

    # Collect remaining data for futher processing
    filtered_data = df.filter((col("V4") < high) & (col("V4") > low))
    filtered_data.toPandas().to_csv("data/filtered_data.csv") #<- remove
    # print("Remaining data exported to data/output/remaining.csv")
    
    
    return {"outliers": outliers, "remaining": filtered_data}


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Outlier Detection").getOrCreate()
    result = outlier_detection(spark, "data/creditcard.csv")
    spark.stop()
    print(result)