from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pandas as pd
from process.utils.pathreader import pathreader
from src.process.outlier_detection import outlier_detection
from src.process.aggregate import aggregate
from src.process.analysis import analysis

def pipeline(spark: SparkSession, complete_data: dict) -> dict:
    '''
    Function that runs the entire pipeline.  The pipeline consists of the following steps:
    1. Outlier Detection
    2. Analysis
    3. Aggregate

    Parameters
    ----------
    spark: SparkSession
        spark session required to run and process the data
    complete_data: str
        uses the util function 'pathreader' to read the CSV file

    Output
    ------
    Parquet files:
        - anomalies.parquet: data with V4 values filtered for outliers for human processing
        - filtered_data.csv: data with V4 values filtered for non-outliers for further processing
        - high_bucket.parquet: data with V4 values filtered for the top 25% interquartile range
        - medium_bucket.parquet: data with V4 values filtered for the middle 50% interquartile range
        - low_bucket.parquet: data with V4 values filtered for the bottom 25% interquartile range
        - aggregate.parquet: A summary of the filtered data

    Return
    ------
    dict
        a dictionary containing the results in DataFrame format from the outlier detection, analysis, and aggregation processes
    '''
    try:
        # Step 1: Reading the CSV file
        # data = pathreader(spark, complete_data)

        # Step 1: Outlier Detection
        outlier_result = outlier_detection(spark,
                                           complete_data)
        # Step 2: Analysis
        analysis_result = analysis(spark,
                                   outlier_result["filtered_data"],
                                   complete_data)
        # Step 3: Aggregate
        aggregate_result = aggregate(spark,
                                     outlier_result["filtered_data"],
                                     complete_data)
    except AnalysisException as ae:
        print("Analysis Exception: ", ae) # normally for path not exist?
        raise ae
    except Exception as e:
        # return {"error": fnf}
        print("Error: ", e)
    # except FileNotFoundError as fnf:
    #     print("File not found: ", fnf)
    return {"outlier_result": outlier_result, "analysis_result": analysis_result, "aggregate_result": aggregate_result}

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Pipeline").getOrCreate()
    complete_data = pathreader(spark, "complete_data") # should this be in the pipeline?
    result = pipeline(spark, complete_data)
    spark.stop()
