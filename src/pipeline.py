from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pandas as pd
from src.process.outlier_detection import outlier_detection
from src.process.aggregate import aggregate
from src.process.analysis import analysis

def pipeline(spark: SparkSession, complete_data: str, filtered_data: str) -> dict:
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
        path to the original CSV file
    filtered_data: str
        path to the filtered CSV file

    Output
    ------
    Parquet file:
        aggregated data from the CSV file

    Return
    ------
    dict
        a dictionary containing the results in DataFrame format from the outlier detection, analysis, and aggregation processes
    '''
    try:
        # Step 1: Outlier Detection
        outlier_result = outlier_detection(spark, complete_data)
        
        # Step 2: Analysis
        analysis_result = analysis(spark, filtered_data)
        
        # Step 3: Aggregate
        aggregate_result = aggregate(spark, filtered_data)
    
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
    result = pipeline(spark, "data/ccreditcard.csv", "data/filtered_data.csv")
    spark.stop()
    # print(result)