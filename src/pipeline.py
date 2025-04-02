import polars as pl
from src.process.utils.pathreader import pathreader
from src.process.outlier_detection import outlier_detection
from src.process.aggregate import aggregate
from src.process.analysis import analysis

# logger = logging.getLogger()


def pipeline(config: str, file: str = "complete_data") -> dict:
    """
    Function that runs the entire pipeline.  The pipeline consists of the following steps:
    1. Outlier Detection
    2. Analysis
    3. Aggregate

    Parameters
    ----------
    config: str
        name of the config.yaml file
    file: str
        default value: "complete_data"
        name of the file to read within config.yaml

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
    """
    # Step 1: Reading the CSV file <-should be able to run just from the config. E&O = io, T should be decoupled.
    data = pathreader(config, file)

    # Step 2: Outlier Detection
    outlier_result = outlier_detection(data)
    # Step 3: Analysis
    analysis_result = analysis(outlier_result["filtered_data"], data)
    # Step 4: Aggregate
    aggregate_result = aggregate(outlier_result["filtered_data"], data)

    return {
        "outlier_result": outlier_result,
        "analysis_result": analysis_result,
        "aggregate_result": aggregate_result,
    }


if __name__ == "__main__":
    result = pipeline("config.yaml")
    print(result)
