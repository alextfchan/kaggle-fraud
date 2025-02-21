from pyspark.sql import SparkSession
import yaml


def pathreader(spark: SparkSession, config: dict, file: str) -> dict:
    '''
    Util that provides 
        1. the path to the specific file, and returns a DataFrame
        2. returns the config file containing the paths.

    Parameters
    ----------
    config: str
        name of the config.yaml file
    file: str
        name of the file to read located within config.yaml
    
    Return
    ------
    dict
        a dictionary containing the DataFrame and a dict containing the file paths
    '''

    # Access the config.yaml file
    with open(config, "r") as f:
        config = yaml.safe_load(f)

    # Read the CSV file
    df = spark.read.csv(config["data"][file], header=True, inferSchema=True)
    
    return {"dataframe": df, "file_path": config}


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Path Reader").getOrCreate()
    result = pathreader(spark, "config.yaml","complete_data")
    print("result: ", result)
    spark.stop()
