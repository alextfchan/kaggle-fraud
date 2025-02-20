from pyspark.sql import SparkSession, DataFrame
import yaml

# Note:	Extract that file path config into yaml config. So don't need to show raw file path. Shouldn't expose any part of our system architecture.


# Putting the initial file path though... necessary?
# Want a way to extract the file path in a yaml or something, so that it's safe.

# How? YAML config, to have a predefined variable going direct to $PWD/data....
    # File NAME should be ok?
    # File PATH should be hidden -> all necessary files will be in the same folder.

def pathreader(spark: SparkSession, file: str) -> DataFrame:
    '''
    Util that provides 
        1. the path to the specific file, and returns a DataFrame
        2. returns the config file containing the paths.

    Parameters
    ----------
    file: str
        the name of the file to read located within config.yaml
    
    Return
    ------
    dict
        a dictionary containing the DataFrame and a dict containing the file paths
    '''
    # Access the config.yaml file
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # Read the CSV file
    df = spark.read.csv(config["data"][file], header=True, inferSchema=True)
    
    return {"dataframe": df, "file_path": config}


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Path Reader").getOrCreate()
    result = pathreader(spark, "complete_data")
    spark.stop()
