from pyspark.sql import SparkSession
import yaml


def pathreader(spark: SparkSession, config: dict, file: str) -> dict:
    '''
    Returns a dict containing 
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
    try:
        # Access the config.yaml file
        with open(config, "r") as f:
            file_paths = yaml.safe_load(f)

        # Read the CSV file
        df = spark.read.csv(file_paths["data"][file], header=True, inferSchema=True)
    
        return {"dataframe": df, config: file_paths}
    
    except FileNotFoundError as fnf:
        print("File Not Found: ", fnf)
        return {"error": fnf}
    except Exception as e:
        print("Error: ", e)


# # Sucessful Run
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Path Reader").getOrCreate()
    result = pathreader(spark, "config.yaml","complete_data")
    print("result: ", result)
    spark.stop()
