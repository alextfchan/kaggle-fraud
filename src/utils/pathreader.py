import logging

import yaml
from polars import read_csv

logger = logging.getLogger()

# seems to be a pathread for CSV files now.


def pathreader(config: str = "config.yaml", file: str = "complete_data") -> dict:
    """
    Returns a dict containing
        1. the DataFrame, read from the CSV file.
        2. returns the config file path.
        3. returns the config file contents, containing the paths.

    Parameters
    ----------
    config: str
        default: "config.yaml"
        name of the config.yaml file
    file: str
        default: "complete_data"
        name of the file to read located within config.yaml

    Return
    ------
    dict
        a dictionary containing the DataFrame, the path of the config file, and a dict containing the file paths
    """

    df = None
    file_paths = {}

    try:
        with open(config, "r") as f:
            file_paths = yaml.safe_load(f)

        df = read_csv(
            file_paths["data"][file], ignore_errors=True
        )  # keeps correct dtype for each column.

        logger.info(f"Read {file} from: {file_paths['data'][file]}")

    except KeyError as ke:
        logger.exception(f"Error reading: {ke}, CSV does not exist.")
    except FileNotFoundError as fnf:
        logger.exception(f"Config File Not Found: {fnf}")
    except Exception as e:
        logger.exception(f"Unexcepted Error: {e}")

    return {"dataframe": df, "config_file": config, "file_path": file_paths}


# Sucessful Run
if __name__ == "__main__":
    result = pathreader("config.yaml", "complete_data")
    print("result: ", result)
    # print("result: ", type(result["dataframe"]))
    # print(dir(pl))
    # print(isinstance(result["dataframe"], DataFrame))
