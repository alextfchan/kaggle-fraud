import pytest
from polars import DataFrame, read_csv
import yaml


# Data -> have in fixtures... or in variable like in test_data.py?
@pytest.fixture(scope="module")  # used in test_pathreader & test_outlier_detection
def full_expected_columns():
    return [
        "Time",
        "V1",
        "V2",
        "V3",
        "V4",
        "V5",
        "V6",
        "V7",
        "V8",
        "V9",
        "V10",
        "V11",
        "V12",
        "V13",
        "V14",
        "V15",
        "V16",
        "V17",
        "V18",
        "V19",
        "V20",
        "V21",
        "V22",
        "V23",
        "V24",
        "V25",
        "V26",
        "V27",
        "V28",
        "Amount",
        "Class",
    ]


@pytest.fixture(scope="module")
def aggregate_expected_columns():
    return [
        "statistic",
        "Time",
        "V4",
        "Amount",
        "Class",
    ]


@pytest.fixture(scope="module")
def aggregate_expected_rows():
    return [
        "count",
        "null_count",
        "mean",
        "std", 
        "min",
        "25%",
        "50%",
        "75%",
        "max",
    ]


@pytest.fixture(scope="module")
def analysis_expected_columns():
    return [
        "Time",
        "V4",
        "Amount",
        "Class",
    ]


# Temporary Complete Data File/Path Fixture -> Used by pathreader. Imported by get_raw_data.py
@pytest.fixture(scope="function")
def test_complete_dataframe(tmp_path, data):
    """arg: data:str will be passed from parametrize test_cases"""
    if data is None:
        return None, ""
    data_path = tmp_path / "test_creditcard.csv"
    data_path.write_text(data)
    df = read_csv(str(data_path), ignore_errors=True)
    return df, str(data_path)


# Temporary Config File/Path Fixture -> Used by pathreader.py
@pytest.fixture(scope="function")
def test_config(tmp_path, test_complete_dataframe):
    """arg: data:str will be passed from parametrize test_cases"""
    config_content = f"""
    data:
        complete_data: {test_complete_dataframe[1]}
        root: data/
    paths:
        anomalies: data/anomalies/
        output: data/output/
    """
    config_path = tmp_path / "test_config.yaml"
    config_path.write_text(config_content)

    with open(config_path, "r") as config_file:
        yaml.safe_load(config_file)

    return str(config_path)


# Temporary Filtered Data File/Path Fixture -> Result from outlier_detection.py
@pytest.fixture(scope="function")
def test_filtered_data(tmp_path, data) -> DataFrame:
    """arg: data:str will be passed from parametrize test_cases"""
    # if len(data) == 0:
    # return None
    # print("data: ", data)
    if data is None:
        return None
    data_path = tmp_path / "test_filtered_data.csv"
    data_path.write_text(data)
    df = read_csv(str(data_path), ignore_errors=True)
    return df


# Create Test Complete Data Dict -> Comes from Pathreader.py
@pytest.fixture(scope="function")
def test_complete_data(tmp_path, test_complete_dataframe) -> dict:
    """
    Test dictionary which would have been received from pathreader.py function.
    """
    test_complete_data = {
        "dataframe": test_complete_dataframe[0],
        "config_file": "test_config.yaml",
        "file_path": {
            "data": {
                "complete_data": str(
                    tmp_path
                ),  # might need to change this for outlierdetection.
                "root": str(tmp_path),
            },  # might need to change this for outlierdetection.
            "paths": {
                "anomalies": str(tmp_path),
                "output": str(tmp_path),
            },
        },
    }
    return test_complete_data
