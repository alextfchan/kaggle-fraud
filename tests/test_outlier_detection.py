import pytest
from pyspark.sql import SparkSession, DataFrame
from src.process.outlier_detection import outlier_detection
import logging
import yaml

logger = logging.getLogger()

expected_columns = [
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

key = ["outliers_data", "filtered_data"]


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("TestOutlierDetected").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="function")
def test_data(tmp_path):
    data = """Time,V1,V2,V3,V4,V5,V6,V7,V8,V9,V10,V11,V12,V13,V14,V15,V16,V17,V18,V19,V20,V21,V22,V23,V24,V25,V26,V27,V28,Amount,Class
    0,-1,-0.1,2.5,1.3,-0.3,0.4,0.2,0.09,0.3,0.2,-0.5,-0.6,-0.9,-0.3,1.6,-0.4,0.2,0.025,0.4,0.25,-0.04,0.27,-0.1,0.066,0.1,-0.17,0.1,-0.02,149.62,0
    1,1.19,0.2,0.16,0.44,0.06,-0.082,-0.078,0.0851,-0.2,-0.16,1.61,1.06,0.48,-0.14,0.63,0.4,-0.11,-0.1,-0.14,-0.0,-0.22,0.3,-0.49,0.0,0.24,-0.0,0.11,-0.0,2.69,0"""
    data_path = tmp_path / "test_creditcard.csv"
    data_path.write_text(data)
    return str(data_path)

@pytest.fixture(scope="function")
def test_config(test_data, tmp_path):
    '''
    Creating a temporary config file so that the original config.yaml is not modified during the process.
    '''
    config_content = f"""
    data:
        complete_data: {test_data}
        root: {str(tmp_path)}
    paths:
        anomalies: {str(tmp_path)}
        output: {str(tmp_path)}
    """
    config_path = tmp_path / "test_config.yaml"
    config_path.write_text(config_content)
    return str(config_path)

@pytest.fixture(scope="function")
def test_complete_data(spark, test_data, test_config):
    '''
    Test dictionary which would have been received from pathreader.py function.
    '''
    with open(test_config, "r") as f:
        config_content = yaml.safe_load(f)

    test_complete_data = {
        "dataframe": spark.read.csv(test_data, header=True, inferSchema=True),
        "config_file": test_config,
        "file_path": config_content,
        }
    return test_complete_data


def test_output_is_dataframe(spark, test_complete_data):
    actual = outlier_detection(spark, test_complete_data)
    for df in key:
        assert isinstance(actual[df], DataFrame)
    
def test_number_of_keys_returned(spark, test_complete_data):
    actual = outlier_detection(spark, test_complete_data)
    assert len(actual) == 2

def test_column_output(spark, test_complete_data):
    actual = outlier_detection(spark, test_complete_data)
    for df in key:
        assert actual[df].columns == expected_columns

def test_output_returns_none_if_no_df(spark):
    test_bad_data = None
    actual = outlier_detection(spark, test_bad_data)
    for df in key:
        assert actual[df] is None

def test_missing_complete_data(spark):
    bad_data = {"dataframe": None,
            "config_file": "",
            "file_path": {}}
    actual = outlier_detection(spark, bad_data)
    expected = {"outliers_data": None, "filtered_data": None}
    assert actual == expected

def test_incorrect_dict_input(spark):
    bad_data = {}
    actual = outlier_detection(spark, bad_data)
    expected = {"outliers_data": None, "filtered_data": None}
    assert actual == expected