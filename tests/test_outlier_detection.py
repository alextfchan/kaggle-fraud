import pytest
from pyspark.sql import SparkSession, DataFrame
from src.process.outlier_detection import outlier_detection

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
    data_path = tmp_path / "test_creditcard.csv" #file path is correct. uses complete data
    data_path.write_text(data)
    return str(data_path)

def test_outlier_output_is_dataframe(spark, test_data, tmp_path):
    actual = outlier_detection(spark, test_data)
    for df in actual.values():
        assert isinstance(df, DataFrame)

def test_analysis_number_of_dataframes_returned(spark, test_data, tmp_path):
    actual = outlier_detection(spark, test_data)
    assert len(actual) == 2

def test_outlier_column_output(spark, test_data, tmp_path):
    actual = outlier_detection(spark, test_data)
    for df in actual.values():
        assert df.columns == expected_columns