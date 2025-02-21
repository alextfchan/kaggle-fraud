import pytest
from pyspark.sql import SparkSession, DataFrame
from src.process.aggregate import aggregate
import yaml


expected_columns = [
    "summary",
    "Time",
    "V4",
    "Amount",
    "Class",
]

expected_rows = [
    'count',
    'mean',
    'stddev',
    'min',
    'max'
]

filtered_data = [
    """Time,V4,Amount,Class
    0,1.3,149.62,0
    1,0.44,2.69,0
    2,-1.5,200.00,1
    3,2.0,50.00,0
    4,0.0,100.00,1
    """,
    """Time,V4,Amount,Class
    0,-0.5,120.50,0
    1,1.2,5.00,0
    2,-2.0,300.00,1
    3,0.8,75.00,0
    4,1.5,60.00,1
    """,
    """Time,V4,Amount,Class
    0,0.9,80.00,0
    1,-1.1,15.00,0
    2,1.7,250.00,1
    3,-0.3,40.00,0
    4,2.2,90.00,1
    """,
    """Time,V4,Amount,Class
    0,1.0,130.00,0
    1,-0.7,20.00,0
    2,0.5,180.00,1
    3,1.8,55.00,0
    4,-1.4,110.00,1
    """,
    """Time,V4,Amount,Class
    0,0.6,140.00,0
    1,1.5,10.00,0
    2,-0.8,220.00,1
    3,2.1,65.00,0
    4,-1.2,95.00,1
    """,
    # """Time,V1,V2,V3,V4,V5,V6,V7,V8,V9,V10,V11,V12,V13,V14,V15,V16,V17,V18,V19,V20,V21,V22,V23,V24,V25,V26,V27,V28,Amount,Class
    # 0,-1,-0.1,2.5,1.3,-0.3,0.4,0.2,0.09,0.3,0.2,-0.5,-0.6,-0.9,-0.3,1.6,-0.4,0.2,0.025,0.4,0.25,-0.04,0.27,-0.1,0.066,0.1,-0.17,0.1,-0.02,149.62,0
    # 1,1.19,0.2,0.16,0.44,0.06,-0.082,-0.078,0.0851,-0.2,-0.16,1.61,1.06,0.48,-0.14,0.63,0.4,-0.11,-0.1,-0.14,-0.0,-0.22,0.3,-0.49,0.0,0.24,-0.0,0.11,-0.0,2.69,0""",
]


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("TestAggregate").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="function", params=filtered_data)
def test_data(tmp_path, request):
    data = request.param
    data_path = tmp_path / "test_filtered_data.csv"
    data_path.write_text(data)
    return str(data_path)

@pytest.fixture(scope="function")
def test_filtered_data(spark, test_data, tmp_path):
    return spark.read.csv(test_data, header=True, inferSchema=True)

@pytest.fixture(scope="function")
def test_file_paths(tmp_path):
    file_paths = {
        "dataframe": "test_dataframe",
        "file_path": {
            "data": {"complete_data": "test_complete_data",
                     "root": "test_root/"},
            "paths": {"anomalies": str(tmp_path),
                     "output": "test_output/"}
        }
    }
    return file_paths


def test_aggregate_output_is_dataframe(spark, test_filtered_data, test_file_paths, tmp_path):
    actual = aggregate(spark, test_filtered_data, test_file_paths)
    assert isinstance(actual["summary"], DataFrame)

def test_aggregate_column_output(spark, test_filtered_data, test_file_paths, tmp_path):
    actual = aggregate(spark, test_filtered_data, test_file_paths)
    assert actual["summary"].columns == expected_columns

def test_aggregate_returns_correct_rows(spark, test_filtered_data, test_file_paths, tmp_path):
    actual = aggregate(spark, test_filtered_data, test_file_paths)
    row_names = [name[0] for name in actual["summary"].select("summary").collect()]
    assert row_names == expected_rows

def test_aggregate_returns_correct_count(spark, test_filtered_data, test_file_paths, tmp_path):
    actual = aggregate(spark, test_filtered_data, test_file_paths)
    result = actual["summary"].select("V4").collect()[0][0]
    assert result == "5"
