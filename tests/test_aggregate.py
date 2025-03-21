import pytest
from pyspark.sql import DataFrame
from src.process.aggregate import aggregate
from .test_data import sample_data as data
import logging


pytestmark = pytest.mark.parametrize(
    "data, filtered_data, expected_output",
    [
        pytest.param(data[0], "test_filtered_data", DataFrame, id="basic test case"),
        pytest.param(data[1], "test_filtered_data", DataFrame, id="basic test case"),
        pytest.param(
            data[5], "test_filtered_data", DataFrame, id="test case: full columns"
        ),
    ],
)


def test_aggregate_output_is_dataframe(
    data, filtered_data, expected_output, test_complete_data, spark, request
):
    filtered_data = request.getfixturevalue(filtered_data)
    actual = aggregate(spark, filtered_data, test_complete_data)
    assert isinstance(actual, expected_output)


def test_aggregate_column_output(
    data,
    filtered_data,
    expected_output,
    test_complete_data,
    aggregate_expected_columns,
    spark,
    request,
):
    filtered_data = request.getfixturevalue(filtered_data)
    actual = aggregate(spark, filtered_data, test_complete_data)
    assert actual.columns == aggregate_expected_columns


def test_aggregate_returns_correct_rows(
    data,
    filtered_data,
    expected_output,
    aggregate_expected_rows,
    test_complete_data,
    spark,
    request,
):
    filtered_data = request.getfixturevalue(filtered_data)
    actual = aggregate(spark, filtered_data, test_complete_data)
    actual_row = [name[0] for name in actual.select("summary").collect()]
    assert actual_row == aggregate_expected_rows


def test_aggregate_returns_correct_count(
    data, filtered_data, expected_output, test_complete_data, spark, request
):
    filtered_data = request.getfixturevalue(filtered_data)
    actual = aggregate(spark, filtered_data, test_complete_data)
    result = actual.select("V4").collect()[0][0]
    assert result == "5"


def test_aggregate_returns_dict_with_bad_input(
    data, filtered_data, expected_output, test_complete_data, spark
):
    actual = aggregate(spark, None, test_complete_data)
    expected = None
    assert actual == expected


def test_will_return_error_log_if_no_dataframe(
    data, filtered_data, expected_output, test_complete_data, spark, caplog
):
    with caplog.at_level(logging.ERROR):
        aggregate(spark, None, test_complete_data)
        expected = "Filtered Data was not found."
        assert expected in caplog.text
