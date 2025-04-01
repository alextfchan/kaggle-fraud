import pytest
from polars import DataFrame
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
    data, filtered_data, expected_output, test_complete_data, request
):
    filtered_data = request.getfixturevalue(filtered_data)
    actual = aggregate(filtered_data, test_complete_data)
    assert isinstance(actual, expected_output)


def test_aggregate_column_names_output(
    data,
    filtered_data,
    expected_output,
    test_complete_data,
    aggregate_expected_columns,
    request,
):
    filtered_data = request.getfixturevalue(filtered_data)
    actual = aggregate(filtered_data, test_complete_data)
    assert actual.columns == aggregate_expected_columns


def test_aggregate_returns_correct_statistics(
    data,
    filtered_data,
    expected_output,
    aggregate_expected_rows,
    test_complete_data,
    request,
):
    filtered_data = request.getfixturevalue(filtered_data)
    actual = aggregate(filtered_data, test_complete_data)
    actual_row = [row["statistic"] for row in actual.iter_rows(named=True)]
    print(actual_row)
    assert actual_row == aggregate_expected_rows


def test_aggregate_returns_correct_count(
    data, filtered_data, expected_output, test_complete_data, request
):
    filtered_data = request.getfixturevalue(filtered_data)
    actual = aggregate(filtered_data, test_complete_data)
    result = actual.item(0, 2)
    assert result == 5


def test_aggregate_returns_dict_with_bad_input(
    data, filtered_data, expected_output, test_complete_data
):
    actual = aggregate(None, test_complete_data)
    expected = None
    assert actual == expected


def test_will_return_error_log_if_no_dataframe(
    data, filtered_data, expected_output, test_complete_data, caplog
):
    with caplog.at_level(logging.ERROR):
        aggregate(None, test_complete_data)
        expected = "Filtered Data was not found."
        assert expected in caplog.text
