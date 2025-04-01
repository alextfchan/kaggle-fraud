import pytest
from polars import DataFrame
from src.process.outlier_detection import outlier_detection
from .test_data import sample_data as data


key = ["outliers_data", "filtered_data"]


@pytest.mark.parametrize("data, expected", [(data[5], DataFrame)])
def test_output_is_dataframe(data, expected, test_complete_data):
    actual = outlier_detection(test_complete_data)
    # print(actual)
    for df in actual:
        assert isinstance(actual[df], expected)


@pytest.mark.parametrize("data, expected", [(data[5], 2)])
def test_number_of_keys_returned(data, expected, test_complete_data):
    actual = outlier_detection(test_complete_data)
    assert len(actual) == expected


@pytest.mark.parametrize("data, expected", [(data[5], "full_expected_columns")])
def test_column_output(data, expected, test_complete_data, request):
    expected = request.getfixturevalue("full_expected_columns")
    actual = outlier_detection(test_complete_data)
    for df in key:
        assert actual[df].columns == expected


def test_output_returns_none_if_no_df():
    test_bad_data = None
    actual = outlier_detection(test_bad_data)
    for df in key:
        assert actual[df] is None


def test_missing_complete_data():
    bad_data = {"dataframe": None, "config_file": "", "file_path": {}}
    actual = outlier_detection(bad_data)
    expected = {"outliers_data": None, "filtered_data": None}
    assert actual == expected


def test_incorrect_dict_input():
    bad_data = {}
    actual = outlier_detection(bad_data)
    expected = {"outliers_data": None, "filtered_data": None}
    assert actual == expected
