import pytest
from polars import DataFrame
from src.process.analysis import analysis
from .test_data import sample_data as data
import logging


@pytest.mark.parametrize(
    "data, filtered_data, expected",
    [
        pytest.param(data[0], "test_filtered_data", DataFrame, id="basic test case"),
        pytest.param(data[1], "test_filtered_data", DataFrame, id="basic test case"),
        pytest.param(
            data[5], "test_filtered_data", DataFrame, id="test case: full columns"
        ),
        pytest.param(
            data[8],
            "test_filtered_data",
            None,
            marks=pytest.mark.xfail(strict=True),
            id="bad_input(None) = Fail",
        ),
    ],
)
def test_analysis_output_is_dataframe(
    data,  # used in filtered_data fixture
    filtered_data,  # creates a temporary dataframe
    expected,
    test_complete_data,  # all parametrized tests share same fixture, so no need to add it into pytest.mark.parametrize.
    request,
):
    filtered_data = request.getfixturevalue(filtered_data)
    actual = analysis(filtered_data, test_complete_data)
    for df in actual.values():
        assert isinstance(df, expected)


@pytest.mark.parametrize(
    "data, filtered_data, expected",
    [
        pytest.param(data[0], "test_filtered_data", 3, id="basic test case"),
        pytest.param(data[1], "test_filtered_data", 3, id="basic test case"),
        pytest.param(
            data[5], "test_filtered_data", 3, id="test case: complete columns data"
        ),
        pytest.param(
            data[8], "test_filtered_data", 3, id="test case: no filtered dataframe"
        ),
    ],
)
def test_analysis_number_of_dataframes_returned(
    data,
    filtered_data,
    expected,
    test_complete_data,
    test_complete_dataframe,
    request,
):
    filtered_data = request.getfixturevalue(filtered_data)
    actual = analysis(filtered_data, test_complete_data)
    assert len(actual) == expected


@pytest.mark.parametrize(
    "data, filtered_data, expected",
    [
        pytest.param(
            data[0],
            "test_filtered_data",
            "analysis_expected_columns",
            id="basic test case",
        ),
        pytest.param(
            data[1],
            "test_filtered_data",
            "analysis_expected_columns",
            id="basic test case",
        ),
        pytest.param(
            data[5],
            "test_filtered_data",
            "analysis_expected_columns",
            id="test case: complete columns data",
        ),
        pytest.param(
            data[8],
            "test_filtered_data",
            "analysis_expected_columns",
            marks=pytest.mark.xfail(strict=True),
            id="fail test case: no dataframe passed",
        ),
    ],
)
def test_analysis_column_output(
    data, filtered_data, expected, test_complete_data, request
):
    filtered_data, expected = (
        request.getfixturevalue(filtered_data),
        request.getfixturevalue(expected),
    )

    actual = analysis(filtered_data, test_complete_data)
    for df in actual.values():
        assert df.columns == expected


@pytest.mark.parametrize("data, expected", [(data[8], None)])
def test_output_is_none_if_bad_input(data, expected, test_complete_data):
    actual = analysis(data, test_complete_data)
    for df in actual.values():
        assert df is expected


@pytest.mark.parametrize("data", [(data[8])])
def test_analysis_returns_dict_with_bad_input(data, test_complete_data):
    actual = analysis(None, test_complete_data)
    expected = {"high_bucket": None, "medium_bucket": None, "low_bucket": None}
    assert actual == expected


def test_will_return_error_log_if_no_dataframe(caplog):
    with caplog.at_level(logging.ERROR):
        analysis(None, None)
        expected = "Filtered Data was not found."
        assert expected in caplog.text
