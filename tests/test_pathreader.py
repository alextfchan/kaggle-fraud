import pytest
from polars import DataFrame
from src.process.utils.pathreader import pathreader
from .test_data import sample_data as data
import logging


@pytest.mark.parametrize("data, expected", [(data[5], dict)])
def test_pathreader_return_dict(data, expected, test_config):
    actual = pathreader(test_config, "complete_data")
    assert isinstance(actual, expected)


@pytest.mark.parametrize("data, expected", [(data[5], DataFrame)])
def test_pathreader_returns_df(data, expected, test_config):
    actual = pathreader(test_config, "complete_data")
    assert isinstance(actual["dataframe"], expected)


@pytest.mark.parametrize("data", [(data[5])])
def test_pathreader_returns_correct_columns(data, test_config, full_expected_columns):
    actual = pathreader(test_config, "complete_data")
    assert actual["dataframe"].columns == full_expected_columns


@pytest.mark.parametrize("data", [(data[5])])
def test_pathreader_returns_correct_filepaths(
    data, test_complete_dataframe, test_config
):
    actual = pathreader(test_config, "complete_data")
    expected = {
        "data": {"complete_data": test_complete_dataframe[1], "root": "data/"},
        "paths": {"anomalies": "data/anomalies/", "output": "data/output/"},
    }
    assert actual["file_path"] == expected


@pytest.mark.parametrize("data", [(data[5])])
def test_pathreader_returns_correct_successful_log(
    data, test_complete_dataframe, test_config, caplog
):
    with caplog.at_level(logging.INFO):
        pathreader(test_config, "complete_data")
        assert test_complete_dataframe[1] in caplog.text


@pytest.mark.parametrize("data", [(data[5])])
def test_pathreader_returns_unsuccessful_log_csv(data, test_config, caplog):
    with caplog.at_level(logging.ERROR):
        pathreader(test_config, "incorrect_data")
        expected = "Error reading: 'incorrect_data', CSV does not exist."
        assert expected in caplog.text


def test_pathreader_returns_unsuccessful_log_yaml(caplog):
    with caplog.at_level(logging.ERROR):
        pathreader("incorrect.yaml", "complete_data")
        expected = "Config File Not Found: [Errno 2] No such file or directory: 'incorrect.yaml'"
        assert expected in caplog.text


def test_pathreader_returns_despite_fail():
    actual = pathreader("incorrect.yaml", "incomplete_data")
    assert actual == {
        "dataframe": None,
        "config_file": "incorrect.yaml",
        "file_path": {},
    }
