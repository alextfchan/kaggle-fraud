from polars import DataFrame
import logging


logger = logging.getLogger()


def to_parquet(df: DataFrame, file_path: str) -> None:
    """
    Function exports a DataFrame to a Parquet file.
    """
    try:
        df.write_parquet(file_path)
        logger.info(f"Data: {df} successfully exported to Parquet: {file_path}")
    except Exception as e:
        logger.exception(f"Error exporting to Parquet: {e}")


def to_csv(df: DataFrame, file_path: str) -> None:
    """
    Function exports a DataFrame to a CSV file.
    """
    try:
        df.write_csv(file_path)
        logger.info(f"Data: {df} successfully exported to CSV: {file_path}")
    except Exception as e:
        logger.exception(f"Error exporting to CSV: {e}")
