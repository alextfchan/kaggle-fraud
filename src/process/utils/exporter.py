import pandas as pd
import logging


logger = logging.getLogger()


def to_parquet(df: pd.DataFrame, file_path: str) -> None:
    '''
    Function exports a DataFrame to a Parquet file.
    '''
    try:
        df.toPandas().to_parquet(file_path)
        logger.info(f"Successfully exported to Parquet: {file_path}")
    except Exception as e:
        logger.exception(f"Error exporting to Parquet: {e}")


def to_csv(df: pd.DataFrame, file_path: str) -> None:
    '''
    Function exports a DataFrame to a CSV file.
    '''
    try:
        df.toPandas().to_csv(file_path)
        logger.info(f"Successfully exported to CSV: {file_path}")

    except Exception as e:
        logger.exception(f"Error exporting to CSV: {e}")
