import logging.config
import os

from pyspark.sql import DataFrame

logging.config.fileConfig(
    fname="config/logging.config.ini", disable_existing_loggers=False
)
logger = logging.getLogger(__name__)


def save_as_tsv(df: DataFrame, output_dir: str, filename: str):
    """Save the DataFrame to a TSV file.

    Important: use this method only with small DataFrames!
    """
    file_path = os.path.join(output_dir, filename)
    pandas_df = df.toPandas()

    pandas_df.to_csv(file_path, sep="\t", index=False, header=True)
    logger.info(f"Saved DataFrame to {file_path}")
