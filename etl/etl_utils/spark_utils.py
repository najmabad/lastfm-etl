import os

from pyspark.sql import DataFrame, SparkSession


class SparkSessionCreator:
    """Utility class for configuring and accessing a SparkSession"""

    @staticmethod
    def get_session(app_name: str, config=None):
        """Return a SparkSession instance with specified app name and configuration"""

        builder = SparkSession.builder.appName(app_name)
        if config is not None:
            for key, value in config.items():
                builder = builder.config(key, value)
        return builder.getOrCreate()
