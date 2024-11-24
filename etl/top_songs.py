import argparse
import json
import logging.config
import os
import sys
import time

from etl_tasks import transform
from etl_tasks.analyze import get_top_songs_in_longest_session
from etl_tasks.extract import extract
from etl_tasks.load import save_as_tsv
from etl_utils.spark_utils import SparkSessionCreator

logging.config.fileConfig(
    fname="config/logging.config.ini", disable_existing_loggers=False
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Spark application with custom configuration"
    )
    parser.add_argument(
        "--config", default="config/default.json", help="Path to the configuration file"
    )
    parser.add_argument(
        "--session-threshold", type=int, default=20, help="Session duration in minutes"
    )
    parser.add_argument(
        "--session-number",
        type=int,
        default=50,
        help="Number of longest sessions to consider",
    )
    parser.add_argument(
        "--song-number",
        type=int,
        default=10,
        help="Number of most popular songs to consider",
    )
    return parser.parse_args()


def load_configs(config_name: str):
    """Loads and returns configuration settings from a JSON file"""
    base_dir: str = os.path.dirname(os.path.realpath(__file__))

    try:
        logger.info("Importing project configurations")
        with open(config_name, "r") as config_file:
            config = json.load(config_file)

        data_dir: str = os.path.join(base_dir, config["data_directory"])
        output_dir: str = os.path.join(base_dir, config["output_directory"])
        url = config["url"]

        expected_filepaths = [
            os.path.join(data_dir, path) for path in config["expected_files"]
        ]

        if not all(
            key in config
            for key in ["data_directory", "output_directory", "url", "expected_files"]
        ):
            raise ValueError("Configuration values missing")

        logger.info("All configurations have been imported successfully ✅ ")
        return data_dir, output_dir, url, expected_filepaths

    except FileNotFoundError:
        logger.error("Configuration file not found ❌ ")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error("Error decoding JSON ❌ ")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e} ❌ ")
        sys.exit(1)


def get_app_name() -> str:
    app_path = __file__
    app_name_with_extension = os.path.basename(app_path)
    app_name = os.path.splitext(app_name_with_extension)[0]

    return app_name


def main():
    args = parse_args()
    config = args.config
    session_threshold = args.session_threshold
    session_number = args.session_number
    song_number = args.song_number

    data_dir, output_dir, url, expected_filepaths = load_configs(config)
    app_name = get_app_name()

    try:
        logger.info("ETL process start 🚀 ")

        # Extract
        logger.info("Fetch Last.fm 1K users data")
        extract(data_dir, url, expected_filepaths)
        logger.info("Fetch Completed ✅ ")

        # Transform
        logger.info("Create intermediate enhanced events and sessions DataFrames")
        spark = SparkSessionCreator().get_session(app_name=app_name)
        music_events_filepath = expected_filepaths[1]

        events = transform.create_events_dataframe(
            spark,
            music_events_filepath,
        )

        events_with_sessions = transform.enhance_events_with_session_columns(
            events, session_threshold
        ).cache()

        session_metrics = transform.create_session_metrics_dataframe(
            events_with_sessions
        )

        # Analyze
        logger.info("Start analysis to compute top songs in longest sessions")
        top_songs = get_top_songs_in_longest_session(
            events_with_sessions, session_metrics, session_number, song_number
        )

        # Load
        logger.info(
            f"Loading top {song_number} songs from the {session_number} longest sessions into a TSV file"
        )
        filename = f"top_{song_number}_songs_in_{session_number}_longest_session.tsv"
        save_as_tsv(df=top_songs, output_dir=output_dir, filename=filename)
        logger.info("Completed ✅ ")

        logger.info("ETL process completed 🎉 ")
        time.sleep(100)

    except Exception as e:
        logger.error("Error running ETL process ❌ ")
        logger.error(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
