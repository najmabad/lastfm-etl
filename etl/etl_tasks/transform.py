import logging.config

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

logging.config.fileConfig(
    fname="config/logging.config.ini", disable_existing_loggers=False
)
logger = logging.getLogger(__name__)


class MusicEventLoader:
    """Handle loading of music event data from a TSV file using a predefined schema"""

    schema = StructType(
        [
            StructField("user_id", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("musicbrainz_artist_id", StringType()),
            StructField("artist_name", StringType()),
            StructField("musicbrainz_track_id", StringType()),
            StructField("track_name", StringType()),
        ]
    )

    def __init__(self, spark: SparkSession, file_path: str):
        self.spark = spark
        self.file_path = file_path
        logger.debug(f"Initialized MusicEventLoader with file path: {file_path}")

    def create_dataframe(self) -> DataFrame:
        logger.info(f"Create Spark DataFrame from file: {self.file_path}")
        df = (
            self.spark.read.options(
                delimiter="\t",
                header=False,
            )
            .schema(self.schema)
            .csv(self.file_path)
        )

        return df


class EventUserSessionEnhancer:
    """Enhance event data with user session columns"""

    def __init__(self, session_threshold: int, user_id_col: str):
        self._validate_threshold(session_threshold)
        self.session_threshold = session_threshold
        self.session_threshold_seconds = session_threshold * 60
        self.user_id_col = user_id_col
        logger.info(
            f"User session initialized with threshold: {self.session_threshold} minutes and user column: {self.user_id_col}"
        )

    def create_dataframe(self, events: DataFrame) -> DataFrame:
        self._validate_input_df_columns(events)
        logger.info("Enhance events with session information")
        enhanced_events = self._add_session_columns(events)
        return enhanced_events

    def _validate_input_df_columns(self, events: DataFrame):
        required_columns = ["timestamp"] + [self.user_id_col]
        missing_columns = [col for col in required_columns if col not in events.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in DataFrame: {', '.join(missing_columns)}"
            )

    def _add_session_columns(self, events: DataFrame) -> DataFrame:
        window = Window.partitionBy(self.user_id_col).orderBy("timestamp")
        events = self._add_previous_timestamp(events, window)
        events = self._add_time_diff(events)
        events = self._add_new_session_flag(events)
        events = self._add_session_id(events, window)
        return events

    @staticmethod
    def _add_previous_timestamp(events: DataFrame, window) -> DataFrame:
        return events.withColumn("previous_timestamp", F.lag("timestamp").over(window))

    @staticmethod
    def _add_time_diff(events: DataFrame) -> DataFrame:
        return events.withColumn(
            "time_diff_seconds",
            F.unix_timestamp("timestamp") - F.unix_timestamp("previous_timestamp"),
        )

    def _add_new_session_flag(self, events: DataFrame) -> DataFrame:
        return events.withColumn(
            "new_session",
            (
                (F.col("time_diff_seconds").isNull())
                | (F.col("time_diff_seconds") > self.session_threshold_seconds)
            ).cast("integer"),
        )

    @staticmethod
    def _add_session_id(events: DataFrame, window) -> DataFrame:
        return events.withColumn(
            "session_id", F.sum("new_session").over(window).cast("integer")
        )

    @staticmethod
    def _validate_threshold(session_threshold: int):
        """Validates that the session threshold is greater than 0"""
        if session_threshold <= 0:
            raise ValueError("Session threshold must be greater than 0.")


class SessionMetricsGenerator:
    """Create sessions from music event data"""

    def __init__(self, unique_session_id: list[str]):
        """Initialize the SessionData object with session identifiers.

        :param unique_session_id: a mandatory unique session identifier
        """
        self.unique_session_id = unique_session_id

    def create_dataframe(self, events_with_sessions: DataFrame) -> DataFrame:
        self._validate_input_df_columns(events_with_sessions)
        logger.info("Creating session DataFrame")

        # Step 1: aggregate sessions
        aggregated_sessions = self._aggregate_sessions(events_with_sessions)

        # Step 2: calculate session duration
        sessions_with_duration = self._calculate_session_duration(aggregated_sessions)

        return sessions_with_duration

    def _validate_input_df_columns(self, events_with_sessions: DataFrame):
        """Validation logic to ensure required columns are present"""
        required_columns = ["timestamp"] + self.unique_session_id
        missing_columns = [
            col for col in required_columns if col not in events_with_sessions.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in DataFrame: {', '.join(missing_columns)}"
            )

    def _aggregate_sessions(self, events_with_sessions: DataFrame) -> DataFrame:
        """Aggregate session data."""
        return events_with_sessions.groupBy(self.unique_session_id).agg(
            F.min("timestamp").alias("min_timestamp"),
            F.max("timestamp").alias("max_timestamp"),
        )

    @staticmethod
    def _calculate_session_duration(sessions: DataFrame) -> DataFrame:
        """Calculate session duration"""
        return sessions.withColumn(
            "session_duration",
            F.unix_timestamp("max_timestamp") - F.unix_timestamp("min_timestamp"),
        )


def create_events_dataframe(
    spark: SparkSession,
    file_path: str,
):
    # Load events from file
    music_event_loader = MusicEventLoader(spark, file_path)
    events = music_event_loader.create_dataframe()

    return events


def enhance_events_with_session_columns(
    events: DataFrame,
    session_threshold: int,
) -> DataFrame:
    # Enhance events by creating session information
    event_user_session_enhancer = EventUserSessionEnhancer(
        session_threshold=session_threshold, user_id_col="user_id"
    )
    events_with_sessions = event_user_session_enhancer.create_dataframe(events)

    return events_with_sessions


def create_session_metrics_dataframe(events_with_sessions: DataFrame) -> DataFrame:
    # Create session DataFrame from enhanced events
    session_metrics_generator = SessionMetricsGenerator(
        unique_session_id=["user_id", "session_id"]
    )
    sessions = session_metrics_generator.create_dataframe(events_with_sessions)

    return sessions
