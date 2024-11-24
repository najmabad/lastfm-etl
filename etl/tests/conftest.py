from datetime import datetime

import pyspark.sql.functions as F
import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for the tests"""
    spark = (
        SparkSession.builder.master("spark://test-runner-master:7077")
        .appName("Testing Top 10 Songs App")
        .getOrCreate()
    )
    return spark


@pytest.fixture(scope="session")
def sample_data_path():
    """Returns the path to the sample TSV data for schema testing"""
    return "tests/sample_data.tsv"


@pytest.fixture(scope="module")
def expected_events_with_sessions_df(spark_session):
    expected_data = [
        Row(
            "user-1",
            datetime(2024, 4, 21, 0, 0),
            "musicbrainz-artist-id-1",
            "artist-name-1",
            "musicbrainztrack-id-1",
            "song-name-1",
            None,
            None,
            1,
            1,
        ),
        Row(
            "user-1",
            datetime(2024, 4, 21, 0, 20),
            "musicbrainz-artist-id-1",
            "artist-name-1",
            None,
            "song-name-1",
            datetime(2024, 4, 21, 0, 0),
            1200,
            0,
            1,
        ),
        Row(
            "user-1",
            datetime(2024, 4, 21, 0, 40),
            "musicbrainz-artist-id-1",
            "artist-name-1",
            None,
            "song-name-1",
            datetime(2024, 4, 21, 0, 20),
            1200,
            0,
            1,
        ),
        Row(
            "user-1",
            datetime(2024, 4, 21, 1, 0),
            "musicbrainz-artist-id-1",
            "artist-name-1",
            None,
            "song-name-1",
            datetime(2024, 4, 21, 0, 40),
            1200,
            0,
            1,
        ),
        Row(
            "user-1",
            datetime(2024, 4, 21, 1, 20),
            "musicbrainz-artist-id-2",
            "artist-name-2",
            None,
            "song-name-1",
            datetime(2024, 4, 21, 1, 0),
            1200,
            0,
            1,
        ),
        Row(
            "user-1",
            datetime(2024, 4, 22, 0, 0),
            "musicbrainz-artist-id-3",
            "artist-name-3",
            "musicbrainztrack-id-3",
            "song-name-3",
            datetime(2024, 4, 21, 1, 20),
            81600,
            1,
            2,
        ),
        Row(
            "user-1",
            datetime(2024, 4, 22, 0, 20, 1),
            "musicbrainz-artist-id-3",
            "artist-name-3",
            "musicbrainztrack-id-3",
            "song-name-3",
            datetime(2024, 4, 22, 0, 0),
            1201,
            1,
            3,
        ),
        Row(
            "user-2",
            datetime(2024, 4, 21, 0, 0),
            "musicbrainz-artist-id-4",
            "artist-name-4",
            "musicbrainztrack-id-4",
            "song-name-4",
            None,
            None,
            1,
            1,
        ),
        Row(
            "user-2",
            datetime(2024, 4, 21, 0, 20),
            "musicbrainz-artist-id-4",
            "artist-name-4",
            "musicbrainztrack-id-5",
            "song-name-4",
            datetime(2024, 4, 21, 0, 0),
            1200,
            0,
            1,
        ),
        Row(
            "user-2",
            datetime(2024, 4, 21, 0, 40),
            "musicbrainz-artist-id-4",
            "artist-name-4",
            "musicbrainztrack-id-5",
            "song-name-4",
            datetime(2024, 4, 21, 0, 20),
            1200,
            0,
            1,
        ),
    ]
    expected_schema = StructType(
        [
            StructField("user_id", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("musicbrainz_artist_id", StringType()),
            StructField("artist_name", StringType()),
            StructField("musicbrainz_track_id", StringType()),
            StructField("track_name", StringType()),
            StructField("previous_timestamp", TimestampType()),
            StructField("time_diff_seconds", IntegerType()),
            StructField("new_session", IntegerType()),
            StructField("session_id", IntegerType()),
        ]
    )
    return spark_session.createDataFrame(expected_data, schema=expected_schema)


@pytest.fixture(scope="module")
def expected_session_metrics_df(spark_session):
    session_data = [
        Row(
            "user-1",
            "1",
            datetime(2024, 4, 21, 0, 0),
            datetime(2024, 4, 21, 1, 0),
            3600,
        ),
        Row(
            "user-1",
            "2",
            datetime(2024, 4, 22, 0, 0),
            datetime(2024, 4, 22, 0, 0),
            0,
        ),
        Row(
            "user-1",
            "3",
            datetime(2024, 4, 22, 20, 1),
            datetime(2024, 4, 22, 20, 1),
            0,
        ),
        Row(
            "user-2",
            "1",
            datetime(2024, 4, 21, 0, 0),
            datetime(2024, 4, 21, 0, 40),
            2400,
        ),
    ]
    session_schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("session_id", StringType(), False),
            StructField("min_timestamp", TimestampType(), False),
            StructField("max_timestamp", TimestampType(), False),
            StructField("session_duration", IntegerType(), False),
        ]
    )
    return spark_session.createDataFrame(session_data, schema=session_schema)
