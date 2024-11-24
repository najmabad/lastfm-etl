from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from etl_tasks.transform import (
    EventUserSessionEnhancer,
    MusicEventLoader,
    SessionMetricsGenerator,
    create_events_dataframe,
    enhance_events_with_session_columns,
)


def test_music_event_loader_initialization(spark_session, sample_data_path):
    loader = MusicEventLoader(spark_session, sample_data_path)

    assert (
        loader.spark == spark_session
    ), f"Spark session in loader did not initialize correctly. Expected Spark session {spark_session}, but got {loader.spark}"
    assert (
        loader.file_path == sample_data_path
    ), f"File path in loader did not initialize correctly. Expected file path {sample_data_path}, but got {loader.file_path}"


def test_create_spark_dataframe(spark_session, sample_data_path):
    loader = MusicEventLoader(spark_session, sample_data_path)
    df = loader.create_dataframe()

    # DataFrame has the correct schema
    expected_schema = MusicEventLoader.schema
    assert (
        df.schema == expected_schema
    ), "DataFrame schema does not match expected schema"

    # DataFrame is not empty
    assert not df.isEmpty(), "DataFrame is empty but it shouldn't be"

    # DataFrame has the expected numer of rows
    expected_row_count = 10
    actual_row_count = df.count()

    assert (
        actual_row_count == expected_row_count
    ), f"DataFrame should have {expected_row_count} rows, but it has {actual_row_count} rows."

    # DataFrame has the expected numer of columns
    expected_column_count = 6
    actual_column_count = len(df.schema.fields)

    assert (
        actual_column_count == expected_column_count
    ), f"DataFrame should have {expected_column_count} columns, but it has {actual_column_count} columns."


def test_event_user_session_enhancer_initialization():
    session_threshold = 30
    user_id_col = "user_id"
    enhancer = EventUserSessionEnhancer(session_threshold, user_id_col)

    assert (
        enhancer.session_threshold == session_threshold
    ), "Session threshold did not initialize correctly."
    assert (
        enhancer.user_id_col == user_id_col
    ), "User ID column did not initialize correctly."
    assert (
        enhancer.session_threshold_seconds == session_threshold * 60
    ), "Session threshold seconds did not compute correctly."


def test_validate_input_df_columns_with_required_columns(spark_session):
    schema = StructType(
        [
            StructField("timestamp", TimestampType()),
            StructField("user_id", StringType()),
        ]
    )
    data = [Row(timestamp=datetime.now(), user_id="user1")]
    df = spark_session.createDataFrame(data, schema)
    enhancer = EventUserSessionEnhancer(30, "user_id")

    # This should pass without raising an exception
    try:
        enhancer._validate_input_df_columns(df)
    except Exception as e:
        pytest.fail(f"Unexpected exception raised: {e}")


def test_validate_input_df_columns_with_missing_columns(spark_session):
    # Schema missing the 'timestamp' column
    schema_missing_columns = StructType([StructField("user_id", StringType())])
    data = [Row(user_id="user1")]
    df_missing_columns = spark_session.createDataFrame(data, schema_missing_columns)
    enhancer = EventUserSessionEnhancer(30, "user_id")

    # Expecting a ValueError to be raised due to missing 'timestamp' column
    with pytest.raises(ValueError) as excinfo:
        enhancer._validate_input_df_columns(df_missing_columns)
    assert "Missing required columns in DataFrame: timestamp" in str(excinfo.value)


def test_add_session_columns(spark_session):
    # Create a DataFrame with necessary columns and data
    schema = StructType(
        [
            StructField("timestamp", TimestampType()),
            StructField("user_id", StringType()),
        ]
    )
    data = [
        Row(timestamp=datetime.now(), user_id="user1"),
        Row(
            timestamp=datetime.now(), user_id="user1"
        ),  # Another entry for the same user
    ]
    df = spark_session.createDataFrame(data, schema)
    enhancer = EventUserSessionEnhancer(30, "user_id")
    enhanced_df = enhancer.create_dataframe(df)

    # Check if new columns are added
    required_columns = {
        "previous_timestamp",
        "time_diff_seconds",
        "new_session",
        "session_id",
    }
    missing_columns = required_columns - set(enhanced_df.columns)
    assert (
        not missing_columns
    ), f"Missing columns in enhanced DataFrame: {missing_columns}"

    # Check for data correctness, e.g., new_session flags
    results = enhanced_df.collect()
    assert (
        results[0]["new_session"] == 1
    ), "New session flag not set correctly on the first record."


def test_session_metrics_generator_initialization():
    unique_session_id = ["user_id", "session_id"]
    generator = SessionMetricsGenerator(unique_session_id)

    assert (
        generator.unique_session_id == unique_session_id
    ), "Unique session ID did not initialize correctly."


def test_validate_input_df_columns(spark_session):
    schema = StructType(
        [
            StructField("timestamp", TimestampType()),
            StructField("user_id", StringType()),
            StructField("session_id", StringType()),
        ]
    )
    data = [Row(timestamp=datetime.now(), user_id="user1", session_id="session1")]
    df = spark_session.createDataFrame(data, schema)
    generator = SessionMetricsGenerator(["user_id", "session_id"])

    # This should pass without raising an exception
    try:
        generator._validate_input_df_columns(df)
    except Exception as e:
        pytest.fail(f"Unexpected exception raised: {e}")


def test_validate_input_df_columns_with_missing_columns(spark_session):
    # Schema missing the 'timestamp' column
    schema_missing_columns = StructType(
        [StructField("user_id", StringType()), StructField("session_id", StringType())]
    )
    data = [Row(user_id="user1", session_id="session1")]
    df_missing_columns = spark_session.createDataFrame(data, schema_missing_columns)
    generator = SessionMetricsGenerator(["user_id", "session_id"])

    # Expecting a ValueError to be raised due to missing 'timestamp' column
    with pytest.raises(ValueError) as excinfo:
        generator._validate_input_df_columns(df_missing_columns)
    assert "Missing required columns in DataFrame: timestamp" in str(excinfo.value)


def test_aggregate_sessions(spark_session):
    schema = StructType(
        [
            StructField("timestamp", TimestampType()),
            StructField("user_id", StringType()),
            StructField("session_id", StringType()),
        ]
    )
    now = datetime.now()
    data = [
        Row(timestamp=now, user_id="user1", session_id="session1"),
        Row(timestamp=now, user_id="user1", session_id="session1"),
    ]
    df = spark_session.createDataFrame(data, schema)
    generator = SessionMetricsGenerator(["user_id", "session_id"])
    result_df = generator._aggregate_sessions(df)

    # Test if the aggregation results match expected values
    expected_min = result_df.select("min_timestamp").collect()[0][0]
    expected_max = result_df.select("max_timestamp").collect()[0][0]
    assert expected_min == now, "Aggregation of min_timestamp is incorrect"
    assert expected_max == now, "Aggregation of max_timestamp is incorrect"


def test_calculate_session_duration(spark_session):
    schema = StructType(
        [
            StructField("min_timestamp", TimestampType()),
            StructField("max_timestamp", TimestampType()),
        ]
    )
    start_time = datetime(2021, 1, 1, 12, 0, 0)
    end_time = datetime(2021, 1, 1, 12, 30, 0)
    data = [Row(min_timestamp=start_time, max_timestamp=end_time)]
    df = spark_session.createDataFrame(data, schema)
    generator = SessionMetricsGenerator(["user_id", "session_id"])

    result_df = generator._calculate_session_duration(df)
    expected_duration = result_df.select("session_duration").collect()[0][0]
    assert expected_duration == 1800, "Session duration calculation is incorrect"


def test_create_events_dataframe(spark_session, sample_data_path):
    # Call the function with the Spark session and the sample data path
    df = create_events_dataframe(spark_session, sample_data_path)

    # Check the count of rows to ensure data is loaded correctly
    assert df.count() == 10, "DataFrame should contain 10 rows of data"

    # Check for specific data points to ensure data integrity
    first_row = df.filter(
        (df.user_id == "user-1") & (df.timestamp == datetime(2024, 4, 21, 0, 0))
    ).collect()[0]
    assert (
        first_row.artist_name == "artist-name-1"
    ), "Artist name does not match expected value"


def test_enhance_events_with_session_columns(
    spark_session, sample_data_path, expected_events_with_sessions_df
):
    # Load sample data into DataFrame
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

    df = (
        spark_session.read.option("delimiter", "\t")
        .option("header", "false")
        .schema(schema)
        .csv(sample_data_path)
    )
    session_threshold = 20  # minutes

    # Call the function to enhance DataFrame
    enhanced_df = enhance_events_with_session_columns(df, session_threshold).orderBy(
        "user_id", "timestamp"
    )
    print("enhanved")
    enhanced_df.show()

    print("expected")
    expected_events_with_sessions_df.show()

    # Compare the dataframes
    assert (
        enhanced_df.collect() == expected_events_with_sessions_df.collect()
    ), "DataFrames do not match"
