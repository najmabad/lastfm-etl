from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from etl_tasks.analyze import get_top_songs_in_longest_session


def test_get_top_songs_in_longest_session(
    spark_session, expected_events_with_sessions_df, expected_session_metrics_df
):
    session_number = 2
    song_number = 1

    top_songs = get_top_songs_in_longest_session(
        events_with_sessions=expected_events_with_sessions_df,
        sessions=expected_session_metrics_df,
        session_number=session_number,
        song_number=song_number,
    )

    print("actual")
    top_songs.show()

    # Validate the output DataFrame
    assert top_songs.count() <= song_number, "More songs returned than expected"
    assert (
        len(top_songs.columns) == 3
    ), "Expected columns are missing or extra columns are present"

    # Expected DataFrame
    top_song_data = [
        Row(
            "musicbrainztrack-id-5",
            "song-name-4",
            "artist-name-4",
        ),
    ]
    schema = StructType(
        [
            StructField("musicbrainz_track_id", StringType()),
            StructField("track_name", StringType()),
            StructField("artist_name", StringType()),
        ]
    )
    expected_top_songs = spark_session.createDataFrame(top_song_data, schema=schema)

    # Compare the DataFrames
    assert (
        top_songs.collect() == expected_top_songs.collect()
    ), "DataFrames do not match"
