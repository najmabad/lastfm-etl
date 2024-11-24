import logging.config

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logging.config.fileConfig(
    fname="config/logging.config.ini", disable_existing_loggers=False
)
logger = logging.getLogger(__name__)


def get_top_n_rows(df: DataFrame, order_by_col: str, n: int) -> DataFrame:
    """Retrieve the top n rows of a DataFrame ordered by 'order_by_col' in descending order.

    :param df: DataFrame to be sorted.
    :param order_by_col: The column name to sort the DataFrame by in descending order.
    :param n: The number of top rows to return.
    """
    logger.info(f"Retrieving top {n} rows based on '{order_by_col}'.")
    return df.orderBy(F.desc(order_by_col)).limit(n)


class SessionSongAnalyzer:
    """Analyzes top songs from session data"""

    def __init__(
        self,
        song_id: str,
        unique_session_id: list[str],
        additional_cols: list[str] = None,
    ):
        self.song_id = song_id
        self.unique_session_id = unique_session_id
        self.additional_cols = additional_cols if additional_cols else []

    def create_dataframe(
        self, events_with_sessions: DataFrame, top_n_sessions: DataFrame
    ) -> DataFrame:
        """Return a DataFrame containing the songs in the top n sessions."""
        events_with_sessions = self._filter_non_null_song_id(events_with_sessions)

        events_in_top_n_sessions = self._filter_top_sessions(
            events_with_sessions, top_n_sessions
        )
        song_count = self._count_songs(events_in_top_n_sessions)
        return song_count

    def _filter_non_null_song_id(self, events_with_sessions: DataFrame) -> DataFrame:
        return events_with_sessions.filter(
            events_with_sessions[self.song_id].isNotNull()
        )

    def _filter_top_sessions(
        self, events_with_sessions: DataFrame, top_n_sessions: DataFrame
    ) -> DataFrame:
        """Filter events to include only those that occur in the top sessions."""
        return events_with_sessions.join(
            top_n_sessions.select(self.unique_session_id),
            on=self.unique_session_id,
            how="inner",
        )

    def _count_songs(self, events_in_top_n_sessions: DataFrame) -> DataFrame:
        """Group by song_id and additional columns, and count occurrences."""
        group_by_cols = [self.song_id] + self.additional_cols

        return events_in_top_n_sessions.groupBy(group_by_cols).agg(
            F.count("*").alias("song_count")
        )


def get_top_songs_in_longest_session(
    events_with_sessions: DataFrame,
    sessions: DataFrame,
    session_number: int,
    song_number: int,
) -> DataFrame:
    # Get the n longest sessions
    top_sessions = get_top_n_rows(
        df=sessions, order_by_col="session_duration", n=session_number
    )

    # Analyze songs within the top n longest sessions to find the most popular
    session_song_analyzer = SessionSongAnalyzer(
        song_id="musicbrainz_track_id",
        unique_session_id=["user_id", "session_id"],
        additional_cols=["track_name", "artist_name"],
    )
    songs_in_top_sessions = session_song_analyzer.create_dataframe(
        events_with_sessions,
        top_sessions,
    )

    # Get the top n songs
    top_songs = get_top_n_rows(
        df=songs_in_top_sessions, order_by_col="song_count", n=song_number
    )

    return top_songs.select(
        [session_song_analyzer.song_id] + session_song_analyzer.additional_cols
    )
