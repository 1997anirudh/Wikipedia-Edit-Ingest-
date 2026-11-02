"""Unit tests for common.aggregations (the pandas mirror of the Spark job).

Semantics under test: 1-minute tumbling windows in UTC, bot vs human
split, byte-change statistics, and deterministic top-N ranking.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from common.aggregations import (
    compute_top_editors,
    compute_top_pages,
    compute_wiki_minute_stats,
)
from tests.conftest import edits_frame


def _row_for(df: pd.DataFrame, **match) -> pd.Series:
    rows = df
    for column, value in match.items():
        rows = rows[rows[column] == value]
    assert len(rows) == 1, f"expected exactly one row for {match}"
    return rows.iloc[0]


def test_wiki_minute_stats_counts_and_splits(sample_edits: pd.DataFrame) -> None:
    stats = compute_wiki_minute_stats(sample_edits)
    minute_a = stats[stats["window_start"] == stats["window_start"].min()]

    enwiki_a = _row_for(minute_a, wiki="enwiki")
    assert enwiki_a["edits"] == 3
    assert enwiki_a["bot_edits"] == 1
    assert enwiki_a["human_edits"] == 2
    assert enwiki_a["total_byte_change"] == 300 - 50 + 100
    assert enwiki_a["abs_byte_change"] == 300 + 50 + 100
    assert enwiki_a["avg_byte_change"] == pytest.approx((300 - 50 + 100) / 3)

    dewiki_a = _row_for(minute_a, wiki="dewiki")
    assert dewiki_a["edits"] == 1
    assert dewiki_a["bot_edits"] == 0
    assert dewiki_a["human_edits"] == 1


def test_wiki_minute_stats_windows_are_one_minute_apart(
    sample_edits: pd.DataFrame,
) -> None:
    stats = compute_wiki_minute_stats(sample_edits)
    starts = sorted(stats["window_start"].unique())
    assert len(starts) == 2
    assert (starts[1] - starts[0]) == pd.Timedelta(minutes=1)
    first = stats.iloc[0]
    assert first["window_end"] - first["window_start"] == pd.Timedelta(minutes=1)


def test_top_pages_ranks_within_each_window(sample_edits: pd.DataFrame) -> None:
    top = compute_top_pages(sample_edits, n=2)
    minute_a = top[top["window_start"] == top["window_start"].min()]
    # Page A leads; the 1-edit tie between Seite C (dewiki) and Page B
    # (enwiki) is broken alphabetically by wiki, so Seite C takes slot 2.
    assert list(minute_a["title"]) == ["Page A", "Seite C"]
    assert list(minute_a["edits"]) == [2, 1]


def test_top_pages_respects_n(sample_edits: pd.DataFrame) -> None:
    top = compute_top_pages(sample_edits, n=1)
    assert set(top["title"]) == {"Page A"}
    assert len(top) == 2  # one winner per window, two windows


def test_top_editors(sample_edits: pd.DataFrame) -> None:
    top = compute_top_editors(sample_edits, n=10)
    minute_a = top[top["window_start"] == top["window_start"].min()]
    alice = _row_for(minute_a, user="Alice")
    assert alice["edits"] == 2
    assert set(minute_a["user"]) == {"Alice", "BotX", "Hans"}


def test_empty_input_returns_empty_frames_with_columns() -> None:
    empty = edits_frame([])
    stats = compute_wiki_minute_stats(empty)
    assert stats.empty
    assert "bot_edits" in stats.columns
    assert compute_top_pages(empty).empty
    assert compute_top_editors(empty).empty


def test_duckdb_roundtrip_matches_dashboard_queries(
    sample_edits: pd.DataFrame, tmp_path
) -> None:
    """The stats frame must load into DuckDB with the table shape and
    column names the dashboard queries against wiki_minute_stats."""
    stats = compute_wiki_minute_stats(sample_edits)
    db_path = str(tmp_path / "test.duckdb")
    con = duckdb.connect(db_path)
    con.register("stats", stats)
    con.execute(
        """
        CREATE TABLE wiki_minute_stats AS
        SELECT window_start, window_end, wiki,
               edits::BIGINT AS edits, bot_edits::BIGINT AS bot_edits,
               human_edits::BIGINT AS human_edits,
               total_byte_change::BIGINT AS total_byte_change,
               abs_byte_change::BIGINT AS abs_byte_change,
               avg_byte_change::DOUBLE AS avg_byte_change
        FROM stats
        """
    )
    total, bot, human = con.execute(
        "SELECT sum(edits), sum(bot_edits), sum(human_edits) FROM wiki_minute_stats"
    ).fetchone()
    con.close()
    assert (total, bot, human) == (5, 1, 4)
