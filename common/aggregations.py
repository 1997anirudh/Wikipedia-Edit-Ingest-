"""Windowed aggregation logic over normalized wiki edits.

These pandas implementations mirror, one for one, the aggregations the
Spark Structured Streaming job computes (see streaming/streaming_job.py)
and the DuckDB tables it writes:

    wiki_minute_stats  <- compute_wiki_minute_stats()
    top_pages          <- compute_top_pages()
    top_editors        <- compute_top_editors()

They exist so the aggregation semantics (1-minute tumbling windows,
bot/human split, top-N ranking) can be unit-tested quickly without a
Spark cluster. If you change the semantics, change both places.

Input frames are expected to carry the WikiEdit columns (see
common/events.py): timestamp (epoch seconds), wiki, title, user, bot,
byte_change.
"""

from __future__ import annotations

import pandas as pd

WINDOW = "1min"


def _with_window(df: pd.DataFrame) -> pd.DataFrame:
    """Add window_start/window_end columns (UTC, 1-minute tumbling)."""
    out = df.copy()
    event_time = pd.to_datetime(out["timestamp"], unit="s", utc=True)
    out["window_start"] = event_time.dt.floor(WINDOW)
    out["window_end"] = out["window_start"] + pd.Timedelta(minutes=1)
    return out


def compute_wiki_minute_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Edits per wiki per minute, with bot/human and byte-change stats."""
    columns = [
        "window_start", "window_end", "wiki", "edits", "bot_edits",
        "human_edits", "total_byte_change", "abs_byte_change",
        "avg_byte_change",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)
    w = _with_window(df)
    w["is_bot"] = w["bot"].astype(int)
    w["abs_byte_change_col"] = w["byte_change"].abs()
    grouped = w.groupby(["window_start", "window_end", "wiki"], as_index=False).agg(
        edits=("timestamp", "size"),
        bot_edits=("is_bot", "sum"),
        total_byte_change=("byte_change", "sum"),
        abs_byte_change=("abs_byte_change_col", "sum"),
        avg_byte_change=("byte_change", "mean"),
    )
    grouped["human_edits"] = grouped["edits"] - grouped["bot_edits"]
    return (
        grouped[columns]
        .sort_values(["window_start", "wiki"])
        .reset_index(drop=True)
    )


def _top_n(df: pd.DataFrame, group_cols: list[str], n: int) -> pd.DataFrame:
    """Rank groups by edit count within each window and keep the top n.

    Ties are broken alphabetically by the group columns so results are
    deterministic.
    """
    columns = ["window_start", "window_end", *group_cols, "edits"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    w = _with_window(df)
    counts = (
        w.groupby(["window_start", "window_end", *group_cols], as_index=False)
        .size()
        .rename(columns={"size": "edits"})
    )
    counts = counts.sort_values(
        ["window_start", "edits", *group_cols],
        ascending=[True, False, *([True] * len(group_cols))],
    )
    return counts.groupby("window_start", group_keys=False).head(n).reset_index(drop=True)


def compute_top_pages(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Most-edited pages per 1-minute window (grouped by wiki + title)."""
    return _top_n(df, ["wiki", "title"], n)


def compute_top_editors(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Most active editors per 1-minute window."""
    return _top_n(df, ["user"], n)
