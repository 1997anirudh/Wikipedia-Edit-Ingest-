"""Streamlit dashboard over the DuckDB aggregates written by Spark.

Reads the DuckDB file read-only and refreshes every REFRESH_SECONDS.
DuckDB allows a single writer (the Spark job) alongside read-only
connections, but a read can still collide with a write checkpoint, so
queries retry briefly and the UI degrades to a "waiting for data" state
instead of crashing.

Configuration (environment variables):
    DUCKDB_PATH       default data/duckdb/wikipedia.duckdb
    REFRESH_SECONDS   default 15
"""

from __future__ import annotations

import os
import time

import duckdb
import pandas as pd
import streamlit as st

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "data/duckdb/wikipedia.duckdb")
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "15"))
TABLES = ("wiki_minute_stats", "top_pages", "top_editors")


def query(sql: str, attempts: int = 3) -> pd.DataFrame:
    """Run a read-only query with a short retry for write contention."""
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            con = duckdb.connect(DUCKDB_PATH, read_only=True)
            try:
                existing = {
                    row[0]
                    for row in con.execute("SHOW TABLES").fetchall()
                }
                if not set(TABLES) & existing:
                    return pd.DataFrame()
                return con.execute(sql).df()
            finally:
                con.close()
        except duckdb.Error as exc:
            last_error = exc
            time.sleep(0.5 * (attempt + 1))
    if last_error is not None:
        raise last_error
    return pd.DataFrame()


@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def live_dashboard() -> None:
    totals = query(
        """
        SELECT
            coalesce(sum(edits), 0)     AS total_edits,
            coalesce(sum(bot_edits), 0) AS bot_edits,
            coalesce(sum(human_edits), 0) AS human_edits
        FROM wiki_minute_stats
        """
    )
    if totals.empty or int(totals.loc[0, "total_edits"]) == 0:
        st.info(
            "Waiting for data. Make sure the producer and streaming job "
            "are running; aggregates appear within about a minute."
        )
        return

    total = int(totals.loc[0, "total_edits"])
    bot = int(totals.loc[0, "bot_edits"])
    human = int(totals.loc[0, "human_edits"])

    col1, col2, col3 = st.columns(3)
    col1.metric("Total edits processed", f"{total:,}")
    col2.metric("Bot edits", f"{bot:,} ({bot / total:.0%})" if total else "0")
    col3.metric("Human edits", f"{human:,} ({human / total:.0%})" if total else "0")

    per_minute = query(
        """
        SELECT window_start, sum(edits) AS edits,
               sum(bot_edits) AS bot_edits, sum(human_edits) AS human_edits
        FROM wiki_minute_stats
        GROUP BY window_start
        ORDER BY window_start
        """
    )
    st.subheader("Edits per minute")
    st.line_chart(
        per_minute.set_index("window_start")[["edits", "bot_edits", "human_edits"]]
    )

    st.subheader("Bot vs human (all time)")
    split = pd.DataFrame(
        {"kind": ["Bot", "Human"], "edits": [bot, human]}
    ).set_index("kind")
    st.bar_chart(split)

    latest_pages = query(
        """
        SELECT wiki, title, edits FROM top_pages
        WHERE window_start = (SELECT max(window_start) FROM top_pages)
        ORDER BY edits DESC
        LIMIT 10
        """
    )
    latest_editors = query(
        """
        SELECT "user" AS editor, edits FROM top_editors
        WHERE window_start = (SELECT max(window_start) FROM top_editors)
        ORDER BY edits DESC
        LIMIT 10
        """
    )
    left, right = st.columns(2)
    with left:
        st.subheader("Top pages (latest minute)")
        st.dataframe(latest_pages, use_container_width=True, hide_index=True)
    with right:
        st.subheader("Top editors (latest minute)")
        st.dataframe(latest_editors, use_container_width=True, hide_index=True)

    top_wikis = query(
        """
        SELECT wiki, sum(edits) AS edits, sum(bot_edits) AS bot_edits,
               sum(human_edits) AS human_edits
        FROM wiki_minute_stats
        GROUP BY wiki
        ORDER BY edits DESC
        LIMIT 15
        """
    )
    st.subheader("Top wikis (all time)")
    st.dataframe(top_wikis, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Wikipedia Edit Stream", layout="wide")
    st.title("Wikipedia edits, live")
    st.caption(
        "Real-time pipeline: Wikimedia EventStreams -> Redpanda -> Spark "
        f"Structured Streaming -> DuckDB. Refreshing every {REFRESH_SECONDS}s. "
        "Times are UTC."
    )
    if not os.path.exists(DUCKDB_PATH):
        st.info(
            f"DuckDB file not found yet at {DUCKDB_PATH}. "
            "Start the pipeline with `docker compose up` and wait a minute."
        )
        return
    live_dashboard()


if __name__ == "__main__":
    main()
