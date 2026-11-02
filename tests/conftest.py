"""Shared sample data for the test suite.

RAW_EDIT mirrors the shape of a real Wikimedia recentchange event.
BASE_TS is aligned to a minute boundary so window assertions are exact.
"""

from __future__ import annotations

import copy

import pandas as pd
import pytest

BASE_TS = 1_730_000_040  # divisible by 60: a clean minute boundary

RAW_EDIT: dict = {
    "$schema": "/mediawiki/recentchange/1.0.0",
    "meta": {
        "uri": "https://en.wikipedia.org/wiki/Example_Page",
        "request_id": "00000000-0000-0000-0000-000000000000",
        "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "dt": "2024-10-26T21:33:20Z",
        "domain": "en.wikipedia.org",
        "stream": "mediawiki.recentchange",
        "topic": "eqiad.mediawiki.recentchange",
        "partition": 0,
        "offset": 1234567,
    },
    "id": 987654321,
    "type": "edit",
    "namespace": 0,
    "title": "Example Page",
    "comment": "Fixed a typo",
    "timestamp": BASE_TS + 15,
    "user": "SomeEditor",
    "bot": False,
    "minor": False,
    "patrolled": True,
    "length": {"old": 1000, "new": 1250},
    "revision": {"old": 111111, "new": 222222},
    "server_url": "https://en.wikipedia.org",
    "server_name": "en.wikipedia.org",
    "server_script_path": "/w",
    "wiki": "enwiki",
    "parsedcomment": "Fixed a typo",
}


def raw_event(**overrides) -> dict:
    """A copy of RAW_EDIT with top-level fields replaced."""
    event = copy.deepcopy(RAW_EDIT)
    event.update(overrides)
    return event


def edits_frame(rows: list[tuple]) -> pd.DataFrame:
    """Build a normalized-edits frame from compact tuples.

    Each row: (timestamp_offset_seconds, wiki, title, user, bot, byte_change)
    """
    records = []
    for offset, wiki, title, user, bot, byte_change in rows:
        records.append(
            {
                "timestamp": BASE_TS + offset,
                "wiki": wiki,
                "title": title,
                "user": user,
                "bot": bot,
                "byte_change": byte_change,
            }
        )
    return pd.DataFrame.from_records(
        records, columns=["timestamp", "wiki", "title", "user", "bot", "byte_change"]
    )


@pytest.fixture
def sample_edits() -> pd.DataFrame:
    """Two minutes of edits across two wikis, humans and one bot."""
    return edits_frame(
        [
            # minute A
            (5, "enwiki", "Page A", "Alice", False, 300),
            (20, "enwiki", "Page A", "BotX", True, -50),
            (40, "enwiki", "Page B", "Alice", False, 100),
            (50, "dewiki", "Seite C", "Hans", False, 20),
            # minute B
            (65, "enwiki", "Page A", "Alice", False, 10),
        ]
    )
