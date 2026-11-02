"""Parsing and normalization for Wikimedia recentchange events.

The Wikimedia EventStreams `recentchange` feed emits one JSON object per
change across all wikis (edits, new pages, log actions, categorizations,
...). This module turns a raw feed event into the flat, normalized record
that flows through the rest of the pipeline:

    raw recentchange JSON -> parse_recentchange() -> WikiEdit -> Kafka

Both the producer (which publishes to Kafka) and the Spark job (whose
schema mirrors `WikiEdit`) depend on the field names defined here, and the
unit tests exercise this module directly, so it must stay free of any
Kafka/Spark imports.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

# Default Kafka topic for normalized edit events.
DEFAULT_TOPIC = "wikipedia.edits"

# Only content changes are interesting for this pipeline. The raw feed also
# carries "log", "categorize" and "external" events, which are dropped.
EDIT_TYPES = frozenset({"edit", "new"})


@dataclass(frozen=True)
class WikiEdit:
    """One normalized Wikipedia edit, ready to serialize to Kafka."""

    event_id: int | None  # recentchange id; absent for some wikis
    type: str  # "edit" or "new"
    wiki: str  # e.g. "enwiki", "dewiki"
    title: str  # page title
    user: str  # editor username or IP for anonymous edits
    bot: bool
    minor: bool
    namespace: int  # 0 = article, 6 = file, 14 = category, ...
    timestamp: int  # event time, epoch seconds (UTC)
    comment: str  # edit summary, may be empty
    byte_change: int  # new_length - old_length (0 for new pages' baseline)
    old_length: int | None
    new_length: int | None
    revision_old: int | None
    revision_new: int | None
    server_name: str  # e.g. "en.wikipedia.org"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


def parse_recentchange(raw: dict[str, Any]) -> WikiEdit | None:
    """Normalize one raw recentchange event.

    Returns None for events the pipeline does not process: non-edit event
    types and events missing the fields the aggregations depend on
    (wiki, title, user, timestamp).
    """
    if not isinstance(raw, dict):
        return None

    event_type = raw.get("type")
    if event_type not in EDIT_TYPES:
        return None

    wiki = raw.get("wiki")
    title = raw.get("title")
    user = raw.get("user")
    timestamp = raw.get("timestamp")
    if not wiki or not title or user is None or timestamp is None:
        return None

    length = raw.get("length") or {}
    old_length = length.get("old")
    new_length = length.get("new")
    if old_length is not None and new_length is not None:
        byte_change = int(new_length) - int(old_length)
    else:
        byte_change = 0

    revision = raw.get("revision") or {}

    return WikiEdit(
        event_id=raw.get("id"),
        type=str(event_type),
        wiki=str(wiki),
        title=str(title),
        user=str(user),
        bot=bool(raw.get("bot", False)),
        minor=bool(raw.get("minor", False)),
        namespace=int(raw.get("namespace", 0)),
        timestamp=int(timestamp),
        comment=str(raw.get("comment") or ""),
        byte_change=byte_change,
        old_length=old_length,
        new_length=new_length,
        revision_old=revision.get("old"),
        revision_new=revision.get("new"),
        server_name=str(raw.get("server_name") or ""),
    )
