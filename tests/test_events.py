"""Unit tests for common.events.parse_recentchange."""

from __future__ import annotations

import json

from common.events import WikiEdit, parse_recentchange
from tests.conftest import BASE_TS, raw_event

# The producer's JSON keys are the contract the Spark job's EVENT_SCHEMA
# (streaming/streaming_job.py) is built against. If a field is added or
# renamed, both places change together.
EXPECTED_FIELDS = {
    "event_id",
    "type",
    "wiki",
    "title",
    "user",
    "bot",
    "minor",
    "namespace",
    "timestamp",
    "comment",
    "byte_change",
    "old_length",
    "new_length",
    "revision_old",
    "revision_new",
    "server_name",
}


def test_parses_a_standard_edit() -> None:
    edit = parse_recentchange(raw_event())
    assert isinstance(edit, WikiEdit)
    assert edit.event_id == 987654321
    assert edit.type == "edit"
    assert edit.wiki == "enwiki"
    assert edit.title == "Example Page"
    assert edit.user == "SomeEditor"
    assert edit.bot is False
    assert edit.minor is False
    assert edit.namespace == 0
    assert edit.timestamp == BASE_TS + 15
    assert edit.comment == "Fixed a typo"
    assert edit.server_name == "en.wikipedia.org"


def test_byte_change_is_new_minus_old() -> None:
    edit = parse_recentchange(raw_event())
    assert edit is not None
    assert edit.old_length == 1000
    assert edit.new_length == 1250
    assert edit.byte_change == 250


def test_negative_byte_change_for_removals() -> None:
    edit = parse_recentchange(raw_event(length={"old": 900, "new": 400}))
    assert edit is not None
    assert edit.byte_change == -500


def test_new_page_events_are_kept() -> None:
    edit = parse_recentchange(
        raw_event(type="new", length={"old": 0, "new": 640}, revision={"old": None, "new": 5})
    )
    assert edit is not None
    assert edit.type == "new"
    assert edit.byte_change == 640
    assert edit.revision_old is None
    assert edit.revision_new == 5


def test_missing_length_defaults_byte_change_to_zero() -> None:
    event = raw_event()
    del event["length"]
    edit = parse_recentchange(event)
    assert edit is not None
    assert edit.byte_change == 0
    assert edit.old_length is None
    assert edit.new_length is None


def test_bot_flag_passes_through() -> None:
    edit = parse_recentchange(raw_event(bot=True, user="CleanupBot"))
    assert edit is not None
    assert edit.bot is True


def test_non_edit_types_are_dropped() -> None:
    for event_type in ("log", "categorize", "external"):
        assert parse_recentchange(raw_event(type=event_type)) is None


def test_missing_required_fields_are_dropped() -> None:
    for field in ("wiki", "title", "timestamp"):
        event = raw_event()
        del event[field]
        assert parse_recentchange(event) is None, field


def test_non_dict_input_is_dropped() -> None:
    assert parse_recentchange("not an event") is None  # type: ignore[arg-type]
    assert parse_recentchange(None) is None  # type: ignore[arg-type]


def test_json_schema_matches_pipeline_contract() -> None:
    edit = parse_recentchange(raw_event())
    assert edit is not None
    payload = json.loads(edit.to_json())
    assert set(payload.keys()) == EXPECTED_FIELDS
    # Values must stay JSON-native so the producer can publish them as-is.
    assert isinstance(payload["timestamp"], int)
    assert isinstance(payload["bot"], bool)
