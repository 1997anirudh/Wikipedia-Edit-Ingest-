"""Unit tests for the producer's SSE reader and its parse integration.

The fake response only implements iter_lines, which is all StreamReader
uses, so no network or Kafka is involved.
"""

from __future__ import annotations

import json

from producer.producer import StreamReader
from tests.conftest import raw_event


class FakeResponse:
    def __init__(self, lines: list[str]) -> None:
        self._lines = lines

    def iter_lines(self, decode_unicode: bool = True):
        yield from self._lines


def _sse_lines(*events: dict) -> list[str]:
    lines: list[str] = []
    for event in events:
        lines.append("event: message")
        lines.append(f"data: {json.dumps(event)}")
        lines.append("")
    return lines


def test_reads_data_payloads() -> None:
    reader = StreamReader(FakeResponse(_sse_lines(raw_event(), raw_event())))
    events = list(reader.events())
    assert len(events) == 2
    assert events[0]["title"] == "Example Page"


def test_skips_malformed_payloads() -> None:
    lines = ["data: {not json", "", *_sse_lines(raw_event())]
    events = list(StreamReader(FakeResponse(lines)).events())
    assert len(events) == 1


def test_ignores_comments_and_heartbeats() -> None:
    lines = [": heartbeat", "", *_sse_lines(raw_event())]
    events = list(StreamReader(FakeResponse(lines)).events())
    assert len(events) == 1
