"""Producer: Wikimedia EventStreams -> Kafka (Redpanda).

Connects to the Wikimedia recentchange Server-Sent Events stream, parses
each event with common.events.parse_recentchange, and publishes the
normalized edit as JSON to the `wikipedia.edits` topic.

The Wikimedia stream is firehose-scale (~250k edits/day across all
wikis) and the connection drops regularly, so the read loop reconnects
with exponential backoff. Delivery to Kafka is asynchronous with an
error callback; the producer is flushed on shutdown.

Configuration (environment variables):
    KAFKA_BROKER        default localhost:19092 (Redpanda external listener)
                        docker-compose overrides this to redpanda:9092
    KAFKA_TOPIC         default wikipedia.edits
    WIKIMEDIA_STREAM_URL default https://stream.wikimedia.org/v2/stream/recentchange
    USER_AGENT          Wikimedia asks API clients to identify themselves
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from collections.abc import Iterator

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Make the repo-root `common` package importable when run as a script or
# inside the producer container (where the layout is /app/producer/...).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.events import DEFAULT_TOPIC, parse_recentchange  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("producer")

STREAM_URL = os.environ.get(
    "WIKIMEDIA_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
)
USER_AGENT = os.environ.get(
    "USER_AGENT", "wikipedia-edit-ingest/1.0 (portfolio project)"
)

BACKOFF_INITIAL_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0
LOG_EVERY_N_EVENTS = 1000


class StreamReader:
    """Reads `data:` payloads from a Server-Sent Events response."""

    def __init__(self, response: requests.Response) -> None:
        self._response = response

    def events(self) -> Iterator[dict]:
        buffer: list[str] = []
        for line in self._response.iter_lines(decode_unicode=True):
            if line is None:
                continue
            if line == "":
                # Blank line terminates an SSE event.
                if buffer:
                    payload = "\n".join(buffer)
                    buffer = []
                    try:
                        yield json.loads(payload)
                    except json.JSONDecodeError:
                        logger.warning("skipping malformed SSE payload")
            elif line.startswith("data:"):
                buffer.append(line[len("data:") :].lstrip())
            # Other SSE fields (event:, id:, comments) are ignored.


def connect_stream() -> requests.Response:
    response = requests.get(
        STREAM_URL,
        stream=True,
        timeout=(10, 30),
        headers={"User-Agent": USER_AGENT, "Accept": "text/event-stream"},
    )
    response.raise_for_status()
    return response


def build_producer(broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        linger_ms=100,
        retries=5,
    )


def run() -> None:
    broker = os.environ.get("KAFKA_BROKER", "localhost:19092")
    topic = os.environ.get("KAFKA_TOPIC", DEFAULT_TOPIC)

    stopping = False

    def handle_signal(signum: int, _frame: object) -> None:
        nonlocal stopping
        logger.info("received signal %s, shutting down", signum)
        stopping = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    producer = build_producer(broker)
    logger.info("connected to Kafka at %s, topic %s", broker, topic)

    def on_error(exc: KafkaError) -> None:
        logger.error("failed to deliver message: %s", exc)

    published = 0
    backoff = BACKOFF_INITIAL_SECONDS
    try:
        while not stopping:
            try:
                logger.info("connecting to %s", STREAM_URL)
                with connect_stream() as response:
                    backoff = BACKOFF_INITIAL_SECONDS
                    for raw_event in StreamReader(response).events():
                        if stopping:
                            break
                        edit = parse_recentchange(raw_event)
                        if edit is None:
                            continue
                        future = producer.send(
                            topic, key=edit.wiki, value=edit.to_json()
                        )
                        future.add_errback(on_error)
                        published += 1
                        if published % LOG_EVERY_N_EVENTS == 0:
                            logger.info("published %d edits", published)
            except requests.RequestException as exc:
                logger.warning("stream connection failed: %s", exc)
            if not stopping:
                logger.info("reconnecting in %.0fs", backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
    finally:
        logger.info("flushing producer (%d edits published)", published)
        producer.flush(timeout=10)
        producer.close(timeout=10)


if __name__ == "__main__":
    run()
