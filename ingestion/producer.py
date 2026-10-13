import json
import logging
import os
import signal
import sys
import time
from collections import deque
from datetime import UTC, datetime

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "github-events")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

API_URL = "https://api.github.com/events"
PER_PAGE = 100
MAX_PAGES = 3  # GitHub exposes at most 300 recent events
SEEN_CACHE_SIZE = 20_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("github-producer")

running = True


def handle_shutdown(signum, frame):
    global running
    log.info("Shutdown signal received, finishing current cycle...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


class SeenIds:
    """Bounded memory of recently seen event IDs."""

    def __init__(self, maxlen):
        self._queue = deque(maxlen=maxlen)
        self._set = set()

    def __contains__(self, event_id):
        return event_id in self._set

    def add(self, event_id):
        if len(self._queue) == self._queue.maxlen:
            self._set.discard(self._queue[0])
        self._queue.append(event_id)
        self._set.add(event_id)


def build_session():
    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "realtime-open-source-data-platform",
    })
    if GITHUB_TOKEN:
        session.headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    else:
        log.warning("No GITHUB_TOKEN set: limited to 60 requests/hour")
    return session


def delivery_report(err, message):
    if err is not None:
        log.error("Delivery failed: %s", err)


def build_producer():
    return Producer({
        "bootstrap.servers": BROKER,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "lz4",
    })


def sleep_interruptible(seconds):
    end = time.time() + seconds
    while running and time.time() < end:
        time.sleep(0.5)


def rate_limit_wait(response):
    """Seconds to wait after a 403/429."""
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        return int(retry_after) + 1
    reset = response.headers.get("X-RateLimit-Reset")
    if reset:
        return max(int(reset) - int(time.time()), 0) + 1
    return 60


def fetch_events(session, etag):
    """
    Returns (events, new_etag, poll_interval, wait_override).
    events is None when nothing changed (304) or on error.
    """
    headers = {"If-None-Match": etag} if etag else {}
    all_events = []
    new_etag = etag
    poll_interval = 60

    for page in range(1, MAX_PAGES + 1):
        try:
            response = session.get(
                API_URL,
                params={"per_page": PER_PAGE, "page": page},
                headers=headers if page == 1 else {},
                timeout=15,
            )
        except requests.RequestException as exc:
            log.error("Network error: %s", exc)
            return None, etag, poll_interval, 30

        poll_interval = int(response.headers.get("X-Poll-Interval", poll_interval))

        if response.status_code == 304:
            log.info("304 Not Modified, nothing new")
            return None, etag, poll_interval, None

        if response.status_code in (403, 429):
            wait = rate_limit_wait(response)
            log.warning("Rate limited, waiting %ss", wait)
            return None, etag, poll_interval, wait

        if response.status_code >= 500:
            log.error("GitHub server error %s", response.status_code)
            return None, etag, poll_interval, 30

        response.raise_for_status()

        if page == 1:
            new_etag = response.headers.get("ETag", etag)

        batch = response.json()
        all_events.extend(batch)
        if len(batch) < PER_PAGE:
            break

    remaining = response.headers.get("X-RateLimit-Remaining")
    log.info("Fetched %d events (rate limit remaining: %s)", len(all_events), remaining)
    return all_events, new_etag, poll_interval, None


def main():
    session = build_session()
    producer = build_producer()
    seen = SeenIds(SEEN_CACHE_SIZE)
    etag = None
    total_sent = 0

    log.info("Starting producer -> %s / topic=%s", BROKER, TOPIC)

    while running:
        events, etag, poll_interval, wait_override = fetch_events(session, etag)

        if events:
            # oldest first so offsets roughly follow event time
            new_events = [e for e in reversed(events) if e["id"] not in seen]
            ingested_at = datetime.now(UTC).isoformat()

            for event in new_events:
                seen.add(event["id"])
                event["ingested_at"] = ingested_at
                producer.produce(
                    topic=TOPIC,
                    key=event["repo"]["name"],
                    value=json.dumps(event),
                    callback=delivery_report,
                )
                producer.poll(0)

            total_sent += len(new_events)
            log.info(
                "Published %d new events (%d duplicates skipped, %d total)",
                len(new_events), len(events) - len(new_events), total_sent,
            )

        sleep_interruptible(wait_override if wait_override else poll_interval)

    log.info("Flushing pending messages...")
    producer.flush(10)
    log.info("Stopped cleanly. Total published: %d", total_sent)


if __name__ == "__main__":
    sys.exit(main())