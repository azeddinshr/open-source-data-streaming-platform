import json
import logging
import os
import signal
import sys

import psycopg
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "github-events")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "github-events-dlq")
GROUP_ID = "raw-postgres-loader"

PG_DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=analytics user=de_user password=de_password",
)

BATCH_SIZE = 500
BATCH_TIMEOUT = 5  # seconds

INSERT_SQL = """
    INSERT INTO raw.github_events (
        event_id, event_type, repo_name, actor_login,
        created_at, ingested_at, kafka_partition, kafka_offset, payload
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING
"""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("raw-consumer")

running = True


def handle_shutdown(signum, frame):
    global running
    log.info("Shutdown signal received...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def parse_message(msg):
    """Turn a Kafka message into a DB row. Raises on malformed input."""
    event = json.loads(msg.value())
    return (
        str(event["id"]),
        event["type"],
        event["repo"]["name"],
        event["actor"]["login"],
        event["created_at"],
        event.get("ingested_at"),
        msg.partition(),
        msg.offset(),
        Jsonb(event),
    )


def send_to_dlq(dlq_producer, msg, error):
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers={
            "error": str(error)[:500],
            "source_topic": msg.topic(),
            "source_offset": str(msg.offset()),
        },
    )
    dlq_producer.poll(0)


def main():
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # we commit manually, after Postgres commits
    })
    dlq_producer = Producer({"bootstrap.servers": BROKER})
    consumer.subscribe([TOPIC])

    total_rows = total_dlq = 0
    log.info("Consuming %s -> raw.github_events (group=%s)", TOPIC, GROUP_ID)

    with psycopg.connect(PG_DSN) as conn:
        while running:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=BATCH_TIMEOUT)
            if not messages:
                continue

            rows = []
            for msg in messages:
                if msg.error():
                    log.error("Kafka error: %s", msg.error())
                    continue
                try:
                    rows.append(parse_message(msg))
                except (ValueError, KeyError, TypeError) as exc:
                    log.warning("Bad message at offset %s -> DLQ (%s)", msg.offset(), exc)
                    send_to_dlq(dlq_producer, msg, exc)
                    total_dlq += 1

            inserted = 0
            if rows:
                with conn.cursor() as cur:
                    cur.executemany(INSERT_SQL, rows)
                    inserted = cur.rowcount
                conn.commit()  # 1) Postgres first

            dlq_producer.flush(5)
            consumer.commit(asynchronous=False)  # 2) then Kafka offsets

            total_rows += inserted
            log.info(
                "Batch: %d messages, %d inserted, %d already existed (total inserted: %d, DLQ: %d)",
                len(messages), inserted, len(rows) - inserted, total_rows, total_dlq,
            )

    consumer.close()
    log.info("Stopped cleanly.")


if __name__ == "__main__":
    sys.exit(main())