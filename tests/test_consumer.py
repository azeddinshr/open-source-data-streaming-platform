import json

import pytest

from ingestion import raw_consumer


class FakeMessage:
    def __init__(self, value, partition=0, offset=7):
        self._value = value
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


VALID_EVENT = {
    "id": 12345,
    "type": "PushEvent",
    "actor": {"login": "alice"},
    "repo": {"name": "acme/app"},
    "created_at": "2026-10-12T11:05:00Z",
    "ingested_at": "2026-10-12T11:06:00+00:00",
}


def test_parse_message_maps_fields_in_insert_order():
    row = raw_consumer.parse_message(FakeMessage(json.dumps(VALID_EVENT)))
    assert row[0] == "12345"
    assert row[1:5] == ("PushEvent", "acme/app", "alice", "2026-10-12T11:05:00Z")
    assert row[6:8] == (0, 7)


@pytest.mark.parametrize(
    "bad_value",
    [b"not json", b"{}", b"null", json.dumps({"id": 1}).encode()],
)
def test_parse_message_raises_the_errors_routed_to_the_dlq(bad_value):
    with pytest.raises((ValueError, KeyError, TypeError)):
        raw_consumer.parse_message(FakeMessage(bad_value))