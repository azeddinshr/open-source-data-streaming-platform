from types import SimpleNamespace

import requests

from ingestion import producer


def make_response(status=200, headers=None, body=None):
    return SimpleNamespace(
        status_code=status,
        headers=headers or {},
        json=lambda: body if body is not None else [],
        raise_for_status=lambda: None,
    )


class FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"params": params, "headers": headers})
        return self._responses.pop(0)


def test_seen_ids_remembers_recent_ids():
    seen = producer.SeenIds(maxlen=3)
    for event_id in ("a", "b", "c"):
        seen.add(event_id)
    assert "a" in seen and "c" in seen


def test_seen_ids_evicts_oldest_when_full():
    seen = producer.SeenIds(maxlen=2)
    for event_id in ("a", "b", "c"):
        seen.add(event_id)
    assert "a" not in seen
    assert "b" in seen and "c" in seen


def test_rate_limit_wait_prefers_retry_after():
    response = make_response(403, {"Retry-After": "30"})
    assert producer.rate_limit_wait(response) == 31


def test_rate_limit_wait_uses_reset_header(monkeypatch):
    monkeypatch.setattr(producer, "time", SimpleNamespace(time=lambda: 1_000))
    response = make_response(403, {"X-RateLimit-Reset": "1060"})
    assert producer.rate_limit_wait(response) == 61


def test_rate_limit_wait_defaults_to_sixty_seconds():
    assert producer.rate_limit_wait(make_response(429, {})) == 60


def test_fetch_events_returns_none_on_304():
    session = FakeSession([make_response(304, {"X-Poll-Interval": "60"})])
    events, etag, interval, wait = producer.fetch_events(session, "etag-1")
    assert events is None
    assert etag == "etag-1"
    assert interval == 60
    assert wait is None
    assert session.calls[0]["headers"] == {"If-None-Match": "etag-1"}


def test_fetch_events_backs_off_when_rate_limited():
    session = FakeSession([make_response(403, {"Retry-After": "10"})])
    events, _, _, wait = producer.fetch_events(session, None)
    assert events is None
    assert wait == 11


def test_fetch_events_collects_pages_until_a_short_page():
    full_page = [{"id": str(i)} for i in range(producer.PER_PAGE)]
    session = FakeSession([
        make_response(200, {"ETag": "abc", "X-Poll-Interval": "60"}, full_page),
        make_response(200, {"X-Poll-Interval": "60"}, [{"id": "last"}]),
    ])
    events, etag, _, _ = producer.fetch_events(session, None)
    assert len(events) == producer.PER_PAGE + 1
    assert etag == "abc"
    assert len(session.calls) == 2


def test_fetch_events_survives_network_errors():
    class Down:
        def get(self, *args, **kwargs):
            raise requests.RequestException("connection refused")

    events, etag, _, wait = producer.fetch_events(Down(), "etag-1")
    assert events is None
    assert etag == "etag-1"
    assert wait == 30