from datetime import UTC, datetime

from batch import download_gharchive as dl


class NotFoundResponse:
    status_code = 404

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class NotFoundSession:
    def get(self, *args, **kwargs):
        return NotFoundResponse()


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def test_hour_range_is_inclusive_and_crosses_midnight():
    hours = list(dl.hour_range(utc(2026, 10, 12, 22), utc(2026, 10, 13, 1)))
    assert len(hours) == 4
    assert hours[-1] == utc(2026, 10, 13, 1)


def test_parse_hour_returns_utc():
    assert dl.parse_hour("2026-10-12T09") == utc(2026, 10, 12, 9)


def test_download_skips_existing_file_without_network(tmp_path, monkeypatch):
    monkeypatch.setattr(dl, "OUT_DIR", tmp_path)
    existing = tmp_path / "2026-10-12-9.json.gz"
    existing.write_bytes(b"x")
    assert dl.download_hour(utc(2026, 10, 12, 9), session=None) == existing


def test_download_returns_none_when_hour_is_not_published(tmp_path, monkeypatch):
    monkeypatch.setattr(dl, "OUT_DIR", tmp_path)
    assert dl.download_hour(utc(2026, 10, 12, 9), NotFoundSession()) is None