import argparse
import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests

BASE_URL = "https://data.gharchive.org"
OUT_DIR = Path("data/raw/gharchive")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gharchive-download")


def hour_range(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(hours=1)


def download_hour(hour, session):
    # GH Archive file names use a non-padded hour: 2026-10-12-9.json.gz
    name = f"{hour:%Y-%m-%d}-{hour.hour}.json.gz"
    target = OUT_DIR / name
    if target.exists():
        log.info("Skip %s (already downloaded)", name)
        return target

    tmp = target.with_suffix(".part")
    for attempt in range(1, 4):
        try:
            with session.get(f"{BASE_URL}/{name}", stream=True, timeout=60) as resp:
                if resp.status_code == 404:
                    log.warning("%s not available yet (404)", name)
                    return None
                resp.raise_for_status()
                with open(tmp, "wb") as f:
                    f.writelines(resp.iter_content(chunk_size=1 << 20))
            tmp.rename(target)  # atomic: never leave half-written files
            log.info("Downloaded %s (%.1f MB)", name, target.stat().st_size / 1e6)
            return target
        except requests.RequestException as exc:
            log.error("Attempt %d failed for %s: %s", attempt, name, exc)
    return None


def parse_hour(value):
    return datetime.strptime(value, "%Y-%m-%dT%H").replace(tzinfo=UTC)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="UTC hour, e.g. 2026-10-12T11")
    parser.add_argument("--end", required=True, help="UTC hour, inclusive")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    failures = 0
    with requests.Session() as session:
        for hour in hour_range(parse_hour(args.start), parse_hour(args.end)):
            if download_hour(hour, session) is None:
                failures += 1

    if failures:
        log.error("%d hour(s) could not be downloaded", failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
