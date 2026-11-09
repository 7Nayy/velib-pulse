"""Shared helpers for the Vélib' Pulse producers.

- Kafka producer factory (idempotent, acks=all, zstd)
- HTTP GET with exponential back-off
- Local NDJSON.gz archive (safety net: raw events are kept even before
  Kafka/HDFS exist, and can be replayed into Kafka later)
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import requests

SCHEMA_VERSION = 1
USER_AGENT = "velib-pulse/1.0 (ESILV big data student project)"

log = logging.getLogger("velib-pulse")


def setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def epoch_to_iso(ts: int | float) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


# --------------------------------------------------------------------------- HTTP
def get_json(session: requests.Session, url: str, *, params: Optional[dict] = None,
             timeout: float = 15.0, max_wait: float = 300.0) -> Any:
    """GET a JSON document, retrying with exponential back-off + jitter.

    Retries on network errors, 5xx and 429. Gives up only on other 4xx.
    """
    wait = 2.0
    while True:
        try:
            resp = session.get(url, params=params, timeout=timeout,
                               headers={"User-Agent": USER_AGENT})
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else None
            if code is not None and 400 <= code < 500 and code != 429:
                raise
            log.warning("GET %s failed (%s), retrying in %.0fs", url, exc, wait)
        except (requests.ConnectionError, requests.Timeout, ValueError) as exc:
            log.warning("GET %s failed (%s), retrying in %.0fs", url, exc, wait)
        time.sleep(wait + random.uniform(0, 1))
        wait = min(wait * 2, max_wait)


# --------------------------------------------------------------------------- Kafka
class KafkaSink:
    """Thin wrapper around confluent_kafka.Producer with delivery accounting."""

    def __init__(self, bootstrap: str, client_id: str):
        from confluent_kafka import Producer  # imported lazily: archive-only mode needs no Kafka

        self.delivered = 0
        self.failed = 0
        self._p = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 50,
            "batch.size": 262144,
            "delivery.timeout.ms": 120000,
        })

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed: %s", err)
        else:
            self.delivered += 1

    def send(self, topic: str, key: str, value: dict) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode()
        while True:
            try:
                self._p.produce(topic, key=key.encode(), value=payload, on_delivery=self._on_delivery)
                break
            except BufferError:  # local queue full: let it drain
                self._p.poll(0.5)
        self._p.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        return self._p.flush(timeout)


# --------------------------------------------------------------------------- archive
class NdjsonArchive:
    """Append events to <dir>/<topic>/date=YYYY-MM-DD/part-<pid>.ndjson.gz (UTC date)."""

    def __init__(self, root: str | os.PathLike):
        self.root = Path(root)

    def write(self, topic: str, events: list[dict]) -> None:
        if not events:
            return
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        folder = self.root / topic / f"date={day}"
        folder.mkdir(parents=True, exist_ok=True)
        # each call appends a new gzip member: valid for `zcat` and gzip readers
        with gzip.open(folder / f"part-{os.getpid()}.ndjson.gz", "at", encoding="utf-8") as fh:
            for ev in events:
                fh.write(json.dumps(ev, separators=(",", ":")) + "\n")


# --------------------------------------------------------------------------- loop
class Stopper:
    """SIGTERM/SIGINT-aware sleep so systemd stops the service cleanly."""

    def __init__(self) -> None:
        self.stop = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, *_):
        log.info("stop requested")
        self.stop = True

    def sleep(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while not self.stop and time.monotonic() < end:
            time.sleep(min(1.0, end - time.monotonic()))


def run_every(period_s: float, tick: Callable[[], None], stopper: Stopper) -> None:
    """Call tick() every period_s seconds, aligned on wall clock, until stopped."""
    while not stopper.stop:
        started = time.monotonic()
        try:
            tick()
        except Exception:  # never let one bad poll kill the service
            log.exception("poll failed")
        stopper.sleep(max(0.0, period_s - (time.monotonic() - started)))
