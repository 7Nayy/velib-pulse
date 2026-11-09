"""Vélib' Métropole GBFS -> Kafka producer.

Polls station_status.json every POLL_SECONDS (default 60 s, the feed's refresh
rate) and publishes one message per station to `velib.status`, keyed by
station_id. A snapshot less than MIN_SNAPSHOT_GAP_S after the last published
one is a replica serving the same refresh and is skipped. Every hour it also publishes station_information.json to the
compacted topic `velib.station_info` (capacity, coordinates, name).

Modes
  default          publish to Kafka (+ optional local archive)
  --archive-only   no Kafka: only write NDJSON.gz locally (start collecting
                   history before the cluster exists, replay later)

Env: KAFKA_BOOTSTRAP, VELIB_BASE_URL, POLL_SECONDS, ARCHIVE_DIR, LOG_LEVEL
"""
from __future__ import annotations

import argparse
import hashlib
import os
import time
from typing import Any, Optional

import requests

from common import (SCHEMA_VERSION, KafkaSink, NdjsonArchive, Stopper, epoch_to_iso,
                    get_json, log, run_every, setup_logging, utc_now_iso)

DEFAULT_BASE = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
TOPIC_STATUS = "velib.status"
TOPIC_INFO = "velib.station_info"
INFO_EVERY_S = 3600
# The feed is served by several replicas that answer in turn: one may stamp the same refresh
# 1 s later, or still serve the previous one. Real refreshes are ~60 s apart, so anything
# closer than this to the last published snapshot (or older) is the same refresh again.
MIN_SNAPSHOT_GAP_S = 30


def _int(v: Any) -> Optional[int]:
    try:
        return None if v is None else int(v)
    except (TypeError, ValueError):
        return None


def bike_types(raw: Any) -> tuple[Optional[int], Optional[int]]:
    """Vélib' encodes types as [{"mechanical": n}, {"ebike": m}] (GBFS 1.x/2.x style)."""
    merged: dict[str, Any] = {}
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                merged.update(item)
    elif isinstance(raw, dict):
        merged = raw
    return _int(merged.get("mechanical")), _int(merged.get("ebike"))


def feed_timestamp(doc: dict) -> Optional[int]:
    """Snapshot time of the feed (epoch s). Vélib' adds lastUpdatedOther next to GBFS last_updated."""
    for k in ("lastUpdatedOther", "last_updated"):
        ts = _int(doc.get(k))
        if ts:
            return ts
    return None


def status_events(doc: dict, ingest_time: str) -> tuple[Optional[int], list[dict]]:
    snap_ts = feed_timestamp(doc)
    if snap_ts is None:
        raise ValueError("feed without last_updated / lastUpdatedOther")
    snap_iso = epoch_to_iso(snap_ts)
    events = []
    for s in doc.get("data", {}).get("stations", []):
        sid = s.get("station_id")
        if sid is None:
            continue
        sid = str(sid)
        mech, ebike = bike_types(s.get("num_bikes_available_types"))
        last_rep = _int(s.get("last_reported"))
        events.append({
            "schema_version": SCHEMA_VERSION,
            "event_id": hashlib.sha1(f"{sid}|{snap_ts}".encode()).hexdigest(),
            "station_id": sid,
            "station_code": str(s.get("stationCode")) if s.get("stationCode") is not None else None,
            "event_time": snap_iso,
            "last_reported": epoch_to_iso(last_rep) if last_rep else None,
            "ingest_time": ingest_time,
            "bikes": _int(s.get("num_bikes_available", s.get("numBikesAvailable"))),
            "mechanical": mech,
            "ebike": ebike,
            "docks": _int(s.get("num_docks_available", s.get("numDocksAvailable"))),
            "is_installed": _int(s.get("is_installed")),
            "is_renting": _int(s.get("is_renting")),
            "is_returning": _int(s.get("is_returning")),
            "source": "velib_gbfs",
        })
    return snap_ts, events


def is_new_snapshot(last_snap: Optional[int], snap_ts: int) -> bool:
    return last_snap is None or snap_ts >= last_snap + MIN_SNAPSHOT_GAP_S


def info_events(doc: dict, ingest_time: str) -> list[dict]:
    snap_ts = feed_timestamp(doc) or int(time.time())
    out = []
    for s in doc.get("data", {}).get("stations", []):
        if s.get("station_id") is None:
            continue
        out.append({
            "schema_version": SCHEMA_VERSION,
            "station_id": str(s["station_id"]),
            "station_code": str(s.get("stationCode")) if s.get("stationCode") is not None else None,
            "name": s.get("name"),
            "lat": s.get("lat"),
            "lon": s.get("lon"),
            "capacity": _int(s.get("capacity")),
            "event_time": epoch_to_iso(snap_ts),
            "ingest_time": ingest_time,
            "source": "velib_gbfs",
        })
    return out


class VelibProducer:
    def __init__(self, base_url: str, sink: Optional[KafkaSink], archive: Optional[NdjsonArchive]):
        self.base = base_url.rstrip("/")
        self.sink, self.archive = sink, archive
        self.session = requests.Session()
        self.last_snap: Optional[int] = None
        self.last_info = 0.0
        self.sent_total = 0

    def _emit(self, topic: str, events: list[dict]) -> None:
        if self.archive:
            self.archive.write(topic, events)
        if self.sink:
            for ev in events:
                self.sink.send(topic, ev["station_id"], ev)
            self.sink.flush()

    def tick(self) -> None:
        now = time.time()
        if now - self.last_info >= INFO_EVERY_S:
            info = info_events(get_json(self.session, f"{self.base}/station_information.json"), utc_now_iso())
            self._emit(TOPIC_INFO, info)
            self.last_info = now
            log.info("station_info: %d stations published", len(info))

        doc = get_json(self.session, f"{self.base}/station_status.json")
        snap_ts, events = status_events(doc, utc_now_iso())
        if not is_new_snapshot(self.last_snap, snap_ts):
            log.info("feed not refreshed yet (snapshot %s, last published %s), skipping",
                     epoch_to_iso(snap_ts), epoch_to_iso(self.last_snap))
            return
        self._emit(TOPIC_STATUS, events)
        self.last_snap = snap_ts
        self.sent_total += len(events)
        extra = ""
        if self.sink:
            extra = f" delivered={self.sink.delivered} failed={self.sink.failed}"
        log.info("snapshot %s: %d events (total %d)%s", epoch_to_iso(snap_ts), len(events),
                 self.sent_total, extra)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--archive-only", action="store_true", help="do not use Kafka")
    ap.add_argument("--once", action="store_true", help="single poll then exit (smoke test)")
    args = ap.parse_args()
    setup_logging()

    archive_dir = os.environ.get("ARCHIVE_DIR")
    archive = NdjsonArchive(archive_dir) if archive_dir else None
    sink = None if args.archive_only else KafkaSink(os.environ.get("KAFKA_BOOTSTRAP", "kafka1:9092"),
                                                    "velib-producer")
    if sink is None and archive is None:
        ap.error("--archive-only requires ARCHIVE_DIR")

    prod = VelibProducer(os.environ.get("VELIB_BASE_URL", DEFAULT_BASE), sink, archive)
    if args.once:
        prod.tick()
        return
    run_every(float(os.environ.get("POLL_SECONDS", "60")), prod.tick, Stopper())
    if sink:
        sink.flush()


if __name__ == "__main__":
    main()
