"""Open-Meteo -> Kafka producer (weather context for Vélib' demand).

Polls the Open-Meteo forecast API every 15 minutes (its `current` block is
refreshed on a 15-minute grid) for a few points of the Vélib' area and
publishes to `weather.paris`, keyed by point id.

No API key. Free tier is for non-commercial use (limit 10 000 calls/day);
we use 4 points x 96 polls = 384 calls/day. Attribution: "Weather data by
Open-Meteo.com" (CC BY 4.0) on the dashboard.

Env: KAFKA_BOOTSTRAP, POLL_SECONDS, ARCHIVE_DIR, LOG_LEVEL
"""
from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime, timezone
from typing import Optional

import requests

from common import (SCHEMA_VERSION, KafkaSink, NdjsonArchive, Stopper, get_json, log,
                    run_every, setup_logging, utc_now_iso)

URL = "https://api.open-meteo.com/v1/forecast"
TOPIC = "weather.paris"
VARIABLES = ["temperature_2m", "apparent_temperature", "precipitation", "rain",
             "wind_speed_10m", "cloud_cover", "weather_code", "is_day"]

# A few points so the dashboard/model can join each station to its nearest point.
POINTS = {
    "paris_center": (48.8566, 2.3522),
    "west": (48.8800, 2.2300),      # Neuilly / La Défense side
    "east": (48.8600, 2.4400),      # Montreuil / Vincennes side
    "south": (48.8150, 2.3600),     # Montrouge / Ivry side
}


def to_event(point_id: str, lat: float, lon: float, doc: dict, ingest_time: str) -> dict:
    cur = doc["current"]
    # timezone=GMT in the request => "time" is UTC without suffix, e.g. 2026-11-09T10:15
    t = datetime.fromisoformat(cur["time"]).replace(tzinfo=timezone.utc)
    t_iso = t.isoformat().replace("+00:00", "Z")
    ev = {
        "schema_version": SCHEMA_VERSION,
        "event_id": hashlib.sha1(f"{point_id}|{t_iso}".encode()).hexdigest(),
        "point_id": point_id,
        "lat": lat,
        "lon": lon,
        "event_time": t_iso,
        "ingest_time": ingest_time,
        "source": "open_meteo",
    }
    for v in VARIABLES:
        ev[v] = cur.get(v)
    return ev


class WeatherProducer:
    def __init__(self, sink: Optional[KafkaSink], archive: Optional[NdjsonArchive]):
        self.sink, self.archive = sink, archive
        self.session = requests.Session()
        self.last_seen: dict[str, str] = {}

    def tick(self) -> None:
        events = []
        for pid, (lat, lon) in POINTS.items():
            doc = get_json(self.session, URL, params={
                "latitude": lat, "longitude": lon, "current": ",".join(VARIABLES),
                "timezone": "GMT", "wind_speed_unit": "kmh",
            })
            ev = to_event(pid, lat, lon, doc, utc_now_iso())
            if self.last_seen.get(pid) == ev["event_time"]:
                continue  # same 15-min slot already sent
            self.last_seen[pid] = ev["event_time"]
            events.append(ev)
        if self.archive:
            self.archive.write(TOPIC, events)
        if self.sink:
            for ev in events:
                self.sink.send(TOPIC, ev["point_id"], ev)
            self.sink.flush()
        log.info("weather: %d new points published", len(events))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--archive-only", action="store_true")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    setup_logging()
    archive_dir = os.environ.get("ARCHIVE_DIR")
    archive = NdjsonArchive(archive_dir) if archive_dir else None
    sink = None if args.archive_only else KafkaSink(os.environ.get("KAFKA_BOOTSTRAP", "kafka1:9092"),
                                                    "weather-producer")
    if sink is None and archive is None:
        ap.error("--archive-only requires ARCHIVE_DIR")
    prod = WeatherProducer(sink, archive)
    if args.once:
        prod.tick()
        return
    run_every(float(os.environ.get("POLL_SECONDS", "900")), prod.tick, Stopper())
    if sink:
        sink.flush()


if __name__ == "__main__":
    main()
