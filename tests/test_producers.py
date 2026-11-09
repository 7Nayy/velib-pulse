import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "producers"))

import jsonschema  # noqa: E402
from common import NdjsonArchive  # noqa: E402
from velib_producer import VelibProducer, bike_types, status_events  # noqa: E402
from weather_producer import to_event  # noqa: E402

FIX = json.loads((ROOT / "tests/fixtures/station_status.json").read_text())
SCHEMA = json.loads((ROOT / "schemas/velib.status.v1.json").read_text())


def test_bike_types_list_and_dict():
    assert bike_types([{"mechanical": 1}, {"ebike": 2}]) == (1, 2)
    assert bike_types({"mechanical": 4, "ebike": 0}) == (4, 0)
    assert bike_types(None) == (None, None)


def test_status_events_fields_and_time():
    snap, evs = status_events(FIX, "2026-11-09T10:00:00.000Z")
    assert snap == 1793096485
    assert len(evs) == 2
    e = evs[0]
    assert e["station_id"] == "213688169" and e["station_code"] == "16107"
    assert e["event_time"].endswith("Z") and e["event_time"].startswith("2026-10-27")
    assert (e["bikes"], e["mechanical"], e["ebike"], e["docks"]) == (3, 1, 2, 32)
    assert e["mechanical"] + e["ebike"] == e["bikes"]
    for ev in evs:
        jsonschema.validate(ev, SCHEMA)


def test_event_id_stable_and_unique():
    _, a = status_events(FIX, "x")
    _, b = status_events(FIX, "y")
    assert [e["event_id"] for e in a] == [e["event_id"] for e in b]  # same snapshot => same id (dedup)
    assert len({e["event_id"] for e in a}) == len(a)


class FakeSink:
    def __init__(self):
        self.msgs, self.delivered, self.failed = [], 0, 0

    def send(self, topic, key, value):
        self.msgs.append((topic, key, value))
        self.delivered += 1

    def flush(self, timeout=30):
        return 0


def test_tick_skips_same_snapshot_and_archives(tmp_path, monkeypatch):
    import velib_producer as vp

    info = {"last_updated": 1793096485, "data": {"stations": [
        {"station_id": 213688169, "stationCode": "16107", "name": "Benjamin Godard - Victor Hugo",
         "lat": 48.865983, "lon": 2.275725, "capacity": 35}]}}
    monkeypatch.setattr(vp, "get_json", lambda _s, url, **_: info if "information" in url else FIX)
    sink = FakeSink()
    p = VelibProducer("http://x", sink, NdjsonArchive(tmp_path))
    p.tick()
    p.tick()  # same snapshot => nothing new
    topics = [m[0] for m in sink.msgs]
    assert topics.count("velib.status") == 2 and topics.count("velib.station_info") == 1
    assert sink.msgs[-1][1] == sink.msgs[-1][2]["station_id"]  # key == station_id
    files = list((tmp_path / "velib.status").glob("date=*/*.ndjson.gz"))
    assert len(files) == 1
    with gzip.open(files[0], "rt") as fh:
        assert len(fh.readlines()) == 2


def test_weather_event_is_utc():
    doc = {"current": {"time": "2026-11-09T10:15", "interval": 900, "temperature_2m": 9.4,
                       "precipitation": 0.2, "wind_speed_10m": 14.0, "weather_code": 61}}
    ev = to_event("paris_center", 48.8566, 2.3522, doc, "now")
    assert ev["event_time"] == "2026-11-09T10:15:00Z"
    assert ev["temperature_2m"] == 9.4 and ev["rain"] is None
