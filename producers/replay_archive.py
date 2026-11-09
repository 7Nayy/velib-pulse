"""Replay the local NDJSON.gz archive into Kafka.

Used once the cluster is up, to push the history collected in --archive-only
mode. Messages keep their original event_id, so the streaming job's
dropDuplicates() removes anything already sent live.

  python replay_archive.py --dir /var/lib/velib-pulse/archive --topic velib.status \
      --key station_id [--since 2026-11-09] [--rate 5000]
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from pathlib import Path

from common import KafkaSink, log, setup_logging


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--key", required=True, help="field used as Kafka key (station_id / point_id)")
    ap.add_argument("--since", help="first date=YYYY-MM-DD folder to replay")
    ap.add_argument("--rate", type=int, default=5000, help="max messages per second (0 = unlimited)")
    args = ap.parse_args()
    setup_logging()

    sink = KafkaSink(os.environ.get("KAFKA_BOOTSTRAP", "kafka1:9092"), "archive-replay")
    days = sorted(p for p in (Path(args.dir) / args.topic).glob("date=*")
                  if not args.since or p.name >= f"date={args.since}")
    n, t0 = 0, time.monotonic()
    for day in days:
        for f in sorted(day.glob("*.ndjson.gz")):
            with gzip.open(f, "rt", encoding="utf-8") as fh:
                for line in fh:
                    ev = json.loads(line)
                    sink.send(args.topic, str(ev[args.key]), ev)
                    n += 1
                    if args.rate and n % args.rate == 0:
                        elapsed = time.monotonic() - t0
                        if elapsed < n / args.rate:
                            time.sleep(n / args.rate - elapsed)
        log.info("%s replayed, %d messages so far", day.name, n)
    sink.flush()
    log.info("done: %d messages, delivered=%d failed=%d", n, sink.delivered, sink.failed)


if __name__ == "__main__":
    main()
