"""Measure the real source throughput (Guide 1: count the events over 10 minutes).

Polls station_status.json for --minutes and reports snapshots, events,
events/s, changed stations per snapshot and average message size.
Needs only `requests`; run it from a VM:  python3 scripts/measure_throughput.py --minutes 10
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "producers"))

import requests  # noqa: E402

from velib_producer import DEFAULT_BASE, status_events  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=10)
    ap.add_argument("--poll", type=float, default=15, help="poll period (s), < feed ttl to catch every refresh")
    args = ap.parse_args()

    s = requests.Session()
    end = time.time() + args.minutes * 60
    seen, total, sizes, changed = set(), 0, [], []
    prev: dict[str, tuple] = {}
    while time.time() < end:
        doc = s.get(f"{DEFAULT_BASE}/station_status.json", timeout=15).json()
        snap, evs = status_events(doc, "")
        if snap not in seen:
            seen.add(snap)
            total += len(evs)
            sizes += [len(json.dumps(e, separators=(",", ":"))) for e in evs[:200]]
            cur = {e["station_id"]: (e["bikes"], e["docks"], e["mechanical"], e["ebike"]) for e in evs}
            if prev:
                changed.append(sum(1 for k, v in cur.items() if prev.get(k) != v))
            prev = cur
            print(f"snapshot {snap}: {len(evs)} stations, ttl={doc.get('ttl')}", flush=True)
        time.sleep(args.poll)

    dur = args.minutes * 60
    print("\n--- result ---")
    print(f"distinct snapshots : {len(seen)} in {args.minutes:g} min "
          f"(one every {dur / max(len(seen), 1):.0f} s)")
    print(f"events             : {total}  ->  {total / dur:.1f} events/s on average")
    if sizes:
        print(f"avg message size   : {sum(sizes) / len(sizes):.0f} bytes (JSON, uncompressed)")
    if changed:
        print(f"stations changed   : {sum(changed) / len(changed):.0f} per snapshot on average")


if __name__ == "__main__":
    main()
