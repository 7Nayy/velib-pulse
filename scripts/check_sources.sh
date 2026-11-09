#!/usr/bin/env bash
# Guide 1, step 1: test the endpoints by hand before writing any code.
set -euo pipefail
BASE="https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"

echo "== Vélib' GBFS discovery =="
curl -fsS "$BASE/gbfs.json" | python3 -m json.tool | head -40

echo; echo "== station_status: header + first station =="
curl -fsS "$BASE/station_status.json" | python3 -c '
import json,sys; d=json.load(sys.stdin); st=d["data"]["stations"]
print({k:v for k,v in d.items() if k!="data"}); print("stations:",len(st)); print(json.dumps(st[0],indent=2))'

echo; echo "== station_information: first station =="
curl -fsS "$BASE/station_information.json" | python3 -c '
import json,sys; d=json.load(sys.stdin); print(json.dumps(d["data"]["stations"][0],indent=2))'

echo; echo "== Open-Meteo current weather (Paris, UTC) =="
curl -fsS "https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&current=temperature_2m,precipitation,wind_speed_10m,weather_code&timezone=GMT" \
  | python3 -m json.tool
