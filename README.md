# Vélib' Pulse — real-time bike availability pipeline

ESILV Big Data project — solo, 2 Azure VMs.
**Question answered:** *will this Vélib' station still have a bike in 30 minutes?*

```
Vélib' GBFS (1 min) ─┐                                 ┌─> TimescaleDB ─> Grafana
Open-Meteo (15 min) ─┴─> Python producers ─> Kafka ─> Spark Structured Streaming
                                              │            └─> HDFS raw ─> cleaned ─> curated
                                              │                                  └─> Spark ML (GBT) ─> model in HDFS
                                              └─ local NDJSON.gz archive (safety net / replay)
```

| Item | Choice |
|---|---|
| Sources | Vélib' Métropole GBFS `station_status` + `station_information`; Open-Meteo `current` (4 points) |
| Topics | `velib.status` (6 partitions, key `station_id`), `velib.station_info` (compacted), `weather.paris` |
| Event time | feed snapshot time, **UTC** ISO-8601 (`...Z`); `ingest_time` also carried |
| Dedup key | `event_id = sha1(station_id | snapshot_epoch)` |
| ML | binary classification `empty_in_30min`, Spark ML GBT, baseline = persistence, metric = PR-AUC |

Report 1 (Data, Service & ML brief) is delivered separately.

## Measured source throughput

`scripts/measure_throughput.py --minutes 10`, run on 2026-11-09 (Sunday, early afternoon, Paris):

| Metric | Value |
|---|---|
| Refresh rate | 10 snapshots in 10 min — one every **60 s** (the feed's `ttl=3600` is not reliable) |
| Stations per snapshot | 1 520 |
| Events | 15 200 → **25.3 events/s** on average, ≈ 2.2 M messages/day |
| Message size | 331 bytes (JSON, uncompressed) → ≈ 8.4 KB/s, ≈ 725 MB/day before compression |
| Stations changed | 155 per snapshot on average (≈ 10 %) |

The feed is served by several replicas that answer in turn: the same refresh can come back stamped
1 s later, or a lagging replica can still serve the previous one. A naive "new timestamp ⇒ new
snapshot" rule counted 16 snapshots (40 events/s, +60 %) in the first run. The producer and the
measurement script therefore only accept a snapshot at least `MIN_SNAPSHOT_GAP_S` (30 s) after the
last published one.

## Repository layout

```
producers/          velib_producer.py, weather_producer.py, replay_archive.py, common.py
schemas/            JSON Schemas of every Kafka message value (schema_version 1)
scripts/            check_sources.sh, measure_throughput.py, create_topics.sh
deploy/             install_producers.sh + systemd units
spark/              ingest_raw.py (Kafka -> HDFS raw), more jobs at checkpoint 2
infra/              hosts.example, cluster notes
tests/              unit tests on a recorded payload (pytest)
```

## Cluster layout (2 VMs, solo)

| VM | Services |
|---|---|
| vm1 | HDFS NameNode, YARN ResourceManager, Kafka node (broker + controller, KRaft), Spark History Server |
| vm2 | HDFS DataNode, YARN NodeManager, TimescaleDB, Grafana, producers |

Single Kafka node ⇒ replication factor 1 (allowed for solo groups). Partitions = 6 so the streaming
job can read in parallel; RF is a one-line change in `create_topics.sh` if the cluster grows.

## Runbook

### 0. Prerequisites (both VMs)
```bash
sudo apt-get update && sudo apt-get install -y python3-venv openjdk-17-jdk chrony git
cat infra/hosts.example | sudo tee -a /etc/hosts   # edit IPs first
timedatectl                                        # "System clock synchronized: yes"
```
Azure NSG: open the cluster ports **between the two VMs only** (VirtualNetwork source), never to the internet.

### 1. Check the sources by hand
```bash
./scripts/check_sources.sh
python3 scripts/measure_throughput.py --minutes 10   # real events/s, message size
```

### 2. Start collecting now (before Kafka exists)
The ML part needs weeks of history, so the producers run in **archive-only** mode first:
they write `/var/lib/velib-pulse/archive/<topic>/date=YYYY-MM-DD/*.ndjson.gz` on vm2.
```bash
git clone <this repo> && cd velib-pulse
sudo ARCHIVE_ONLY=1 ./deploy/install_producers.sh
journalctl -u velib-producer -f
du -sh /var/lib/velib-pulse/archive/*
```

### 3. Switch to Kafka (once the KRaft node is up on vm1)
```bash
./scripts/create_topics.sh
sudo sed -i 's/ --archive-only//' /etc/systemd/system/{velib,weather}-producer.service
sudo systemctl daemon-reload && sudo systemctl restart velib-producer weather-producer
# push the history collected in step 2 (same event_id => deduplicated downstream)
sudo -u velib /opt/velib-pulse/.venv/bin/python /opt/velib-pulse/producers/replay_archive.py \
     --dir /var/lib/velib-pulse/archive --topic velib.status --key station_id
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka1:9092 \
     --topic velib.status --property print.key=true --max-messages 5
```
The local archive stays on as a safety net (≈ 100 MB/day gzipped, ≈ 7 GB over 10 weeks — check `df -h`).

### 4. Tests
```bash
pip install -r requirements-dev.txt && pytest -q
```

## Versions
Python 3.11+, confluent-kafka 2.6.1, requests 2.32.3. Kafka 4.x (KRaft only), Spark 4.x (Scala 2.13),
Hadoop 3.4.x — exact versions pinned here at checkpoint 2.

## Data licences & attribution
- Vélib' Métropole open data (GBFS) — attribution "Vélib' Métropole" on dashboards; respect the licence stated on the operator's open-data page.
- Weather data by [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0), free non-commercial tier.
- No API key is needed; any future secret goes in `/etc/velib-pulse/*.env`, never in Git.

## Contribution
Solo project — Nadim Benkouidem: everything.
