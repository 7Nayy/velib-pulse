#!/usr/bin/env bash
# Create the Kafka topics. Solo layout (2 VMs): a single KRaft node => RF=1.
# With 3 brokers, run:  RF=3 ./scripts/create_topics.sh
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka1:9092}"
RF="${RF:-1}"
KAFKA_BIN="${KAFKA_HOME:-/opt/kafka}/bin"
SEVEN_DAYS_MS=$((7 * 24 * 3600 * 1000))

create() {  # name partitions extra-configs...
  local name=$1 parts=$2; shift 2
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$parts" --replication-factor "$RF" "$@"
}

# station snapshots, key = station_id (order kept per station)
create velib.status 6 --config retention.ms=$SEVEN_DAYS_MS --config compression.type=producer
# latest station metadata, key = station_id => log compaction keeps the last value per station
create velib.station_info 1 --config cleanup.policy=compact
# weather, key = point_id
create weather.paris 1 --config retention.ms=$SEVEN_DAYS_MS

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe \
  --topic velib.status --topic velib.station_info --topic weather.paris 2>/dev/null \
  || "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe
