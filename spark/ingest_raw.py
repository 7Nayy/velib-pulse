"""Kafka velib.status -> HDFS raw zone (Parquet, partitioned by date).

Checkpoint 2 job, prepared now so the schema stays aligned with the producer.
  spark-submit --master yarn \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:$SPARK_VERSION spark/ingest_raw.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, to_timestamp
from pyspark.sql.types import IntegerType, StringType, StructType

BOOTSTRAP = "kafka1:9092"

schema = (StructType()
          .add("schema_version", IntegerType())
          .add("event_id", StringType())
          .add("station_id", StringType())
          .add("station_code", StringType())
          .add("event_time", StringType())
          .add("last_reported", StringType())
          .add("ingest_time", StringType())
          .add("bikes", IntegerType())
          .add("mechanical", IntegerType())
          .add("ebike", IntegerType())
          .add("docks", IntegerType())
          .add("is_installed", IntegerType())
          .add("is_renting", IntegerType())
          .add("is_returning", IntegerType())
          .add("source", StringType()))

spark = (SparkSession.builder.appName("ingest_raw_velib")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", "velib.status")
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

events = (raw.select(col("key").cast("string").alias("kafka_key"),
                     col("partition"), col("offset"),
                     col("timestamp").alias("kafka_ts"),
                     from_json(col("value").cast("string"), schema).alias("e"))
          .select("kafka_key", "partition", "offset", "kafka_ts", "e.*")
          .withColumn("event_time", to_timestamp("event_time"))
          .withColumn("last_reported", to_timestamp("last_reported"))
          .withColumn("ingest_time", to_timestamp("ingest_time"))
          .withColumn("date", to_date("event_time")))

# raw zone = exactly as received (no dedup here; the cleaned zone dedups on event_id)
(events.writeStream.format("parquet")
 .option("path", "hdfs:///datalake/raw/velib_status")
 .option("checkpointLocation", "hdfs:///checkpoints/raw_velib_status")
 .partitionBy("date")
 .trigger(processingTime="5 minutes")
 .start()
 .awaitTermination())
