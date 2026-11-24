"""BRONZE layer: store the raw data exactly as it arrived. No cleaning here.

Two tables:
  1) bronze_gpu_telemetry  <- streamed from the Kafka topic 'gpu-telemetry'
  2) bronze_gpu_inventory  <- loaded once from data/gpu_inventory.csv

Run (from the project folder on Windows):
    docker compose exec spark /opt/spark/bin/spark-submit pipeline/01_bronze.py
Stop with Ctrl+C. Thanks to the checkpoint, the next run continues where this one stopped.
"""
from pyspark.sql import functions as F

from spark_session import CHECKPOINTS, LAKEHOUSE, get_spark

KAFKA_SERVERS = "kafka:29092"   # Kafka as seen from INSIDE Docker (the INTERNAL listener)
TOPIC = "gpu-telemetry"

TELEMETRY_PATH = f"{LAKEHOUSE}/bronze/gpu_telemetry"
INVENTORY_PATH = f"{LAKEHOUSE}/bronze/gpu_inventory"

spark = get_spark("bronze")
spark.sparkContext.setLogLevel("WARN")  # hide Spark's very chatty INFO logs


# ---------- 1) inventory: a small batch load, replaced on every run ----------
inventory = (
    spark.read.option("header", True).csv("/app/data/gpu_inventory.csv")  # all columns read as text
    .withColumn("ingested_at", F.current_timestamp())
)
inventory.write.format("delta").mode("overwrite").save(INVENTORY_PATH)
print(f"bronze_gpu_inventory: {inventory.count()} rows written")


# ---------- 2) telemetry: a stream from Kafka ----------
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")  # first run only: start from the oldest message
    .load()
)

# Kafka gives key/value as bytes; we turn them into text and keep Kafka's own info as columns.
bronze = raw.select(
    F.col("key").cast("string").alias("kafka_key"),
    F.col("value").cast("string").alias("raw_json"),     # the event, untouched
    F.col("topic").alias("kafka_topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.current_timestamp().alias("ingested_at"),
)

query = (
    bronze.writeStream.format("delta")
    .outputMode("append")                                  # only add new rows
    .option("checkpointLocation", f"{CHECKPOINTS}/bronze_telemetry")  # the bookmark
    .trigger(processingTime="5 seconds")                   # check Kafka for new messages every 5 s
    .start(TELEMETRY_PATH)
)

print("bronze_gpu_telemetry: streaming from Kafka... (Ctrl+C to stop)")
query.awaitTermination()