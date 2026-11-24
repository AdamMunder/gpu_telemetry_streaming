"""Quick look inside the bronze tables (run while or after bronze runs).

    docker compose exec spark /opt/spark/bin/spark-submit pipeline/check_bronze.py
"""
from spark_session import LAKEHOUSE, get_spark

spark = get_spark("check_bronze")
spark.sparkContext.setLogLevel("WARN")

inv = spark.read.format("delta").load(f"{LAKEHOUSE}/bronze/gpu_inventory")
tel = spark.read.format("delta").load(f"{LAKEHOUSE}/bronze/gpu_telemetry")

print("=== bronze_gpu_inventory ===")
inv.show(truncate=False)

print(f"=== bronze_gpu_telemetry: {tel.count()} rows ===")
print("Rows per partition and key (each key should sit in exactly one partition):")
tel.groupBy("kafka_partition", "kafka_key").count().orderBy("kafka_partition", "kafka_key").show()

print("Latest 5 rows:")
tel.orderBy(tel.ingested_at.desc(), tel.kafka_offset.desc()) \
   .select("kafka_partition", "kafka_offset", "raw_json").show(5, truncate=False)