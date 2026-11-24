"""SILVER layer: turn raw bronze data into clean, trustworthy tables, using SQL.

Tables it writes:
  dim_gpu         <- the inventory with proper types (one row per GPU)
  fact_telemetry  <- valid, de-duplicated measurements (one row per GPU per timestamp)
  quarantine      <- rejected events, with the reason and the raw JSON

Run (bronze should be running too):
    docker compose exec spark /opt/spark/bin/spark-submit pipeline/02_silver.py
"""
from spark_session import CHECKPOINTS, LAKEHOUSE, get_spark

BRONZE_TELEMETRY = f"{LAKEHOUSE}/bronze/gpu_telemetry"
BRONZE_INVENTORY = f"{LAKEHOUSE}/bronze/gpu_inventory"
DIM_GPU = f"{LAKEHOUSE}/silver/dim_gpu"
FACT = f"{LAKEHOUSE}/silver/fact_telemetry"
QUARANTINE = f"{LAKEHOUSE}/silver/quarantine"

spark = get_spark("silver")


# ---------- 1) dim_gpu: the inventory with proper types (small, rebuilt on every run) ----------
dim = spark.sql(f"""
    SELECT gpu_id,
           node_id,
           model_name,
           CAST(rated_power_w AS INT)  AS rated_power_w,
           CAST(price_usd AS DOUBLE)   AS price_usd,
           firmware
    FROM delta.`{BRONZE_INVENTORY}`
""")
dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(DIM_GPU)
print(f"dim_gpu: {dim.count()} rows")

# Create the fact table once (empty), so MERGE has a target on the first run.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS delta.`{FACT}` (
        gpu_id STRING, event_ts TIMESTAMP, temp_c DOUBLE, power_w INT, util_pct INT,
        status STRING, kafka_partition INT, kafka_offset BIGINT, ingested_at TIMESTAMP
    ) USING DELTA
""")


# ---------- 2) for each small batch of NEW bronze rows, run plain SQL ----------
def process_batch(batch_df, batch_id):
    s = batch_df.sparkSession
    batch_df.createOrReplaceTempView("new_bronze")  # lets SQL see this batch as a table

    # Parse the JSON, then label each row: reject_reason is NULL for good rows.
    s.sql(f"""
        WITH parsed AS (
            SELECT from_json(raw_json,
                     'gpu_id STRING, node_id STRING, ts STRING, temp_c DOUBLE,
                      power_w INT, util_pct INT, status STRING') AS e,
                   raw_json, kafka_partition, kafka_offset, ingested_at
            FROM new_bronze
        )
        SELECT e.gpu_id,
               to_timestamp(e.ts)  AS event_ts,
               e.temp_c, e.power_w, e.util_pct, e.status,
               p.raw_json, p.kafka_partition, p.kafka_offset, p.ingested_at,
               CASE
                   WHEN e.gpu_id IS NULL OR e.ts IS NULL      THEN 'unparseable event'
                   WHEN e.temp_c IS NULL                      THEN 'missing temp_c'
                   WHEN e.temp_c NOT BETWEEN 0 AND 150        THEN 'temp_c out of range'
                   WHEN d.gpu_id IS NULL                      THEN 'unknown gpu'
               END AS reject_reason
        FROM parsed p
        LEFT JOIN delta.`{DIM_GPU}` d          -- no match in the inventory = unknown gpu
               ON p.e.gpu_id = d.gpu_id
    """).createOrReplaceTempView("classified")

    # Bad rows -> quarantine (kept, not thrown away)
    s.sql("""
        SELECT gpu_id, raw_json, reject_reason, kafka_partition, kafka_offset, ingested_at
        FROM classified
        WHERE reject_reason IS NOT NULL
    """).write.format("delta").mode("append").save(QUARANTINE)

    # Good rows: drop duplicates inside this batch (keep the first by offset) ...
    s.sql("""
        SELECT gpu_id, event_ts, temp_c, power_w, util_pct, status,
               kafka_partition, kafka_offset, ingested_at
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY gpu_id, event_ts
                                         ORDER BY kafka_offset) AS rn
            FROM classified
            WHERE reject_reason IS NULL
        )
        WHERE rn = 1
    """).createOrReplaceTempView("good_rows")

    # ... then MERGE: insert only rows not already in the fact table (idempotent).
    s.sql(f"""
        MERGE INTO delta.`{FACT}` AS f
        USING good_rows AS g
           ON f.gpu_id = g.gpu_id AND f.event_ts = g.event_ts
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"silver batch {batch_id}: processed {batch_df.count()} new bronze rows")


query = (
    spark.readStream.format("delta").load(BRONZE_TELEMETRY)   # only NEW rows of bronze
    .writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", f"{CHECKPOINTS}/silver_telemetry")  # silver's own bookmark
    .trigger(processingTime="5 seconds")
    .start()
)
print("silver: streaming from bronze... (Ctrl+C to stop)")
query.awaitTermination()