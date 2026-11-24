"""GOLD layer: small, ready-to-use summary for the dashboard.

Table:
  gold_model_minute  <- one row per GPU model per minute:
                        readings, avg/max temperature, energy (kWh), DANGER count, model price

Incremental: for each batch of NEW fact rows, only the minutes those rows touch are
recomputed and MERGEd into gold (updated if they exist, inserted if new).
A late event therefore just corrects its own minute.

Run (silver should be running too):
    docker compose exec spark /opt/spark/bin/spark-submit pipeline/03_gold.py
"""
from spark_session import CHECKPOINTS, LAKEHOUSE, get_spark

FACT = f"{LAKEHOUSE}/silver/fact_telemetry"
DIM_GPU = f"{LAKEHOUSE}/silver/dim_gpu"
GOLD = f"{LAKEHOUSE}/gold/model_minute"

spark = get_spark("gold")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS delta.`{GOLD}` (
        model_name STRING, minute TIMESTAMP, readings BIGINT,
        avg_temp_c DOUBLE, max_temp_c DOUBLE, energy_kwh DOUBLE,
        danger_events BIGINT, model_price_usd DOUBLE
    ) USING DELTA
""")


def process_batch(batch_df, batch_id):
    s = batch_df.sparkSession
    batch_df.createOrReplaceTempView("new_fact")

    # 1) which minutes did this batch touch?
    s.sql("SELECT DISTINCT date_trunc('minute', event_ts) AS minute FROM new_fact") \
        .createOrReplaceTempView("touched_minutes")

    # 2) recompute ONLY those minutes, from the full fact table joined to the dimension
    s.sql(f"""
        SELECT d.model_name,
               date_trunc('minute', f.event_ts)                          AS minute,
               COUNT(*)                                                   AS readings,
               ROUND(AVG(f.temp_c), 1)                                    AS avg_temp_c,
               MAX(f.temp_c)                                              AS max_temp_c,
               ROUND(SUM(f.power_w) / 3600000.0, 5)                       AS energy_kwh,  -- W x 1 s each
               SUM(CASE WHEN f.status = 'DANGER' THEN 1 ELSE 0 END)       AS danger_events,
               MAX(d.price_usd)                                           AS model_price_usd
        FROM delta.`{FACT}` f
        JOIN delta.`{DIM_GPU}` d ON f.gpu_id = d.gpu_id
        WHERE date_trunc('minute', f.event_ts) IN (SELECT minute FROM touched_minutes)
        GROUP BY d.model_name, date_trunc('minute', f.event_ts)
    """).createOrReplaceTempView("recomputed")

    # 3) upsert into gold: replace minutes that exist, insert new ones
    s.sql(f"""
        MERGE INTO delta.`{GOLD}` AS g
        USING recomputed AS r
           ON g.model_name = r.model_name AND g.minute = r.minute
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"gold batch {batch_id}: refreshed "
          f"{s.table('touched_minutes').count()} minute(s) from {batch_df.count()} new fact rows")


query = (
    spark.readStream.format("delta").load(FACT)           # only NEW fact rows
    .writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", f"{CHECKPOINTS}/gold_model_minute")
    .trigger(processingTime="10 seconds")
    .start()
)
print("gold: streaming from silver... (Ctrl+C to stop)")
query.awaitTermination()