"""Quick look inside the silver tables.

    docker compose exec spark /opt/spark/bin/spark-submit pipeline/check_silver.py
"""
from spark_session import LAKEHOUSE, get_spark

spark = get_spark("check_silver")
S = f"{LAKEHOUSE}/silver"

print("=== dim_gpu ===")
spark.sql(f"SELECT * FROM delta.`{S}/dim_gpu`").show(truncate=False)

print("=== fact_telemetry: rows per GPU (joined to dim_gpu) ===")
spark.sql(f"""
    SELECT d.model_name, f.gpu_id, COUNT(*) AS readings,
           ROUND(AVG(f.temp_c), 1) AS avg_temp_c, MAX(f.temp_c) AS max_temp_c
    FROM delta.`{S}/fact_telemetry` f
    JOIN delta.`{S}/dim_gpu` d ON f.gpu_id = d.gpu_id
    GROUP BY d.model_name, f.gpu_id
    ORDER BY f.gpu_id
""").show()

print("=== duplicates left in fact (should be 0) ===")
spark.sql(f"""
    SELECT COUNT(*) AS duplicate_groups FROM (
        SELECT gpu_id, event_ts FROM delta.`{S}/fact_telemetry`
        GROUP BY gpu_id, event_ts HAVING COUNT(*) > 1)
""").show()

print("=== quarantine: rejected events by reason ===")
spark.sql(f"""
    SELECT reject_reason, COUNT(*) AS events
    FROM delta.`{S}/quarantine`
    GROUP BY reject_reason ORDER BY events DESC
""").show(truncate=False)