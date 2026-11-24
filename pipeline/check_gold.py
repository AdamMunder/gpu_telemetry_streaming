"""Quick look inside the gold table.

    docker compose exec spark /opt/spark/bin/spark-submit pipeline/check_gold.py
"""
from spark_session import LAKEHOUSE, get_spark

spark = get_spark("check_gold")
G = f"{LAKEHOUSE}/gold/model_minute"

print("=== gold_model_minute (latest minutes first) ===")
spark.sql(f"SELECT * FROM delta.`{G}` ORDER BY minute DESC, model_name").show(20, truncate=False)

print("=== totals per model ===")
spark.sql(f"""
    SELECT model_name,
           SUM(readings)               AS readings,
           ROUND(SUM(energy_kwh), 4)   AS energy_kwh,
           MAX(max_temp_c)             AS max_temp_c,
           SUM(danger_events)          AS danger_events
    FROM delta.`{G}`
    GROUP BY model_name ORDER BY model_name
""").show(truncate=False)