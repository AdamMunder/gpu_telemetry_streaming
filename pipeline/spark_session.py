"""One place that builds the Spark session, so every layer (bronze, silver, gold) uses the same setup.

The add-ons (Kafka connector, Delta Lake) and Delta settings live in conf/spark-defaults.conf,
because Spark must know them at startup, before this Python code runs.
"""
from pyspark.sql import SparkSession

LAKEHOUSE = "/app/lakehouse"                  # = the "lakehouse" folder in your project
CHECKPOINTS = f"{LAKEHOUSE}/_checkpoints"     # Spark's saved bookmarks live here


def get_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")    # hide Spark's very chatty INFO logs
    return spark