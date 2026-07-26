# Databricks notebook source
# Problem: Keep one copy of each valid trip with a deterministic hash key.
CATALOG = "<learner_catalog>"
SCHEMA = "lakehouse_learning"
DATA_DIR = "<path-to-learning_playground/data>"

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
trips = spark.read.option("header", True).csv(f"{DATA_DIR}/nyc_trips.csv")
display(trips.orderBy("source_trip_id"))

# COMMAND ----------
# TODO: validate rows, hash canonical source fields into row_id, then use
# row_number over Window.partitionBy("row_id") to retain the canonical row.
# Expected: 16 valid rows become 15 canonical rows; source_trip_id=1 appears once.
# MAGIC %run ./solutions/03_deduplicate_trips
