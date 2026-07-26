# Databricks notebook source
# Problem: Profile a tiny NYC HVFHV CSV without changing its records.
# Set DATA_DIR to the folder where you uploaded the committed fixtures.
CATALOG = "<learner_catalog>"
SCHEMA = "lakehouse_learning"
DATA_DIR = "<path-to-learning_playground/data>"

# COMMAND ----------
from pyspark.sql import functions as F
trips = spark.read.option("header", True).csv(f"{DATA_DIR}/nyc_trips.csv")
trips.createOrReplaceTempView("lp_raw_trips")
display(trips.orderBy("source_trip_id"))

# COMMAND ----------
# TODO: implement one aggregation returning row_count, operator_null_count,
# pickup_min, and pickup_max. Cast pickup_datetime to timestamp first.
# Expected: row_count=20, operator_null_count=1,
# pickup_min=2024-01-01 08:00:00, pickup_max=2024-01-01 17:00:00.
# This reference keeps the imported exercise runnable and displays expected output.
# Remove this line while solving the TODO independently.
# MAGIC %run ./solutions/01_read_and_profile
