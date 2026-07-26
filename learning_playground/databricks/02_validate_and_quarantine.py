# Databricks notebook source
# Problem: Assign exactly one quarantine reason using the documented priority.
CATALOG = "<learner_catalog>"
SCHEMA = "lakehouse_learning"
DATA_DIR = "<path-to-learning_playground/data>"

# COMMAND ----------
from pyspark.sql import functions as F
trips = spark.read.option("header", True).csv(f"{DATA_DIR}/nyc_trips.csv")
zones = spark.read.option("header", True).csv(f"{DATA_DIR}/taxi_zones.csv")
display(trips.orderBy("source_trip_id"))

# COMMAND ----------
# TODO: cast fields and use a CASE/when expression with this priority:
# MISSING_OPERATOR, INVALID_TIMESTAMP, NEGATIVE_FARE, UNKNOWN_PICKUP_ZONE.
# Register valid_trips and quarantined_trips temporary views.
# Expected counts: valid_trips=16, quarantined_trips=4.
# MAGIC %run ./solutions/02_validate_and_quarantine
