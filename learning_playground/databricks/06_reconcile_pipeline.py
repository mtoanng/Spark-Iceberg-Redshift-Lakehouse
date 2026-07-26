# Databricks notebook source
# Problem: Prove the tiny pipeline has not lost or duplicated accepted trips.
CATALOG = "<learner_catalog>"
SCHEMA = "lakehouse_learning"
DATA_DIR = "<path-to-learning_playground/data>"

# COMMAND ----------
from pyspark.sql import functions as F
trips = spark.read.option("header", True).csv(f"{DATA_DIR}/nyc_trips.csv")
display(trips.orderBy("source_trip_id"))

# COMMAND ----------
# TODO: compute input_count, valid_count, quarantine_count, canonical_count,
# and mart_trip_total. Assert input=valid+quarantine and canonical=mart total.
# Expected: 20=16+4, canonical_count=15, mart_trip_total=15.
# MAGIC %run ./solutions/06_reconcile_pipeline
