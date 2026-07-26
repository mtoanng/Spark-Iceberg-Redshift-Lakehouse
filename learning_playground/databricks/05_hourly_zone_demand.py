# Databricks notebook source
# Problem: Build one small mart grouped by pickup hour and pickup zone.
CATALOG = "<learner_catalog>"
SCHEMA = "lakehouse_learning"
DATA_DIR = "<path-to-learning_playground/data>"

# COMMAND ----------
from pyspark.sql import functions as F
trips = spark.read.option("header", True).csv(f"{DATA_DIR}/nyc_trips.csv")
display(trips.orderBy("source_trip_id"))

# COMMAND ----------
# TODO: make canonical valid trips, derive pickup_hour, then calculate
# trip_count and total_passenger_fare by pickup_hour and pickup_zone_id.
# Expected key row: (8, 1, 2, 22.00); expected mart rows=14.
# MAGIC %run ./solutions/05_hourly_zone_demand
