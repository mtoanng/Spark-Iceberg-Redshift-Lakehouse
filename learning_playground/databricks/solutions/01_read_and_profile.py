# Databricks notebook source
CATALOG = "<learner_catalog>"
SCHEMA = "lakehouse_learning"
DATA_DIR = "<path-to-learning_playground/data>"

# COMMAND ----------
from pyspark.sql import functions as F
trips = spark.read.option("header", True).csv(f"{DATA_DIR}/nyc_trips.csv")
trips = trips.withColumn("pickup_ts", F.to_timestamp("pickup_datetime"))
trips.createOrReplaceTempView("lp_raw_trips")
profile = trips.agg(
    F.count("*").alias("row_count"), F.sum(F.col("hvfhs_license_num").isNull().cast("int")).alias("operator_null_count"),
    F.min("pickup_ts").alias("pickup_min"), F.max("pickup_ts").alias("pickup_max"),
)
display(trips.orderBy("source_trip_id"))
display(profile)  # 20, 1, 2024-01-01 08:00:00, 2024-01-01 17:00:00
