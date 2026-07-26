# Databricks notebook source
CATALOG = "<learner_catalog>"
SCHEMA = "lakehouse_learning"
DATA_DIR = "<path-to-learning_playground/data>"

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
trips = spark.read.option("header", True).csv(f"{DATA_DIR}/nyc_trips.csv")
zones = spark.read.option("header", True).csv(f"{DATA_DIR}/taxi_zones.csv")
typed = trips.withColumn("pickup_ts", F.to_timestamp("pickup_datetime")).withColumn("dropoff_ts", F.to_timestamp("dropoff_datetime")).withColumn("base_passenger_fare", F.col("base_passenger_fare").cast("decimal(10,2)"))
checked = (typed.join(zones.select(F.col("LocationID").alias("known_pickup")), typed.PULocationID == F.col("known_pickup"), "left")
    .withColumn("reason_code", F.when(F.col("hvfhs_license_num").isNull() | (F.trim("hvfhs_license_num") == ""), "MISSING_OPERATOR").when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull() | (F.col("dropoff_ts") <= F.col("pickup_ts")), "INVALID_TIMESTAMP").when(F.col("base_passenger_fare") < 0, "NEGATIVE_FARE").when(F.col("known_pickup").isNull(), "UNKNOWN_PICKUP_ZONE")))
valid = checked.filter("reason_code is null")
fields = ["hvfhs_license_num", "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID", "base_passenger_fare", "trip_miles"]
canonical = (valid.withColumn("row_id", F.sha2(F.concat_ws("||", *[F.col(c) for c in fields]), 256))
    .withColumn("rank", F.row_number().over(Window.partitionBy("row_id").orderBy("source_trip_id"))).filter("rank=1"))
mart = canonical.withColumn("pickup_hour", F.hour("pickup_ts")).groupBy("pickup_hour", "PULocationID").agg(F.count("*").alias("trip_count"))
counts = spark.createDataFrame([(trips.count(), valid.count(), checked.filter("reason_code is not null").count(), canonical.count(), mart.agg(F.sum("trip_count")).first()[0])], ["input_count", "valid_count", "quarantine_count", "canonical_count", "mart_trip_total"])
counts.createOrReplaceTempView("pipeline_reconciliation")
display(counts)
row = counts.first()
assert row.input_count == row.valid_count + row.quarantine_count == 20
assert row.canonical_count == row.mart_trip_total == 15
