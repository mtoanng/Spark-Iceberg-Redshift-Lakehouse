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
    .withColumn("reason", F.when(F.col("hvfhs_license_num").isNull() | (F.trim("hvfhs_license_num") == ""), "x").when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull() | (F.col("dropoff_ts") <= F.col("pickup_ts")), "x").when(F.col("base_passenger_fare") < 0, "x").when(F.col("known_pickup").isNull(), "x")))
fields = ["hvfhs_license_num", "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID", "base_passenger_fare", "trip_miles"]
canonical = (checked.filter("reason is null").withColumn("row_id", F.sha2(F.concat_ws("||", *[F.col(c) for c in fields]), 256))
    .withColumn("rank", F.row_number().over(Window.partitionBy("row_id").orderBy("source_trip_id"))).filter("rank=1"))
pickup = zones.select(F.col("LocationID").alias("PULocationID"), F.col("zone").alias("pickup_zone"))
dropoff = zones.select(F.col("LocationID").alias("DOLocationID"), F.col("zone").alias("dropoff_zone"))
enriched = canonical.join(pickup, "PULocationID", "left").join(dropoff, "DOLocationID", "left")
enriched.createOrReplaceTempView("enriched_trips")
display(enriched.select("source_trip_id", "pickup_zone", "dropoff_zone").orderBy("source_trip_id"))
assert enriched.count() == 15 and enriched.filter("source_trip_id = '14' and dropoff_zone is null").count() == 1
