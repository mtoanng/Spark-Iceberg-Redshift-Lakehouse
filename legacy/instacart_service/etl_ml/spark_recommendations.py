"""
Spark ML recommendation job for AWS Glue.

Reads Gold feature tables from Glue Catalog, trains a simple Spark ML model,
generates top-N product recommendations, and writes them to MongoDB Atlas.
"""

import os
import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Window
from pyspark.sql import functions as F
from pymongo import MongoClient, UpdateOne


FEATURE_COLS = [
    "user_total_orders",
    "user_avg_days_between_orders",
    "user_avg_order_hour",
    "product_total_orders",
    "product_reorder_rate",
    "product_avg_cart_position",
    "user_product_order_count",
    "user_product_reorder_count",
    "user_product_avg_cart_position",
    "user_product_last_order_number",
    "orders_since_last_purchase",
    "user_product_reorder_rate",
]


def load_args_to_env(arg_names):
    """Load Glue-style --KEY value arguments into environment variables."""
    for name in arg_names:
        flag = f"--{name}"
        if flag in sys.argv:
            index = sys.argv.index(flag)
            if index + 1 < len(sys.argv):
                os.environ[name] = sys.argv[index + 1]


def require_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment/job argument: {name}")
    return value


def table_ref(table_name):
    prefix = os.getenv("WAREHOUSE_TABLE_PREFIX", "glue_catalog.gold").strip(".")
    return f"{prefix}.{table_name}" if prefix else table_name


def create_glue_context():
    """Create GlueContext for AWS Glue job compatibility."""
    from pyspark.context import SparkContext
    
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    
    # Configure Iceberg catalog
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    
    return glue_context


def train_model(features_df):
    training_df = (
        features_df.filter(F.col("target_reordered").isNotNull())
        .select("user_id", "product_id", *FEATURE_COLS, "target_reordered")
        .fillna(0, subset=FEATURE_COLS)
        .withColumn("label", F.col("target_reordered").cast("double"))
    )

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    assembled = assembler.transform(training_df).select("features", "label")

    train_count = assembled.count()
    if train_count == 0:
        raise ValueError("No training rows found in mart_user_product_features")

    print(f"Training Spark LogisticRegression on {train_count:,} rows")
    model = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        probabilityCol="probability",
        maxIter=int(os.getenv("ML_MAX_ITER", "30")),
        regParam=float(os.getenv("ML_REG_PARAM", "0.05")),
    ).fit(assembled)

    return model, assembler


def score_candidates(features_df, model, assembler):
    predict_only_unlabeled = os.getenv("PREDICT_ONLY_UNLABELED", "true").lower() == "true"
    candidates = features_df
    if predict_only_unlabeled:
        unlabeled = features_df.filter(F.col("target_reordered").isNull())
        if unlabeled.limit(1).count() > 0:
            candidates = unlabeled

    candidates = candidates.select("user_id", "product_id", *FEATURE_COLS).fillna(0, subset=FEATURE_COLS)
    scored = model.transform(assembler.transform(candidates))

    get_positive_probability = F.udf(lambda probability: float(probability[1]), "double")
    return scored.select(
        "user_id",
        "product_id",
        get_positive_probability("probability").alias("score"),
    )


def build_top_n_recommendations(scored_df, products_df, top_n):
    with_names = scored_df.join(
        products_df.select("product_id", "product_name"),
        on="product_id",
        how="left",
    ).fillna({"product_name": "Unknown Product"})

    window = Window.partitionBy("user_id").orderBy(F.col("score").desc())
    top_rows = (
        with_names.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= top_n)
        .select(
            "user_id",
            F.struct(
                F.col("rank"),
                F.col("product_id"),
                F.col("product_name"),
                F.round(F.col("score"), 6).alias("score"),
            ).alias("product"),
        )
    )

    return top_rows.groupBy("user_id").agg(
        F.array_sort(F.collect_list("product")).alias("products")
    )


def make_mongodb_partition_writer(mongodb_uri, database, collection_name, model_version):
    def write_partition(rows):
        client = MongoClient(mongodb_uri)
        collection = client[database][collection_name]

        operations = []
        generated_at = datetime.utcnow()
        for row in rows:
            products = [
                {
                    "product_id": int(product["product_id"]),
                    "product_name": product["product_name"],
                    "score": float(product["score"]),
                }
                for product in row["products"]
            ]
            operations.append(
                UpdateOne(
                    {"user_id": int(row["user_id"])},
                    {
                        "$set": {
                            "user_id": int(row["user_id"]),
                            "products": products,
                            "model_version": model_version,
                            "generated_at": generated_at,
                        }
                    },
                    upsert=True,
                )
            )

            if len(operations) >= 1000:
                collection.bulk_write(operations, ordered=False)
                operations = []

        if operations:
            collection.bulk_write(operations, ordered=False)

        client.close()

    return write_partition


def main():
    # Initialize Glue Job
    try:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        glue_context = create_glue_context()
        spark = glue_context.spark_session
        job = Job(glue_context)
        job.init(args['JOB_NAME'], args)
    except Exception:
        # Fallback for local testing without Glue environment
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder.appName("instacart-spark-ml-recommendations")
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate()
        )
        job = None
    
    load_args_to_env(
        [
            "MONGODB_URI",
            "MONGODB_DATABASE",
            "MONGODB_RECOMMENDATIONS_COLLECTION",
            "WAREHOUSE_TABLE_PREFIX",
            "TOP_N",
            "ML_MAX_ITER",
            "ML_REG_PARAM",
            "PREDICT_ONLY_UNLABELED",
            "MODEL_VERSION",
        ]
    )
    mongodb_uri = require_env("MONGODB_URI")
    mongodb_database = os.getenv("MONGODB_DATABASE", "instacart_ml_warehouse")
    mongodb_collection = os.getenv("MONGODB_RECOMMENDATIONS_COLLECTION", "recommendations")
    model_version = os.getenv("MODEL_VERSION", "spark_logistic_regression_v1")

    top_n = int(os.getenv("TOP_N", "10"))
    spark.sparkContext.setLogLevel("WARN")

    try:
        features_df = spark.table(table_ref("mart_user_product_features"))
        products_df = spark.table(table_ref("dim_product"))

        model, assembler = train_model(features_df)
        scored_df = score_candidates(features_df, model, assembler)
        recommendations_df = build_top_n_recommendations(scored_df, products_df, top_n)

        user_count = recommendations_df.count()
        print(f"Writing recommendations for {user_count:,} users to MongoDB Atlas")
        recommendations_df.foreachPartition(
            make_mongodb_partition_writer(
                mongodb_uri,
                mongodb_database,
                mongodb_collection,
                model_version,
            )
        )
        print("Spark ML recommendation job completed")
        
        # Commit Glue job if running in Glue
        if job:
            job.commit()
        
        return 0
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
