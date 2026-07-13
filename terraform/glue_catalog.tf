# AWS Glue Data Catalog Configuration

resource "aws_glue_catalog_database" "instacart" {
  name        = "${var.project_name}_${var.environment}"
  description = "Instacart Market Basket Analysis Lakehouse - Iceberg Tables"
  
  location_uri = "s3://${aws_s3_bucket.lakehouse.id}/warehouse/"
  
  parameters = {
    "classification" = "iceberg"
    "table_type"     = "ICEBERG"
  }
}

# Bronze schema (namespace)
# Tables: orders, products, aisles, departments, order_products_prior, order_products_train

# Silver schema (namespace)
# Tables: orders_enriched, order_products_enriched, products_hierarchy

# Gold schema (namespace)
# Tables: Created by dbt (dimensions, facts, marts)

# Note: Tables are created dynamically by Glue Jobs and dbt, not pre-defined here
# Glue Catalog discovers table metadata from Iceberg metadata files in S3
