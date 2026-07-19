resource "aws_glue_catalog_database" "nyc" {
  name        = "${var.project_name}-${var.environment}"
  description = "NYC TLC HVFHV Iceberg lakehouse catalog."
}
