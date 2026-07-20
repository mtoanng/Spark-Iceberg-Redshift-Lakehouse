locals {
  glue_databases = toset(["bronze", "silver", "ops", "gold"])
}

resource "aws_glue_catalog_database" "namespace" {
  for_each    = local.glue_databases
  name        = each.value
  description = "NYC TLC HVFHV ${each.value} namespace in the canonical Glue Catalog."
}
