"""Iceberg catalog/table contracts for the NYC HVFHV lakehouse."""

from .catalog import TABLE_SPECS, TableSpec, namespace_ddl, table_ddl

__all__ = ["TABLE_SPECS", "TableSpec", "namespace_ddl", "table_ddl"]
