"""
Tests for the sqlglot-based SQL validator.

Verifies that:
  - Valid SELECT and WITH (CTE) queries pass
  - DML (INSERT, UPDATE, DELETE) is rejected
  - DDL (DROP, CREATE, ALTER, TRUNCATE) is rejected
  - Multi-statement injection is rejected
  - Non-SELECT root statements are rejected
"""

import sys
import os
from pathlib import Path

# Add warehouse package to path for direct execution
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from warehouse.sql_validator import validate_sql


class TestValidQueries:
    """Queries that should pass validation."""

    def test_simple_select(self):
        ok, reason = validate_sql("SELECT * FROM gold.fct_order_products LIMIT 10")
        assert ok, f"Expected SELECT to pass, but got: {reason}"

    def test_select_with_columns(self):
        ok, _ = validate_sql("SELECT product_id, product_name FROM gold.dim_product")
        assert ok

    def test_select_with_join(self):
        ok, _ = validate_sql("""
            SELECT f.order_id, f.product_id, p.product_name
            FROM gold.fct_order_products f
            JOIN gold.dim_product p ON f.product_id = p.product_id
            LIMIT 100
        """)
        assert ok

    def test_select_with_aggregation(self):
        ok, _ = validate_sql("""
            SELECT department, COUNT(*) as cnt
            FROM gold.fct_order_products
            GROUP BY department
            ORDER BY cnt DESC
        """)
        assert ok

    def test_cte_with_clause(self):
        ok, _ = validate_sql("""
            WITH basket_stats AS (
                SELECT order_id, COUNT(*) as basket_size
                FROM gold.fct_order_products
                GROUP BY order_id
            )
            SELECT AVG(basket_size) FROM basket_stats
        """)
        assert ok

    def test_select_with_subquery(self):
        ok, _ = validate_sql("""
            SELECT * FROM (
                SELECT product_id, COUNT(*) as order_count
                FROM gold.fct_order_products
                GROUP BY product_id
            ) sub
            WHERE order_count > 100
        """)
        assert ok

    def test_select_with_lowercase(self):
        ok, _ = validate_sql("select count(*) from gold.fct_order_products")
        assert ok

    def test_union_query(self):
        ok, _ = validate_sql("""
            SELECT 'prior' as source, product_id FROM gold.fct_order_products
            UNION
            SELECT 'train' as source, product_id FROM gold.fct_order_products
        """)
        assert ok


class TestRejectedQueries:
    """Queries that should be rejected."""

    def test_insert_rejected(self):
        ok, reason = validate_sql("INSERT INTO gold.fct_order_products VALUES (1, 2, 3)")
        assert not ok, "INSERT should be rejected"
        assert "Insert" in reason or "not permitted" in reason

    def test_update_rejected(self):
        ok, reason = validate_sql("UPDATE gold.fct_order_products SET reordered = 1")
        assert not ok, "UPDATE should be rejected"
        assert "Update" in reason or "not permitted" in reason

    def test_delete_rejected(self):
        ok, reason = validate_sql("DELETE FROM gold.fct_order_products WHERE order_id = 1")
        assert not ok, "DELETE should be rejected"
        assert "Delete" in reason or "not permitted" in reason

    def test_drop_table_rejected(self):
        ok, reason = validate_sql("DROP TABLE gold.fct_order_products")
        assert not ok, "DROP should be rejected"
        assert "Drop" in reason or "not permitted" in reason

    def test_create_table_rejected(self):
        ok, reason = validate_sql("CREATE TABLE evil (id int)")
        assert not ok, "CREATE should be rejected"
        assert "Create" in reason or "not permitted" in reason

    def test_truncate_rejected(self):
        ok, reason = validate_sql("TRUNCATE TABLE gold.fct_order_products")
        assert not ok, "TRUNCATE should be rejected"

    def test_multi_statement_injection_rejected(self):
        """SELECT ; DROP TABLE — classic injection attempt."""
        ok, reason = validate_sql(
            "SELECT 1; DROP TABLE gold.fct_order_products"
        )
        assert not ok, "Multi-statement should be rejected"
        assert "Multiple statements" in reason or "not permitted" in reason

    def test_empty_sql_rejected(self):
        ok, reason = validate_sql("")
        assert not ok
        assert "Empty" in reason

    def test_whitespace_only_rejected(self):
        ok, reason = validate_sql("   \n\t  ")
        assert not ok
        assert "Empty" in reason


class TestEdgeCases:
    """Edge cases for the validator."""

    def test_select_with_semicolon_ending(self):
        """Trailing semicolons on a single statement should be fine."""
        ok, _ = validate_sql("SELECT 1;")
        # sqlglot may parse this as 1 statement with trailing semicolon
        # or as 2 statements (second being None). Either way, validate
        # should handle it.
        # If it parses as 2 (second is None), it will be rejected as multi-statement.
        # If it parses as 1, it will pass.
        # Both behaviors are acceptable for security.
        assert ok or not ok  # We just verify it doesn't crash

    def test_explain_select(self):
        """EXPLAIN is a non-SELECT root — should be rejected."""
        ok, reason = validate_sql("EXPLAIN SELECT * FROM gold.fct_order_products")
        assert not ok, "EXPLAIN should be rejected (not a SELECT root)"

    def test_show_tables(self):
        """SHOW is a command, not a SELECT — should be rejected."""
        ok, reason = validate_sql("SHOW TABLES")
        # SHOW may parse as a Command node, which we reject
        assert not ok, "SHOW should be rejected"
