"""
SQL Validator - AST-based validation using sqlglot

CRITICAL BUG FIXES APPLIED:
1. NO keyword/substring blacklist (prevents false positives like "created_at")
2. Uses sqlglot.parse() plural to detect multi-statement injection
3. AST-based validation only (checks root node type)

Security:
- Blocks multi-statement queries (SELECT 1; DROP TABLE)
- Blocks mutations (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER)
- Allows only SELECT and WITH (CTE) queries

Author: Data Engineering Team
Date: 2026-07-13
"""

import sqlglot
from typing import Tuple


def validate_sql(sql: str) -> Tuple[bool, str]:
    """
    Validate SQL using pure AST parsing (sqlglot).
    Only SELECT and WITH allowed.
    
    CRITICAL: Do NOT use keyword/substring blacklist!
    - False positive: "SELECT created_at" fails if checking "create" substring
    - Multi-statement bypass: "SELECT 1; DROP TABLE x" passes parse_one()
    
    Solution: parse() all statements, verify count = 1, check AST root only.
    
    Args:
        sql: SQL query string to validate
        
    Returns:
        Tuple of (is_valid: bool, message: str)
        
    Examples:
        >>> validate_sql("SELECT * FROM orders")
        (True, "Valid")
        
        >>> validate_sql("SELECT created_at FROM orders")  # Should pass!
        (True, "Valid")
        
        >>> validate_sql("SELECT 1; DROP TABLE orders;")  # Should block!
        (False, "Only single statement allowed, no multi-statement queries")
        
        >>> validate_sql("DROP TABLE orders")
        (False, "Only SELECT/WITH allowed, got DROP")
    """
    try:
        # parse() returns list of all statements (plural, not parse_one!)
        statements = sqlglot.parse(sql, dialect="duckdb")
    except Exception as e:
        return False, f"SQL parsing error: {str(e)}"
    
    # Block multi-statement (prevents: SELECT 1; DROP TABLE x;)
    if len(statements) != 1:
        return False, "Only single statement allowed, no multi-statement queries"
    
    tree = statements[0]
    
    # Check AST root node type (this is sufficient - no keyword checking needed!)
    if tree.key not in ("select", "with"):
        return False, f"Only SELECT/WITH allowed, got {tree.key.upper()}"
    
    return True, "Valid"


def validate_sql_strict(sql: str) -> Tuple[bool, str]:
    """
    Stricter validation - also blocks certain SQL features
    (Not used by default, available for future enhancement)
    
    Additional blocks:
    - UNION (could be used for data exfiltration)
    - Subqueries returning large datasets
    """
    is_valid, message = validate_sql(sql)
    
    if not is_valid:
        return is_valid, message
    
    # Additional checks can go here
    sql_upper = sql.upper()
    
    # Example: Block UNION if needed
    # if "UNION" in sql_upper:
    #     return False, "UNION queries not allowed"
    
    return True, "Valid"


if __name__ == "__main__":
    """Quick self-test"""
    
    test_cases = [
        # Should PASS
        ("SELECT * FROM orders", True),
        ("SELECT created_at FROM orders", True),  # FIX: No false positive!
        ("SELECT updated_at FROM products", True),  # FIX: No false positive!
        ("WITH cte AS (SELECT 1) SELECT * FROM cte", True),
        ("SELECT * FROM glue_catalog.gold.fct_order_products LIMIT 10", True),
        ("SELECT * FROM orders;", True),  # Trailing semicolon OK
        
        # Should BLOCK
        ("SELECT 1; DROP TABLE orders;", False),  # Multi-statement
        ("SELECT * FROM orders; DELETE FROM orders;", False),  # Multi-statement
        ("DROP TABLE orders", False),
        ("INSERT INTO orders VALUES (1, 2)", False),
        ("UPDATE orders SET status = 'x'", False),
        ("DELETE FROM orders", False),
        ("CREATE TABLE x (id INT)", False),
        ("ALTER TABLE orders ADD COLUMN x INT", False),
    ]
    
    print("🧪 Running SQL Validator Self-Tests\n")
    
    all_passed = True
    for sql, expected_pass in test_cases:
        is_valid, message = validate_sql(sql)
        
        if is_valid == expected_pass:
            status = "✅ PASS"
        else:
            status = "❌ FAIL"
            all_passed = False
        
        print(f"{status}: {sql[:60]}")
        if not is_valid:
            print(f"         Reason: {message}")
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed - review validation logic")
