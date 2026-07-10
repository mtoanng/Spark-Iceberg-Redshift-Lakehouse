"""
DuckDB query engine for Iceberg Gold layer
"""

import duckdb
import pandas as pd
from typing import Dict, List, Any
import time
import os


class DuckDBEngine:
    """DuckDB engine for querying Iceberg Gold tables"""
    
    def __init__(self, iceberg_path: str = None):
        """
        Initialize DuckDB connection with Iceberg catalog
        
        Args:
            iceberg_path: S3 path to Iceberg warehouse (e.g., s3://bucket/gold/)
        """
        self.iceberg_path = iceberg_path or os.getenv("S3_GOLD_PATH", "s3://instacart-lakehouse/gold")
        self.conn = duckdb.connect(database=':memory:', read_only=False)
        
        # Install and load Iceberg extension
        self.conn.execute("INSTALL iceberg")
        self.conn.execute("LOAD iceberg")
        
        # Configure AWS credentials if available
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION", "us-east-1")
        
        if aws_key and aws_secret:
            self.conn.execute(f"""
                CREATE SECRET aws_secret (
                    TYPE S3,
                    KEY_ID '{aws_key}',
                    SECRET '{aws_secret}',
                    REGION '{aws_region}'
                )
            """)
    
    def query(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL query and return results
        
        Args:
            sql: SQL query string
            
        Returns:
            Dictionary with columns, rows, row_count, execution_time_ms
        """
        start_time = time.time()
        
        try:
            result = self.conn.execute(sql).fetchdf()
            execution_time = (time.time() - start_time) * 1000  # Convert to ms
            
            return {
                "columns": result.columns.tolist(),
                "rows": result.values.tolist(),
                "row_count": len(result),
                "execution_time_ms": round(execution_time, 2)
            }
        except Exception as e:
            raise ValueError(f"Query execution failed: {str(e)}")
    
    def list_tables(self) -> List[str]:
        """List all available tables in the catalog"""
        result = self.conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get schema for a specific table"""
        result = self.conn.execute(f"DESCRIBE {table_name}").fetchdf()
        return [
            {"name": row["column_name"], "type": row["column_type"]}
            for _, row in result.iterrows()
        ]
    
    def close(self):
        """Close DuckDB connection"""
        self.conn.close()
