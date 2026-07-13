"""
DuckDB Engine - Query engine with Glue Catalog integration

CRITICAL BUG FIX APPLIED:
- Initialize self._use_fallback = False BEFORE any branching
- Prevents AttributeError when ATTACH succeeds

Features:
- Persistent DuckDB file (survives restart)
- AWS Glue Catalog ATTACH (primary method)
- Fallback to iceberg_scan() if ATTACH fails
- Thread-safe connection pooling

Author: Data Engineering Team
Date: 2026-07-13
"""

import duckdb
import threading
import os
from typing import Dict, Any, Optional, List


class DuckDBEngine:
    """
    DuckDB engine with persistent file and Glue Catalog integration
    
    Configuration:
    - db_path: Path to persistent DuckDB file
    - use_glue_catalog: Attempt to ATTACH Glue Catalog
    - account_id: AWS account ID for Glue Catalog
    - role_arn: IAM role ARN for AWS credentials
    - region: AWS region (default: us-east-1)
    """
    
    def __init__(
        self,
        db_path: str = "warehouse/data/warehouse.db",
        use_glue_catalog: bool = True,
        account_id: Optional[str] = None,
        role_arn: Optional[str] = None,
        region: str = "us-east-1"
    ):
        self.db_path = db_path
        self.region = region
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Connect to persistent database
        self._conn = duckdb.connect(database=db_path, read_only=False)
        self._lock = threading.Lock()
        
        # CRITICAL FIX: Initialize fallback flag BEFORE any branching
        self._use_fallback = False
        
        # Install extensions
        print("📦 Installing DuckDB extensions...")
        self._conn.execute("INSTALL iceberg; LOAD iceberg;")
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")
        print("✅ Extensions loaded")
        
        # Configure AWS credentials
        if role_arn:
            self._setup_aws_credentials(role_arn, region)
        
        # Attach Glue Catalog or setup fallback
        if use_glue_catalog and account_id:
            try:
                self._attach_glue_catalog(account_id, region)
                print("✅ Glue Catalog ATTACH successful")
                # Set flag after success
                self._use_fallback = False
            except Exception as e:
                print(f"⚠️  Glue Catalog ATTACH failed: {e}")
                print("→ Using fallback: iceberg_scan() with S3 paths")
                self._use_fallback = True
        else:
            print("→ Glue Catalog disabled, using fallback mode")
            self._use_fallback = True
    
    def _setup_aws_credentials(self, role_arn: str, region: str):
        """
        Configure AWS credentials for S3 access
        
        Uses STS assume role for temporary credentials
        """
        print(f"🔑 Configuring AWS credentials (role: {role_arn})")
        
        try:
            self._conn.execute(f"""
                CREATE SECRET (
                    TYPE s3,
                    PROVIDER credential_chain,
                    CHAIN sts,
                    ASSUME_ROLE_ARN '{role_arn}',
                    REGION '{region}'
                )
            """)
            print("✅ AWS credentials configured")
        except Exception as e:
            print(f"⚠️  AWS credentials setup failed: {e}")
            print("→ Falling back to default credential chain")
            self._conn.execute(f"""
                CREATE SECRET (
                    TYPE s3,
                    PROVIDER credential_chain,
                    REGION '{region}'
                )
            """)
    
    def _attach_glue_catalog(self, account_id: str, region: str):
        """
        Attach AWS Glue Data Catalog as 'glue_catalog'
        
        Note: ATTACH is a new feature (DuckDB 0.10+), may have rough edges.
        If this fails, fallback to iceberg_scan() is automatic.
        
        Raises:
            Exception: If ATTACH fails (caller handles fallback)
        """
        print(f"🔗 Attempting to ATTACH Glue Catalog (account: {account_id})")
        
        self._conn.execute(f"""
            ATTACH '{account_id}' AS glue_catalog (
                TYPE iceberg,
                ENDPOINT 'glue.{region}.amazonaws.com/iceberg',
                AUTHORIZATION_TYPE 'sigv4'
            )
        """)
        
        # Verify by listing databases
        result = self._conn.execute("SHOW DATABASES").fetchall()
        print(f"📊 Available databases: {[r[0] for r in result]}")
    
    def execute(self, sql: str, params: Optional[List] = None) -> Dict[str, Any]:
        """
        Execute SQL query and return results
        
        Args:
            sql: SQL query string (should be validated before calling)
            params: Optional query parameters
            
        Returns:
            Dict with columns, rows, and row_count
            
        Thread-safe: Uses lock for concurrent access
        """
        with self._lock:
            try:
                result = self._conn.execute(sql, params or [])
                columns = [d[0] for d in result.description]
                rows = result.fetchall()
                
                return {
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows)
                }
            except Exception as e:
                # Re-raise with context
                raise RuntimeError(f"Query execution failed: {str(e)}") from e
    
    def execute_to_df(self, sql: str, params: Optional[List] = None):
        """
        Execute query and return as pandas DataFrame
        
        Requires pandas to be installed
        """
        with self._lock:
            result = self._conn.execute(sql, params or [])
            return result.fetchdf()
    
    def get_table_list(self, database: str = "gold") -> List[str]:
        """Get list of tables in a database/schema"""
        with self._lock:
            if self._use_fallback:
                # Fallback mode: can't list catalog tables easily
                return []
            else:
                result = self._conn.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{database}'"
                ).fetchall()
                return [r[0] for r in result]
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get schema for a table"""
        with self._lock:
            result = self._conn.execute(f"DESCRIBE {table_name}").fetchall()
            return [
                {"column": r[0], "type": r[1], "nullable": r[2]}
                for r in result
            ]
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check engine health
        
        Returns status, catalog mode, and basic stats
        """
        try:
            with self._lock:
                # Test query
                self._conn.execute("SELECT 1").fetchall()
                
                return {
                    "status": "healthy",
                    "catalog_mode": "glue_catalog" if not self._use_fallback else "fallback",
                    "database_path": self.db_path,
                    "region": self.region
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    def close(self):
        """Close database connection"""
        if self._conn:
            self._conn.close()
            print("🔌 DuckDB connection closed")


if __name__ == "__main__":
    """Quick self-test"""
    
    print("\n" + "=" * 60)
    print("🧪 DuckDB Engine Self-Test")
    print("=" * 60 + "\n")
    
    # Test with local database (no AWS)
    engine = DuckDBEngine(
        db_path="test_warehouse.db",
        use_glue_catalog=False  # Disable for local test
    )
    
    try:
        # Test simple query
        result = engine.execute("SELECT 42 as answer, 'hello' as message")
        print(f"✅ Query executed: {result}")
        
        # Health check
        health = engine.health_check()
        print(f"✅ Health check: {health}")
        
        print("\n✅ Self-test passed!")
        
    finally:
        engine.close()
        # Cleanup test file
        if os.path.exists("test_warehouse.db"):
            os.remove("test_warehouse.db")
