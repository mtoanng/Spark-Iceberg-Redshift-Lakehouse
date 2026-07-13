"""
Warehouse Client SDK - Python client for warehouse API

Usage:
    from warehouse_client import WarehouseClient
    
    client = WarehouseClient("http://localhost:8000")
    
    # Execute SQL query
    result = client.query("SELECT * FROM glue_catalog.gold.fct_order_products LIMIT 10")
    
    # Get recommendations
    recs = client.get_recommendations(user_id=12345)

Author: Data Engineering Team
Date: 2026-07-13
"""

import requests
from typing import Dict, List, Optional, Any


class WarehouseClient:
    """
    Python client for Instacart Warehouse API
    
    Provides convenient methods for:
    - SQL queries (validated, read-only)
    - Product recommendations (ML-powered)
    - Catalog exploration
    """
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize client
        
        Args:
            base_url: Warehouse API base URL (default: http://localhost:8000)
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
    
    def health(self) -> Dict[str, Any]:
        """
        Check warehouse service health
        
        Returns:
            Dict with status, version, engine info
        """
        response = self.session.get(f"{self.base_url}/")
        response.raise_for_status()
        return response.json()
    
    def query(self, sql: str, params: Optional[List] = None) -> Dict[str, Any]:
        """
        Execute SQL query
        
        Args:
            sql: SQL query string (SELECT/WITH only)
            params: Optional query parameters
            
        Returns:
            Dict with columns, rows, row_count
            
        Raises:
            requests.HTTPError: If query validation fails or execution error
            
        Example:
            result = client.query(
                "SELECT * FROM glue_catalog.gold.fct_order_products LIMIT 10"
            )
            
            for row in result['rows']:
                print(row)
        """
        response = self.session.post(
            f"{self.base_url}/query",
            json={"sql": sql, "params": params or []}
        )
        response.raise_for_status()
        return response.json()
    
    def get_recommendations(self, user_id: int) -> Dict[str, Any]:
        """
        Get product recommendations for a user
        
        Args:
            user_id: User ID
            
        Returns:
            Dict with user_id, products list, model_version, generated_at
            
        Raises:
            requests.HTTPError: If user not found (404)
            
        Example:
            recs = client.get_recommendations(12345)
            
            print(f"Top products for user {recs['user_id']}:")
            for p in recs['products']:
                print(f"  {p['product_name']}: {p['score']:.2f}")
        """
        response = self.session.get(
            f"{self.base_url}/recommendations/{user_id}"
        )
        response.raise_for_status()
        return response.json()
    
    def get_recommendations_stats(self) -> Dict[str, Any]:
        """
        Get recommendation store statistics
        
        Returns:
            Dict with total_users, model_version, last_generated
        """
        response = self.session.get(
            f"{self.base_url}/recommendations/stats"
        )
        response.raise_for_status()
        return response.json()
    
    def list_tables(self, database: str = "gold") -> List[str]:
        """
        List tables in a database/schema
        
        Args:
            database: Database/schema name (default: gold)
            
        Returns:
            List of table names
        """
        response = self.session.get(
            f"{self.base_url}/tables/{database}"
        )
        response.raise_for_status()
        return response.json()["tables"]
    
    def get_schema(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get schema for a table
        
        Args:
            table_name: Fully qualified table name
            
        Returns:
            List of column dicts with column, type, nullable
            
        Example:
            schema = client.get_schema("glue_catalog.gold.fct_order_products")
            for col in schema:
                print(f"{col['column']}: {col['type']}")
        """
        response = self.session.get(
            f"{self.base_url}/schema/{table_name}"
        )
        response.raise_for_status()
        return response.json()["columns"]
    
    def close(self):
        """Close HTTP session"""
        self.session.close()
    
    def __enter__(self):
        """Context manager support"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.close()


if __name__ == "__main__":
    """Quick self-test (requires running warehouse API)"""
    
    print("\n" + "=" * 60)
    print("🧪 Warehouse Client Self-Test")
    print("=" * 60 + "\n")
    
    client = WarehouseClient("http://localhost:8000")
    
    try:
        # Test health check
        health = client.health()
        print(f"✅ Health check: {health['status']}")
        
        # Test simple query
        result = client.query("SELECT 42 as answer")
        print(f"✅ Query result: {result['rows']}")
        
        # Test recommendations stats
        stats = client.get_recommendations_stats()
        print(f"✅ Recommendation stats: {stats['total_users']} users")
        
        print("\n✅ Self-test passed!")
        
    except requests.exceptions.ConnectionError:
        print("\n⚠️  Cannot connect to warehouse API (is it running?)")
        print("   Start with: docker-compose up -d")
        
    except Exception as e:
        print(f"\n❌ Self-test failed: {e}")
        
    finally:
        client.close()
