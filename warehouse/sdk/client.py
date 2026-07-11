"""
Python client for Warehouse API
"""

import requests
import pandas as pd
from typing import List, Dict, Any, Optional


class WarehouseClient:
    """Simple Python client for warehouse API"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize warehouse client
        
        Args:
            base_url: Base URL of warehouse API
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """
        List all available datasets
        
        Returns:
            List of dataset metadata dicts
        """
        response = self.session.get(f"{self.base_url}/datasets")
        response.raise_for_status()
        return response.json()
    
    def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get metadata for specific dataset
        
        Args:
            dataset_id: Dataset identifier (e.g., 'gold.dim_product')
            
        Returns:
            Dataset metadata dict
        """
        response = self.session.get(f"{self.base_url}/datasets/{dataset_id}")
        response.raise_for_status()
        return response.json()
    
    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute SQL query and return pandas DataFrame
        
        Args:
            sql: SQL query string
            
        Returns:
            pandas DataFrame with query results
            
        Example:
            >>> client = WarehouseClient()
            >>> df = client.query("SELECT * FROM gold.dim_product LIMIT 10")
        """
        response = self.session.post(
            f"{self.base_url}/query",
            json={"sql": sql}
        )
        response.raise_for_status()
        
        data = response.json()
        return pd.DataFrame(data["rows"], columns=data["columns"])
    
    def query_raw(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL query and return raw JSON response
        
        Args:
            sql: SQL query string
            
        Returns:
            Dict with columns, rows, row_count, execution_time_ms
        """
        response = self.session.post(
            f"{self.base_url}/query",
            json={"sql": sql}
        )
        response.raise_for_status()
        return response.json()

    def health(self) -> Dict[str, Any]:
        """
        Check detailed health status (DuckDB + MongoDB connectivity)
        
        Returns:
            Dict with status ('healthy'/'degraded') and individual checks
        """
        response = self.session.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()

    def query_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent query history
        
        Args:
            limit: Maximum number of records (default 50)
            
        Returns:
            List of query history records
        """
        response = self.session.get(
            f"{self.base_url}/history", params={"limit": limit}
        )
        response.raise_for_status()
        return response.json()

    def get_contract(self, table: str) -> Dict[str, Any]:
        """
        Get data contract for a table (expectations: not_null, unique, etc.)
        
        Args:
            table: Fully qualified table name (e.g., 'gold.fct_order_products')
            
        Returns:
            Dict with table and expectations
        """
        response = self.session.get(f"{self.base_url}/contracts/{table}")
        response.raise_for_status()
        return response.json()
    
    def close(self):
        """Close HTTP session"""
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
