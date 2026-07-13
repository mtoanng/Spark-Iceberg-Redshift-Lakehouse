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
    
    # ========================================
    # METRICS STORE METHODS
    # ========================================
    
    def register_metric(self, metric_def: Dict[str, Any]) -> Dict[str, Any]:
        """
        Register a new business metric definition
        
        Args:
            metric_def: Metric definition dict with:
                - metric_name (required)
                - sql_template (required)
                - display_name (optional)
                - description (optional)
                - materialization (optional): 'table' or 'view'
                - tags (optional): List of tags
                - owner (optional)
                - parameters (optional): List of parameter defs
        
        Returns:
            Dict with status and metric_name
        
        Example:
            >>> client.register_metric({
            ...     "metric_name": "my_metric",
            ...     "sql_template": "SELECT * FROM gold.dim_product LIMIT 10",
            ...     "tags": ["test"]
            ... })
        """
        response = self.session.post(
            f"{self.base_url}/metrics",
            json=metric_def
        )
        response.raise_for_status()
        return response.json()
    
    def list_metrics(
        self, 
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List all registered metrics with optional filtering
        
        Args:
            tags: Filter by tags (OR logic)
            owner: Filter by owner
        
        Returns:
            Dict with count and list of metrics
        
        Example:
            >>> metrics = client.list_metrics(tags=["basket", "hourly"])
            >>> print(f"Found {metrics['count']} metrics")
        """
        params = {}
        if tags:
            params["tags"] = ",".join(tags)
        if owner:
            params["owner"] = owner
        
        response = self.session.get(
            f"{self.base_url}/metrics",
            params=params
        )
        response.raise_for_status()
        return response.json()
    
    def get_metric(self, metric_name: str) -> Dict[str, Any]:
        """
        Get full metric definition including execution history
        
        Args:
            metric_name: Metric identifier
        
        Returns:
            Full metric document
        
        Example:
            >>> metric = client.get_metric("avg_basket_size_by_hour")
            >>> print(metric['sql_template'])
        """
        response = self.session.get(f"{self.base_url}/metrics/{metric_name}")
        response.raise_for_status()
        return response.json()
    
    def execute_metric(
        self, 
        metric_name: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a metric and materialize results in DuckDB
        
        Args:
            metric_name: Metric identifier
            parameters: Optional dict of parameter values
        
        Returns:
            Dict with status, execution_time_ms, row_count, preview, table_name
        
        Example:
            >>> result = client.execute_metric("top_reordered_products", {
            ...     "min_orders": 100,
            ...     "limit": 20
            ... })
            >>> print(f"Executed in {result['execution_time_ms']}ms")
        """
        response = self.session.post(
            f"{self.base_url}/metrics/{metric_name}/execute",
            json=parameters or {}
        )
        response.raise_for_status()
        return response.json()
    
    def delete_metric(self, metric_name: str) -> Dict[str, Any]:
        """
        Delete a metric definition and materialized table
        
        Args:
            metric_name: Metric to delete
        
        Returns:
            Dict with status and message
        """
        response = self.session.delete(f"{self.base_url}/metrics/{metric_name}")
        response.raise_for_status()
        return response.json()
    
    def get_metric_lineage(self, metric_name: str) -> Dict[str, Any]:
        """
        Get metric lineage (upstream and downstream dependencies)
        
        Args:
            metric_name: Metric identifier
        
        Returns:
            Dict with metric_name, upstream, downstream
        
        Example:
            >>> lineage = client.get_metric_lineage("revenue_growth")
            >>> print(f"Depends on: {lineage['upstream']}")
        """
        response = self.session.get(
            f"{self.base_url}/metrics/{metric_name}/lineage"
        )
        response.raise_for_status()
        return response.json()
    
    def refresh_metrics(self, tags: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Refresh all metrics (or filtered by tags)
        
        Args:
            tags: Optional tag filter
        
        Returns:
            Dict with status, total, success, failed, results
        
        Example:
            >>> result = client.refresh_metrics(tags=["daily"])
            >>> print(f"Success: {result['success']}/{result['total']}")
        """
        params = {}
        if tags:
            params["tags"] = ",".join(tags)
        
        response = self.session.post(
            f"{self.base_url}/metrics/refresh",
            params=params
        )
        response.raise_for_status()
        return response.json()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
