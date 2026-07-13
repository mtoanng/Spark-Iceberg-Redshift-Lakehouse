"""
Metrics execution engine - Execute business metrics defined in MongoDB
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import re


class MetricsEngine:
    """
    Execute business metrics stored in MongoDB.
    
    Pattern: Configuration as Data (not Code)
    - Metrics defined in MongoDB (not YAML)
    - Executed on-demand with parameters
    - Results materialized in DuckDB
    - Execution history tracked in MongoDB
    """
    
    def __init__(self, metadata_store, duckdb_engine):
        """
        Initialize metrics engine
        
        Args:
            metadata_store: MetadataStore instance (MongoDB)
            duckdb_engine: DuckDBEngine instance (query execution)
        """
        self.store = metadata_store
        self.duckdb = duckdb_engine
        self.metrics = self.store.db['metrics']
    
    def register_metric(self, metric_definition: Dict[str, Any]) -> str:
        """
        Register a new business metric (replaces YAML file creation)
        
        Args:
            metric_definition: Metric config with name, sql, parameters, etc.
            
        Returns:
            metric_name: Created metric identifier
            
        Example:
            metrics_engine.register_metric({
                "metric_name": "avg_basket_size",
                "display_name": "Average Basket Size by Hour",
                "description": "Average products per order by hour of day",
                "owner": "analytics-team",
                "sql_template": "SELECT order_hour_of_day, AVG(products_per_order) as avg_basket_size FROM gold.dim_orders GROUP BY 1",
                "materialization": "table",
                "refresh_schedule": "0 6 * * *",
                "tags": ["basket", "hourly", "behavior"],
                "parameters": []
            })
        """
        required_fields = ["metric_name", "sql_template"]
        for field in required_fields:
            if field not in metric_definition:
                raise ValueError(f"Missing required field: {field}")
        
        # Add metadata
        metric_definition["created_at"] = datetime.utcnow()
        metric_definition["version"] = "1.0.0"
        metric_definition["last_run_status"] = "pending"
        metric_definition["execution_count"] = 0
        
        # Set defaults
        metric_definition.setdefault("materialization", "table")
        metric_definition.setdefault("tags", [])
        metric_definition.setdefault("parameters", [])
        metric_definition.setdefault("enabled", True)
        
        # Insert into MongoDB
        result = self.metrics.insert_one(metric_definition)
        
        return metric_definition["metric_name"]
    
    def update_metric(self, metric_name: str, updates: Dict[str, Any]) -> bool:
        """
        Update an existing metric (replaces editing YAML + redeploy)
        
        Args:
            metric_name: Metric to update
            updates: Fields to update (sql_template, description, etc.)
            
        Returns:
            bool: True if updated
            
        Example:
            metrics_engine.update_metric("avg_basket_size", {
                "sql_template": "SELECT ... WHERE order_date >= '2024-01-01'",
                "description": "Updated to filter recent orders only"
            })
        """
        updates["updated_at"] = datetime.utcnow()
        
        # Increment version on SQL changes
        if "sql_template" in updates:
            existing = self.metrics.find_one({"metric_name": metric_name})
            if existing:
                version = existing.get("version", "1.0.0")
                major, minor, patch = map(int, version.split("."))
                updates["version"] = f"{major}.{minor + 1}.{patch}"
        
        result = self.metrics.update_one(
            {"metric_name": metric_name},
            {"$set": updates}
        )
        
        return result.modified_count > 0
    
    def execute_metric(
        self, 
        metric_name: str, 
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a metric and materialize results in DuckDB
        
        Args:
            metric_name: Metric to execute
            parameters: Optional runtime parameters (e.g., {"start_date": "2024-01-01"})
            
        Returns:
            Execution result with status, preview data, timing
            
        Example:
            result = metrics_engine.execute_metric(
                "top_reordered_products",
                parameters={"min_orders": 100, "limit": 20}
            )
        """
        # Fetch metric from MongoDB
        metric = self.metrics.find_one({"metric_name": metric_name})
        if not metric:
            raise ValueError(f"Metric '{metric_name}' not found in catalog")
        
        if not metric.get("enabled", True):
            raise ValueError(f"Metric '{metric_name}' is disabled")
        
        # Get SQL template
        sql = metric['sql_template']
        
        # Substitute parameters using {param_name} syntax
        if parameters:
            for param_name, param_value in parameters.items():
                # Validate parameter is declared
                declared_params = {p['name'] for p in metric.get('parameters', [])}
                if param_name not in declared_params and declared_params:
                    raise ValueError(
                        f"Parameter '{param_name}' not declared for metric '{metric_name}'. "
                        f"Declared: {declared_params}"
                    )
                
                # Safe substitution (prevent SQL injection)
                if isinstance(param_value, str):
                    param_value = f"'{param_value}'"
                
                sql = sql.replace(f"{{{param_name}}}", str(param_value))
        
        # Apply default parameters if not provided
        for param in metric.get('parameters', []):
            placeholder = f"{{{param['name']}}}"
            if placeholder in sql:
                default_value = param.get('default')
                if default_value is None:
                    raise ValueError(
                        f"Required parameter '{param['name']}' not provided "
                        f"and no default value"
                    )
                if isinstance(default_value, str):
                    default_value = f"'{default_value}'"
                sql = sql.replace(placeholder, str(default_value))
        
        # Execute
        start_time = datetime.utcnow()
        try:
            # Materialize based on config
            materialization = metric.get('materialization', 'table')
            table_name = f"metric_{metric_name}"
            
            if materialization == 'table':
                self.duckdb.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    {sql}
                """)
            elif materialization == 'view':
                self.duckdb.conn.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    {sql}
                """)
            else:
                raise ValueError(f"Unknown materialization: {materialization}")
            
            # Get preview (first 10 rows)
            preview_df = self.duckdb.conn.execute(
                f"SELECT * FROM {table_name} LIMIT 10"
            ).fetchdf()
            
            # Get row count
            row_count = self.duckdb.conn.execute(
                f"SELECT COUNT(*) as cnt FROM {table_name}"
            ).fetchone()[0]
            
            execution_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            # Update execution status in MongoDB
            self.metrics.update_one(
                {"metric_name": metric_name},
                {
                    "$set": {
                        "last_run_at": datetime.utcnow(),
                        "last_run_status": "success",
                        "last_execution_time_ms": execution_time_ms,
                        "last_row_count": row_count
                    },
                    "$inc": {"execution_count": 1}
                }
            )
            
            return {
                "status": "success",
                "metric_name": metric_name,
                "execution_time_ms": round(execution_time_ms, 2),
                "row_count": row_count,
                "materialization": materialization,
                "table_name": table_name,
                "preview": preview_df.to_dict('records'),
                "columns": list(preview_df.columns)
            }
            
        except Exception as e:
            execution_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            # Record failure
            self.metrics.update_one(
                {"metric_name": metric_name},
                {
                    "$set": {
                        "last_run_at": datetime.utcnow(),
                        "last_run_status": "failed",
                        "last_error": str(e),
                        "last_execution_time_ms": execution_time_ms
                    },
                    "$inc": {"execution_count": 1}
                }
            )
            
            raise ValueError(f"Metric execution failed: {str(e)}")
    
    def list_metrics(
        self, 
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List metrics with optional filters
        
        Args:
            tags: Filter by tags (OR logic)
            owner: Filter by owner team
            status: Filter by last_run_status
            
        Returns:
            List of metric summaries
        """
        query = {}
        
        if tags:
            query['tags'] = {"$in": tags}
        
        if owner:
            query['owner'] = owner
        
        if status:
            query['last_run_status'] = status
        
        return list(self.metrics.find(
            query,
            {
                "_id": 0,
                "metric_name": 1,
                "display_name": 1,
                "description": 1,
                "owner": 1,
                "tags": 1,
                "last_run_status": 1,
                "last_run_at": 1,
                "execution_count": 1,
                "enabled": 1
            }
        ).sort("metric_name", 1))
    
    def get_metric(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """
        Get full metric definition including execution history
        
        Args:
            metric_name: Metric identifier
            
        Returns:
            Full metric document from MongoDB
        """
        metric = self.metrics.find_one(
            {"metric_name": metric_name},
            {"_id": 0}
        )
        return metric
    
    def delete_metric(self, metric_name: str) -> bool:
        """
        Delete a metric definition
        
        Args:
            metric_name: Metric to delete
            
        Returns:
            bool: True if deleted
        """
        result = self.metrics.delete_one({"metric_name": metric_name})
        
        # Also drop materialized table/view if exists
        try:
            table_name = f"metric_{metric_name}"
            self.duckdb.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.duckdb.conn.execute(f"DROP VIEW IF EXISTS {table_name}")
        except:
            pass
        
        return result.deleted_count > 0
    
    def get_execution_history(
        self, 
        metric_name: str, 
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get execution history for a metric
        
        Note: Currently shows last execution only (stored in metric doc).
        For full history tracking, add a separate 'metric_executions' collection.
        
        Args:
            metric_name: Metric identifier
            limit: Max records to return
            
        Returns:
            List of execution records
        """
        metric = self.get_metric(metric_name)
        if not metric:
            return []
        
        # Simple implementation: return last execution info
        if metric.get('last_run_at'):
            return [{
                "metric_name": metric_name,
                "executed_at": metric['last_run_at'],
                "status": metric['last_run_status'],
                "execution_time_ms": metric.get('last_execution_time_ms'),
                "row_count": metric.get('last_row_count'),
                "error": metric.get('last_error')
            }]
        
        return []
    
    def search_metrics(self, search_term: str) -> List[Dict[str, Any]]:
        """
        Search metrics by name or description
        
        Args:
            search_term: Text to search for
            
        Returns:
            Matching metrics
        """
        regex_pattern = {"$regex": search_term, "$options": "i"}
        
        return list(self.metrics.find(
            {
                "$or": [
                    {"metric_name": regex_pattern},
                    {"display_name": regex_pattern},
                    {"description": regex_pattern}
                ]
            },
            {
                "_id": 0,
                "metric_name": 1,
                "display_name": 1,
                "description": 1,
                "tags": 1
            }
        ))

    
    def get_lineage(self, metric_name: str) -> Dict[str, Any]:
        """
        Get metric lineage (upstream dependencies and downstream dependents)
        
        Args:
            metric_name: Metric identifier
            
        Returns:
            Dict with metric_name, upstream, downstream
        """
        metric = self.get_metric(metric_name)
        if not metric:
            raise ValueError(f"Metric '{metric_name}' not found")
        
        upstream = metric.get('depends_on', [])
        
        # Find downstream (metrics that depend on this one)
        downstream = []
        for m in self.metrics.find({"depends_on": metric_name}):
            downstream.append(m['metric_name'])
        
        return {
            "metric_name": metric_name,
            "upstream": upstream,
            "downstream": downstream
        }
    
    def refresh_all(
        self, 
        tags: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Refresh all metrics (or filtered by tags)
        
        Args:
            tags: Optional tag filter
            
        Returns:
            List of execution results
        """
        metrics = self.list_metrics(tags=tags)
        
        results = []
        for metric in metrics:
            metric_name = metric['metric_name']
            try:
                result = self.execute_metric(metric_name)
                results.append({
                    "metric_name": metric_name,
                    "status": result['status'],
                    "execution_time_ms": result['execution_time_ms'],
                    "row_count": result['row_count']
                })
            except Exception as e:
                results.append({
                    "metric_name": metric_name,
                    "status": "failed",
                    "error": str(e)
                })
        
        return results
