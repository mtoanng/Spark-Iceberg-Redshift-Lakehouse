"""
FastAPI warehouse service - Simple SQL API for Iceberg Gold layer
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import List, Dict, Any
import os

from .models import QueryRequest, QueryResponse, DatasetMetadata, ErrorResponse
from .engine import DuckDBEngine
from .metadata import MetadataStore
from .metrics_engine import MetricsEngine
from .sql_validator import validate_sql

# Initialize FastAPI app
app = FastAPI(
    title="Instacart Data Warehouse API",
    description="Simple SQL query API for Iceberg Gold layer with Metrics Store",
    version="2.0.0"
)

# Initialize engines (singleton pattern)
duckdb_engine = None
metadata_store = None
metrics_engine = None


@app.on_event("startup")
async def startup_event():
    """Initialize engines on startup"""
    global duckdb_engine, metadata_store, metrics_engine

    # Load row limit from config (with fallback)
    try:
        from config.instacart_config import DUCKDB_DEFAULT_ROW_LIMIT
    except ImportError:
        DUCKDB_DEFAULT_ROW_LIMIT = 10_000

    duckdb_engine = DuckDBEngine(
        iceberg_path=os.getenv("S3_GOLD_PATH", "s3://instacart-lakehouse/gold"),
        row_limit=DUCKDB_DEFAULT_ROW_LIMIT,
    )
    metadata_store = MetadataStore(
        uri=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        database=os.getenv("MONGODB_DATABASE", "instacart_metadata")
    )
    metrics_engine = MetricsEngine(metadata_store, duckdb_engine)


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if duckdb_engine:
        duckdb_engine.close()
    if metadata_store:
        metadata_store.close()


@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "service": "Instacart Data Warehouse API",
        "status": "healthy",
        "version": "1.0.0"
    }


@app.get("/health", tags=["Health"])
async def health():
    """Detailed health check — verifies DuckDB + MongoDB connectivity"""
    checks = {"duckdb": False, "mongodb": False}
    try:
        duckdb_engine.conn.execute("SELECT 1")
        checks["duckdb"] = True
    except Exception:
        pass
    try:
        metadata_store.db.command("ping")
        checks["mongodb"] = True
    except Exception:
        pass
    status = "healthy" if all(checks.values()) else "degraded"
    return {"status": status, "checks": checks}


@app.get("/datasets", response_model=List[Dict[str, Any]], tags=["Metadata"])
async def list_datasets():
    """
    List all available datasets in the catalog
    
    Returns list of datasets with basic metadata (id, name, row count, etc.)
    """
    try:
        datasets = metadata_store.list_datasets()
        # Convert MongoDB ObjectId to string
        for ds in datasets:
            ds["_id"] = str(ds["_id"])
        return datasets
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list datasets: {str(e)}")


@app.get("/datasets/{dataset_id}", response_model=Dict[str, Any], tags=["Metadata"])
async def get_dataset(dataset_id: str):
    """
    Get detailed metadata for a specific dataset
    
    Args:
        dataset_id: Dataset identifier (e.g., 'gold.fct_order_products')
        
    Returns:
        Full dataset metadata including schema, statistics, lineage
    """
    try:
        dataset = metadata_store.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
        
        # Convert MongoDB ObjectId to string
        dataset["_id"] = str(dataset["_id"])
        return dataset
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dataset: {str(e)}")


@app.get("/contracts/{table}", tags=["Metadata"])
async def get_contract(table: str):
    """
    Get data contract for a table (expectations: not_null, unique, etc.)
    """
    contract = metadata_store.get_contract(table)
    if not contract:
        raise HTTPException(status_code=404, detail=f"No contract for '{table}'")
    contract.pop("_id", None)
    return contract


@app.post("/query", response_model=QueryResponse, tags=["Query"])
async def execute_query(request: QueryRequest):
    """
    Execute SQL query on DuckDB (read-only)
    
    Args:
        request: QueryRequest with SQL string
        
    Returns:
        QueryResponse with columns, rows, execution time
        
    Example:
        POST /query
        {
            "sql": "SELECT * FROM gold.fct_order_products LIMIT 10"
        }
    """
    try:
        # AST-based validation: only SELECT/WITH queries pass.
        # Uses sqlglot to parse the SQL tree — not naive string matching.
        is_valid, reason = validate_sql(request.sql)
        if not is_valid:
            raise HTTPException(
                status_code=400,
                detail=f"Query rejected: {reason}"
            )
        
        # Execute query
        result = duckdb_engine.query(request.sql)
        
        # Record in query history
        metadata_store.record_query(
            request.sql,
            result["execution_time_ms"],
            result["row_count"],
            result["cache_hit"],
        )
        
        return QueryResponse(**result)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")


@app.get("/history", tags=["Query"])
async def query_history(limit: int = 50):
    """
    Get recent query history
    
    Args:
        limit: Maximum number of records to return (default 50)
    """
    return metadata_store.get_query_history(limit)


@app.post("/refresh", tags=["Admin"])
async def refresh_views():
    """
    Re-register Iceberg views after new snapshots are written to S3.
    Call this after dbt run to pick up fresh data.
    """
    try:
        duckdb_engine.refresh_views()
        return {
            "status": "ok",
            "registered_views": duckdb_engine.registered_views,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to refresh views: {str(e)}")


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )


# ========================================
# METRICS STORE ENDPOINTS
# ========================================

@app.post("/metrics", tags=["Metrics"], status_code=201)
async def register_metric(metric_def: Dict[str, Any]):
    """
    Register a new business metric definition in MongoDB
    
    This allows you to define reusable business logic as SQL templates
    that can be executed with parameters at runtime.
    
    **Request Body Example:**
    ```json
    {
        "metric_name": "avg_basket_size_by_hour",
        "display_name": "Average Basket Size by Hour",
        "description": "Average number of products per order by hour of day",
        "sql_template": "SELECT order_hour_of_day, AVG(products_per_order) as avg_basket_size FROM gold.dim_orders GROUP BY 1 ORDER BY 1",
        "materialization": "table",
        "refresh_schedule": "0 6 * * *",
        "tags": ["basket", "hourly", "behavior"],
        "owner": "analytics-team"
    }
    ```
    
    **Parameters:**
    - `metric_name` (required): Unique identifier for the metric
    - `sql_template` (required): SQL query, can include {param} placeholders
    - `display_name` (optional): Human-readable name
    - `description` (optional): What the metric measures
    - `materialization` (optional): 'table' or 'view', default 'table'
    - `refresh_schedule` (optional): Cron expression for scheduled refresh
    - `parameters` (optional): List of parameter definitions
    - `tags` (optional): Tags for categorization
    - `owner` (optional): Team/person responsible
    - `depends_on` (optional): List of upstream dependencies
    """
    try:
        metric_name = metrics_engine.register_metric(metric_def)
        return {
            "status": "registered",
            "metric_name": metric_name,
            "message": f"Metric '{metric_name}' registered successfully"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to register metric: {str(e)}")


@app.get("/metrics", tags=["Metrics"])
async def list_metrics(tags: str = None, owner: str = None):
    """
    List all registered business metrics
    
    **Query Parameters:**
    - `tags`: Comma-separated list of tags to filter by (OR logic)
    - `owner`: Filter by owner
    
    **Example:**
    ```
    GET /metrics?tags=basket,hourly&owner=analytics-team
    ```
    """
    try:
        tag_list = tags.split(",") if tags else None
        metrics = metrics_engine.list_metrics(tags=tag_list, owner=owner)
        return {
            "count": len(metrics),
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list metrics: {str(e)}")


@app.get("/metrics/{metric_name}", tags=["Metrics"])
async def get_metric(metric_name: str):
    """
    Get full metric definition including execution history
    
    **Example:**
    ```
    GET /metrics/avg_basket_size_by_hour
    ```
    
    Returns complete metric definition with:
    - SQL template
    - Parameters
    - Last execution status
    - Execution history
    """
    try:
        metric = metrics_engine.get_metric(metric_name)
        if not metric:
            raise HTTPException(status_code=404, detail=f"Metric '{metric_name}' not found")
        return metric
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get metric: {str(e)}")


@app.post("/metrics/{metric_name}/execute", tags=["Metrics"])
async def execute_metric(metric_name: str, parameters: Dict[str, Any] = None):
    """
    Execute a metric and materialize results in DuckDB
    
    **Path Parameter:**
    - `metric_name`: Metric identifier
    
    **Request Body (optional):**
    ```json
    {
        "min_orders": 100,
        "limit": 20
    }
    ```
    
    Executes the metric's SQL template with provided parameters,
    materializes results as a table/view in DuckDB, and tracks
    execution history in MongoDB.
    
    **Example:**
    ```
    POST /metrics/top_reordered_products/execute
    {
        "min_orders": 100,
        "limit": 20
    }
    ```
    """
    try:
        result = metrics_engine.execute_metric(metric_name, parameters)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")


@app.delete("/metrics/{metric_name}", tags=["Metrics"])
async def delete_metric(metric_name: str):
    """
    Delete a metric definition and its materialized table/view
    
    **Example:**
    ```
    DELETE /metrics/avg_basket_size_by_hour
    ```
    """
    try:
        deleted = metrics_engine.delete_metric(metric_name)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Metric '{metric_name}' not found")
        return {
            "status": "deleted",
            "metric_name": metric_name,
            "message": f"Metric '{metric_name}' deleted successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete metric: {str(e)}")


@app.get("/metrics/{metric_name}/lineage", tags=["Metrics"])
async def get_metric_lineage(metric_name: str):
    """
    Get metric lineage (upstream dependencies and downstream dependents)
    
    **Example:**
    ```
    GET /metrics/revenue_growth/lineage
    ```
    
    Returns:
    ```json
    {
        "metric_name": "revenue_growth",
        "upstream": ["metric.monthly_revenue"],
        "downstream": ["metric.revenue_forecast"]
    }
    ```
    """
    try:
        lineage = metrics_engine.get_lineage(metric_name)
        return lineage
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get lineage: {str(e)}")


@app.post("/metrics/refresh", tags=["Metrics"])
async def refresh_metrics(tags: str = None):
    """
    Refresh all metrics (or filtered by tags)
    
    **Query Parameters:**
    - `tags`: Comma-separated list of tags to filter metrics
    
    **Example:**
    ```
    POST /metrics/refresh?tags=daily,basket
    ```
    
    Executes all matching metrics and returns summary of results.
    """
    try:
        tag_list = tags.split(",") if tags else None
        results = metrics_engine.refresh_all(tags=tag_list)
        
        success_count = sum(1 for r in results if r['status'] == 'success')
        failed_count = len(results) - success_count
        
        return {
            "status": "completed",
            "total": len(results),
            "success": success_count,
            "failed": failed_count,
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to refresh metrics: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

