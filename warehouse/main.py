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
from .sql_validator import validate_sql

# Initialize FastAPI app
app = FastAPI(
    title="Instacart Data Warehouse API",
    description="Simple SQL query API for Iceberg Gold layer",
    version="1.0.0"
)

# Initialize engines (singleton pattern)
duckdb_engine = None
metadata_store = None


@app.on_event("startup")
async def startup_event():
    """Initialize engines on startup"""
    global duckdb_engine, metadata_store
    
    duckdb_engine = DuckDBEngine(
        iceberg_path=os.getenv("S3_GOLD_PATH", "s3://instacart-lakehouse/gold")
    )
    metadata_store = MetadataStore(
        uri=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        database=os.getenv("MONGODB_DATABASE", "instacart_metadata")
    )


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


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
