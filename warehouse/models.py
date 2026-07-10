"""
Pydantic models for warehouse API
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class DatasetMetadata(BaseModel):
    """Metadata for a dataset in the catalog"""
    dataset_id: str = Field(..., description="Unique dataset identifier (e.g., 'gold.dim_user')")
    schema_name: str = Field(..., description="Schema/layer name (e.g., 'gold')")
    table_name: str = Field(..., description="Table name")
    description: Optional[str] = Field(None, description="Dataset description")
    row_count: Optional[int] = Field(None, description="Approximate row count")
    location: str = Field(..., description="S3 location of Iceberg table")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class DatasetSchema(BaseModel):
    """Schema information for a dataset"""
    dataset_id: str
    columns: List[Dict[str, str]] = Field(..., description="List of {name, type, description}")


class QueryRequest(BaseModel):
    """SQL query request"""
    sql: str = Field(..., description="SQL query to execute", min_length=1, max_length=10000)


class QueryResponse(BaseModel):
    """SQL query response"""
    columns: List[str] = Field(..., description="Column names")
    rows: List[List[Any]] = Field(..., description="Result rows")
    row_count: int = Field(..., description="Number of rows returned")
    execution_time_ms: float = Field(..., description="Query execution time in milliseconds")


class ErrorResponse(BaseModel):
    """Error response"""
    error: str
    detail: Optional[str] = None
