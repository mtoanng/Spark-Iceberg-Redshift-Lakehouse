"""
MongoDB metadata catalog client
"""

from pymongo import MongoClient
from typing import List, Optional, Dict, Any
from datetime import datetime
import os


class MetadataStore:
    """MongoDB-based metadata catalog"""
    
    def __init__(self, uri: str = None, database: str = "instacart_metadata"):
        """Initialize MongoDB connection"""
        self.uri = uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.database_name = database
        self.client = MongoClient(self.uri)
        self.db = self.client[database]
        self.datasets_collection = self.db["datasets"]
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """List all datasets in the catalog"""
        return list(self.datasets_collection.find(
            {},
            {"_id": 1, "dataset_id": 1, "table_name": 1, "row_count": 1, "updated_at": 1}
        ))
    
    def get_dataset(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific dataset"""
        return self.datasets_collection.find_one({"dataset_id": dataset_id})
    
    def register_dataset(self, metadata: Dict[str, Any]) -> str:
        """Register or update dataset metadata"""
        metadata["updated_at"] = datetime.utcnow()
        if "created_at" not in metadata:
            metadata["created_at"] = datetime.utcnow()
        
        result = self.datasets_collection.update_one(
            {"dataset_id": metadata["dataset_id"]},
            {"$set": metadata},
            upsert=True
        )
        return metadata["dataset_id"]
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()
