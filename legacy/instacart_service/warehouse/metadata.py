"""MongoDB Atlas recommendations store - PRODUCTION ONLY.
MongoDB Atlas is ONLY for storing ML recommendations.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from pymongo import MongoClient
import os


class MetadataStore:
    """DEPRECATED: This class is not used in production.
    
    Kept for backward compatibility with old scripts only.
    MongoDB Atlas is ONLY for recommendations, not metadata.
    """

    def __init__(
        self,
        uri: str = None,
        database: str = "instacart_ml_warehouse",
    ) -> None:
        if uri is None:
            uri = os.getenv("MONGODB_URI")
            if not uri:
                raise ValueError(
                    "MONGODB_URI environment variable required. "
                    "Use MongoDB Atlas for production: "
                    "mongodb+srv://username:password@cluster.mongodb.net/"
                )
        
        self._client = MongoClient(uri)
        self._db = self._client[database]
        self._collection = self._db["datasets"]
        self._collection.create_index("dataset_id", unique=True)

    def register_dataset(self, metadata: Dict[str, Any]) -> str:
        dataset_id = metadata["dataset_id"]
        document = {
            **metadata,
            "registered_at": metadata.get("registered_at", datetime.utcnow()),
            "updated_at": metadata.get("updated_at", datetime.utcnow()),
        }
        self._collection.update_one(
            {"dataset_id": dataset_id},
            {"$set": document},
            upsert=True,
        )
        return dataset_id

    def get_dataset(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        return self._collection.find_one({"dataset_id": dataset_id}, {"_id": 0})

    def close(self) -> None:
        self._client.close()
