"""MongoDB-backed metadata catalog used by setup scripts."""

from datetime import datetime
from typing import Any, Dict, Optional

from pymongo import MongoClient


class MetadataStore:
    """Small catalog wrapper for dataset metadata documents."""

    def __init__(
        self,
        uri: str = "mongodb://admin:admin123@localhost:27017/",
        database: str = "instacart_warehouse",
    ) -> None:
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
