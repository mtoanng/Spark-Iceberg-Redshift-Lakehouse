"""
MongoDB Recommendation Store - Internal client

INTERNAL ONLY - not exposed outside warehouse plane.
Only warehouse/api/main.py should import this.

Pattern: API Gateway
- MongoDB hidden behind FastAPI
- No direct MongoDB port exposed in docker-compose
- All access through warehouse API

Document Schema:
{
    "user_id": 12345,
    "products": [
        {"product_id": 101, "product_name": "Banana", "score": 0.92},
        {"product_id": 202, "product_name": "Organic Milk", "score": 0.87},
        ...
    ],
    "model_version": "xgboost_v1",
    "generated_at": ISODate("2026-07-13T10:30:00Z")
}

Author: Data Engineering Team
Date: 2026-07-13
"""

from pymongo import MongoClient
from typing import Optional, Dict, List
from datetime import datetime
import os


class RecommendationStore:
    """
    MongoDB-backed recommendation store
    
    Features:
    - Store top-N product recommendations per user
    - Pre-computed by ML model (XGBoost reorder prediction)
    - Read-only from API perspective (writes happen in ETL)
    - Indexed by user_id for fast lookup
    """
    
    def __init__(
        self,
        mongo_uri: Optional[str] = None,
        database: str = "instacart_warehouse"
    ):
        """
        Initialize MongoDB connection
        
        Args:
            mongo_uri: MongoDB connection string (reads from env if not provided)
            database: Database name (default: instacart_warehouse)
        """
        if mongo_uri is None:
            mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        
        self._client = MongoClient(mongo_uri)
        self._db = self._client[database]
        self._collection = self._db['recommendations']
        
        # Create index on user_id for fast lookups
        self._collection.create_index('user_id', unique=True)
        
        print(f"✅ Connected to MongoDB: {database}.recommendations")
    
    def get_recommendations(self, user_id: int) -> Optional[Dict]:
        """
        Get recommendations for a user
        
        Args:
            user_id: User ID to lookup
            
        Returns:
            Dict with user_id, products list, model_version, generated_at
            None if user not found
        """
        doc = self._collection.find_one(
            {'user_id': user_id},
            {'_id': 0}  # Exclude MongoDB internal ObjectId
        )
        return doc
    
    def upsert_recommendations(
        self,
        user_id: int,
        products: List[Dict],
        model_version: str = "xgboost_v1"
    ) -> None:
        """
        Insert or update recommendations for a user
        
        Args:
            user_id: User ID
            products: List of dicts with product_id, product_name, score
            model_version: Model version string
            
        Example:
            store.upsert_recommendations(
                user_id=12345,
                products=[
                    {"product_id": 101, "product_name": "Banana", "score": 0.92},
                    {"product_id": 202, "product_name": "Milk", "score": 0.87}
                ],
                model_version="xgboost_v1"
            )
        """
        self._collection.update_one(
            {'user_id': user_id},
            {
                '$set': {
                    'products': products,
                    'model_version': model_version,
                    'generated_at': datetime.utcnow()
                }
            },
            upsert=True
        )
    
    def count_users(self) -> int:
        """Count total users with recommendations"""
        return self._collection.count_documents({})
    
    def get_stats(self) -> Dict[str, Any]:
        """Get recommendation store statistics"""
        total_users = self.count_users()
        
        # Sample one document to get model version
        sample = self._collection.find_one({}, {'_id': 0, 'model_version': 1, 'generated_at': 1})
        
        return {
            "total_users": total_users,
            "model_version": sample.get("model_version") if sample else None,
            "last_generated": sample.get("generated_at") if sample else None
        }
    
    def delete_all(self) -> int:
        """
        Delete all recommendations (use with caution!)
        
        Returns:
            Number of documents deleted
        """
        result = self._collection.delete_many({})
        return result.deleted_count
    
    def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            print("🔌 MongoDB connection closed")


if __name__ == "__main__":
    """Quick self-test"""
    
    print("\n" + "=" * 60)
    print("🧪 Recommendation Store Self-Test")
    print("=" * 60 + "\n")
    
    # Use test database
    store = RecommendationStore(
        mongo_uri="mongodb://localhost:27017",
        database="test_warehouse"
    )
    
    try:
        # Test upsert
        test_products = [
            {"product_id": 101, "product_name": "Test Banana", "score": 0.95},
            {"product_id": 202, "product_name": "Test Milk", "score": 0.87}
        ]
        
        store.upsert_recommendations(
            user_id=99999,
            products=test_products,
            model_version="test_v1"
        )
        print("✅ Upserted test recommendation")
        
        # Test retrieval
        rec = store.get_recommendations(99999)
        assert rec is not None
        assert rec['user_id'] == 99999
        assert len(rec['products']) == 2
        print(f"✅ Retrieved recommendation: {rec['user_id']}")
        
        # Test stats
        stats = store.get_stats()
        print(f"✅ Stats: {stats}")
        
        # Cleanup
        deleted = store.delete_all()
        print(f"✅ Cleanup: deleted {deleted} documents")
        
        print("\n✅ Self-test passed!")
        
    except Exception as e:
        print(f"\n❌ Self-test failed: {e}")
        
    finally:
        store.close()
