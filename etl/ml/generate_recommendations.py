"""
Generate Product Recommendations and Write to MongoDB

Loads trained model, generates reorder probabilities for all user-product pairs,
and writes top-N recommendations per user to MongoDB Recommendation Store.

Author: Data Engineering Team
Date: 2026-07-13
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import xgboost as xgb
from datetime import datetime
from tqdm import tqdm

# Add warehouse to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / 'warehouse'))

from engine.duckdb_engine import DuckDBEngine
from recommendation_store import RecommendationStore


# Configuration
TOP_N = 10  # Number of recommendations per user
BATCH_SIZE = 10000  # Process users in batches
MODEL_VERSION = "xgboost_v1"


def load_model():
    """Load trained XGBoost model"""
    print("\n" + "=" * 80)
    print("📦 LOADING TRAINED MODEL")
    print("=" * 80)
    
    model_path = Path(__file__).parent / 'model_artifacts' / 'reorder_model.xgb'
    
    if not model_path.exists():
        raise FileNotFoundError(
            f"Model not found: {model_path}\n"
            "Run train_reorder_model.py first!"
        )
    
    print(f"📂 Loading from: {model_path}")
    model = xgb.Booster()
    model.load_model(str(model_path))
    
    print(f"✅ Model loaded successfully")
    print(f"📊 Model version: {MODEL_VERSION}")
    
    return model


def load_prediction_data(engine: DuckDBEngine) -> pd.DataFrame:
    """
    Load ALL user-product pairs from mart_user_product_features
    
    Includes both training samples (target != NULL) and prediction samples (target IS NULL)
    """
    print("\n" + "=" * 80)
    print("📥 LOADING PREDICTION DATA")
    print("=" * 80)
    
    query = """
    SELECT 
        user_id,
        product_id,
        user_total_orders,
        user_avg_days_between_orders,
        user_avg_order_hour,
        product_total_orders,
        product_reorder_rate,
        product_avg_cart_position,
        user_product_order_count,
        user_product_reorder_count,
        user_product_avg_cart_position,
        user_product_last_order_number,
        orders_since_last_purchase,
        user_product_reorder_rate
    FROM mart_user_product_features
    """
    
    print(f"🔍 Query: {query.strip()[:100]}...")
    
    try:
        df = engine.execute_to_df(query)
        print(f"✅ Loaded {len(df):,} user-product pairs")
        print(f"📊 Unique users: {df['user_id'].nunique():,}")
        print(f"📊 Unique products: {df['product_id'].nunique():,}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        raise


def get_product_names(engine: DuckDBEngine, product_ids: list) -> dict:
    """
    Get product names from dim_product
    
    Returns: {product_id: product_name}
    """
    print("\n📋 Loading product names...")
    
    # Convert to comma-separated string for IN clause
    product_ids_str = ','.join(map(str, product_ids))
    
    query = f"""
    SELECT 
        product_id,
        product_name
    FROM dim_product
    WHERE product_id IN ({product_ids_str})
    """
    
    try:
        df = engine.execute_to_df(query)
        product_map = dict(zip(df['product_id'], df['product_name']))
        print(f"✅ Loaded {len(product_map):,} product names")
        return product_map
        
    except Exception as e:
        print(f"⚠️  Error loading product names: {e}")
        print("→ Will use product_id as fallback")
        return {pid: f"Product {pid}" for pid in product_ids}


def generate_predictions(model, df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate reorder probability predictions
    
    Returns: DataFrame with user_id, product_id, score
    """
    print("\n" + "=" * 80)
    print("🔮 GENERATING PREDICTIONS")
    print("=" * 80)
    
    feature_cols = [
        'user_total_orders',
        'user_avg_days_between_orders',
        'user_avg_order_hour',
        'product_total_orders',
        'product_reorder_rate',
        'product_avg_cart_position',
        'user_product_order_count',
        'user_product_reorder_count',
        'user_product_avg_cart_position',
        'user_product_last_order_number',
        'orders_since_last_purchase',
        'user_product_reorder_rate'
    ]
    
    X = df[feature_cols].copy()
    
    # Handle NaN values
    X = X.fillna(0)
    
    # Convert to DMatrix
    dmatrix = xgb.DMatrix(X)
    
    # Predict probabilities
    print(f"⏳ Predicting for {len(df):,} pairs...")
    scores = model.predict(dmatrix)
    
    # Create result DataFrame
    result = pd.DataFrame({
        'user_id': df['user_id'],
        'product_id': df['product_id'],
        'score': scores
    })
    
    print(f"✅ Predictions complete")
    print(f"📊 Score statistics:")
    print(f"   Mean:   {scores.mean():.4f}")
    print(f"   Median: {np.median(scores):.4f}")
    print(f"   Min:    {scores.min():.4f}")
    print(f"   Max:    {scores.max():.4f}")
    
    return result


def create_recommendations(predictions: pd.DataFrame, product_names: dict) -> list:
    """
    Create top-N recommendations per user
    
    Returns: List of recommendation documents for MongoDB
    """
    print("\n" + "=" * 80)
    print(f"🎯 CREATING TOP-{TOP_N} RECOMMENDATIONS")
    print("=" * 80)
    
    recommendations = []
    generated_at = datetime.utcnow()
    
    # Group by user and get top-N
    print(f"⏳ Processing users...")
    
    user_groups = predictions.groupby('user_id')
    total_users = len(user_groups)
    
    for user_id, group in tqdm(user_groups, desc="Users", total=total_users):
        # Sort by score descending and take top-N
        top_products = group.nlargest(TOP_N, 'score')
        
        # Build product list
        products = []
        for _, row in top_products.iterrows():
            product_id = int(row['product_id'])
            products.append({
                'product_id': product_id,
                'product_name': product_names.get(product_id, f"Product {product_id}"),
                'score': float(row['score'])
            })
        
        # Create recommendation document
        rec_doc = {
            'user_id': int(user_id),
            'products': products,
            'model_version': MODEL_VERSION,
            'generated_at': generated_at
        }
        
        recommendations.append(rec_doc)
    
    print(f"\n✅ Created recommendations for {len(recommendations):,} users")
    print(f"📊 Average recommendations per user: {np.mean([len(r['products']) for r in recommendations]):.1f}")
    
    return recommendations


def write_to_mongodb(recommendations: list, rec_store: RecommendationStore):
    """
    Write recommendations to MongoDB
    
    Replaces existing recommendations (upsert by user_id)
    """
    print("\n" + "=" * 80)
    print("💾 WRITING TO MONGODB")
    print("=" * 80)
    
    print(f"📊 Writing {len(recommendations):,} recommendation documents...")
    
    try:
        # Write in batches
        batch_size = 1000
        total_batches = (len(recommendations) + batch_size - 1) // batch_size
        
        for i in tqdm(range(0, len(recommendations), batch_size), 
                     desc="Batches", total=total_batches):
            batch = recommendations[i:i + batch_size]
            rec_store.bulk_upsert_recommendations(batch)
        
        print(f"\n✅ Successfully wrote all recommendations")
        
        # Verify
        stats = rec_store.get_stats()
        print(f"\n📊 MongoDB Statistics:")
        print(f"   Total users:     {stats.get('total_users', 0):,}")
        print(f"   Model version:   {stats.get('model_version', 'N/A')}")
        print(f"   Last generated:  {stats.get('last_generated', 'N/A')}")
        
    except Exception as e:
        print(f"❌ Error writing to MongoDB: {e}")
        raise


def main():
    """Main execution"""
    print("\n" + "=" * 80)
    print("🚀 INSTACART RECOMMENDATION GENERATION")
    print("=" * 80)
    print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Top-N: {TOP_N}")
    print("=" * 80 + "\n")
    
    try:
        # Load model
        model = load_model()
        
        # Initialize DuckDB engine
        print("\n🔧 Initializing DuckDB engine...")
        engine = DuckDBEngine(
            db_path=os.getenv('DUCKDB_PATH', 'warehouse/data/warehouse.db'),
            use_glue_catalog=os.getenv('USE_GLUE_CATALOG', 'true').lower() == 'true',
            account_id=os.getenv('AWS_ACCOUNT_ID'),
            role_arn=os.getenv('DUCKDB_ROLE_ARN'),
            region=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        # Initialize MongoDB client
        print("\n🔧 Initializing MongoDB client...")
        rec_store = RecommendationStore(
            mongo_uri=os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017'),
            database='instacart_warehouse'
        )
        
        # Load prediction data
        df = load_prediction_data(engine)
        
        # Get product names
        unique_products = df['product_id'].unique().tolist()
        product_names = get_product_names(engine, unique_products)
        
        # Generate predictions
        predictions = generate_predictions(model, df)
        
        # Create recommendations
        recommendations = create_recommendations(predictions, product_names)
        
        # Write to MongoDB
        write_to_mongodb(recommendations, rec_store)
        
        # Success
        print("\n" + "=" * 80)
        print("✅ RECOMMENDATION GENERATION COMPLETED")
        print("=" * 80)
        print(f"📊 Total users with recommendations: {len(recommendations):,}")
        print(f"📊 Total recommendations: {len(recommendations) * TOP_N:,}")
        print(f"⏱️  End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Sample recommendation
        if recommendations:
            sample = recommendations[0]
            print(f"\n📋 Sample Recommendation (User {sample['user_id']}):")
            for i, prod in enumerate(sample['products'][:5], 1):
                print(f"   {i}. {prod['product_name'][:40]:40s} (score: {prod['score']:.4f})")
        
        # Cleanup
        engine.close()
        rec_store.close()
        
        return 0
        
    except Exception as e:
        print(f"\n❌ GENERATION FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
