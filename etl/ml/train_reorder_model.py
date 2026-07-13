"""
Train XGBoost Reorder Prediction Model

Reads features from mart_user_product_features (dbt Gold layer),
trains XGBoost classifier, and saves model + metrics.

Target: Will user reorder this product in their next order?

Author: Data Engineering Team
Date: 2026-07-13
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score, f1_score, precision_score, recall_score,
    classification_report, confusion_matrix
)
import json
from datetime import datetime

# Add warehouse to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / 'warehouse'))

from engine.duckdb_engine import DuckDBEngine


def load_training_data(engine: DuckDBEngine) -> pd.DataFrame:
    """
    Load training data from mart_user_product_features
    
    Filter: target_reordered IS NOT NULL (training samples only)
    """
    print("\n" + "=" * 80)
    print("📥 LOADING TRAINING DATA")
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
        user_product_reorder_rate,
        target_reordered
    FROM mart_user_product_features
    WHERE target_reordered IS NOT NULL  -- Training samples only
    """
    
    print(f"🔍 Query: {query.strip()[:100]}...")
    
    try:
        df = engine.execute_to_df(query)
        print(f"✅ Loaded {len(df):,} training samples")
        print(f"📊 Features: {df.shape[1] - 3} (excl user_id, product_id, target)")
        print(f"📊 Target distribution:")
        print(df['target_reordered'].value_counts())
        print(f"📊 Positive rate: {df['target_reordered'].mean():.2%}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        raise


def prepare_features(df: pd.DataFrame) -> tuple:
    """
    Prepare features for training
    
    Returns:
        (X_train, X_test, y_train, y_test, feature_names)
    """
    print("\n" + "=" * 80)
    print("🔧 PREPARING FEATURES")
    print("=" * 80)
    
    # Separate features and target
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
    y = df['target_reordered'].copy()
    
    # Handle NaN values (should be minimal after dbt transformations)
    nan_counts = X.isnull().sum()
    if nan_counts.sum() > 0:
        print("⚠️  Found NaN values:")
        print(nan_counts[nan_counts > 0])
        print("→ Filling with 0")
        X = X.fillna(0)
    
    # Split train/test (80/20)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y  # Maintain target distribution
    )
    
    print(f"✅ Train set: {len(X_train):,} samples ({y_train.mean():.2%} positive)")
    print(f"✅ Test set:  {len(X_test):,} samples ({y_test.mean():.2%} positive)")
    print(f"✅ Features: {len(feature_cols)}")
    
    return X_train, X_test, y_train, y_test, feature_cols


def train_model(X_train, y_train, X_test, y_test) -> tuple:
    """
    Train XGBoost classifier
    
    Returns:
        (model, metrics_dict)
    """
    print("\n" + "=" * 80)
    print("🤖 TRAINING XGBOOST MODEL")
    print("=" * 80)
    
    # Calculate scale_pos_weight for imbalanced classes
    pos_rate = y_train.mean()
    scale_pos_weight = (1 - pos_rate) / pos_rate
    
    print(f"⚖️  Class imbalance: {pos_rate:.2%} positive")
    print(f"⚖️  scale_pos_weight: {scale_pos_weight:.2f}")
    
    # XGBoost parameters
    params = {
        'objective': 'binary:logistic',
        'eval_metric': 'auc',
        'max_depth': 6,
        'eta': 0.1,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'scale_pos_weight': scale_pos_weight,
        'seed': 42,
        'verbosity': 1
    }
    
    print(f"🔧 Parameters: {params}")
    
    # Convert to DMatrix
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)
    
    # Train with early stopping
    evals = [(dtrain, 'train'), (dtest, 'test')]
    
    print("\n⏳ Training model...")
    model = xgb.train(
        params,
        dtrain,
        num_boost_round=200,
        evals=evals,
        early_stopping_rounds=20,
        verbose_eval=20
    )
    
    print(f"\n✅ Training complete!")
    print(f"📊 Best iteration: {model.best_iteration}")
    print(f"📊 Best AUC: {model.best_score:.4f}")
    
    # Predict on test set
    y_pred_proba = model.predict(dtest)
    y_pred = (y_pred_proba >= 0.5).astype(int)
    
    # Calculate metrics
    metrics = {
        'auc': roc_auc_score(y_test, y_pred_proba),
        'f1': f1_score(y_test, y_pred),
        'precision': precision_score(y_test, y_pred),
        'recall': recall_score(y_test, y_pred),
        'best_iteration': int(model.best_iteration),
        'train_samples': len(X_train),
        'test_samples': len(X_test),
        'trained_at': datetime.now().isoformat()
    }
    
    print("\n" + "=" * 80)
    print("📊 MODEL PERFORMANCE")
    print("=" * 80)
    print(f"AUC:       {metrics['auc']:.4f}")
    print(f"F1 Score:  {metrics['f1']:.4f}")
    print(f"Precision: {metrics['precision']:.4f}")
    print(f"Recall:    {metrics['recall']:.4f}")
    
    print("\n📊 Classification Report:")
    print(classification_report(y_test, y_pred, target_names=['Not Reorder', 'Reorder']))
    
    print("\n📊 Confusion Matrix:")
    cm = confusion_matrix(y_test, y_pred)
    print(f"              Predicted")
    print(f"              0        1")
    print(f"Actual 0   {cm[0,0]:7d}  {cm[0,1]:7d}")
    print(f"       1   {cm[1,0]:7d}  {cm[1,1]:7d}")
    
    return model, metrics


def save_model(model, metrics: dict, feature_names: list):
    """
    Save model and metadata
    """
    print("\n" + "=" * 80)
    print("💾 SAVING MODEL")
    print("=" * 80)
    
    # Create artifacts directory
    artifacts_dir = Path(__file__).parent / 'model_artifacts'
    artifacts_dir.mkdir(exist_ok=True)
    
    # Save model
    model_path = artifacts_dir / 'reorder_model.xgb'
    model.save_model(str(model_path))
    print(f"✅ Model saved: {model_path}")
    
    # Save metadata
    metadata = {
        'model_version': 'xgboost_v1',
        'model_type': 'XGBoost Binary Classifier',
        'target': 'reorder_prediction',
        'features': feature_names,
        'metrics': metrics,
        'model_path': str(model_path)
    }
    
    metadata_path = artifacts_dir / 'model_metadata.json'
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"✅ Metadata saved: {metadata_path}")
    
    # Save feature importance
    importance = model.get_score(importance_type='gain')
    importance_sorted = sorted(importance.items(), key=lambda x: x[1], reverse=True)
    
    print("\n📊 Feature Importance (top 10):")
    for i, (feature, score) in enumerate(importance_sorted[:10], 1):
        print(f"  {i:2d}. {feature:35s} {score:10.2f}")
    
    importance_path = artifacts_dir / 'feature_importance.json'
    with open(importance_path, 'w') as f:
        json.dump(dict(importance_sorted), f, indent=2)
    print(f"✅ Feature importance saved: {importance_path}")


def main():
    """Main execution"""
    print("\n" + "=" * 80)
    print("🚀 INSTACART REORDER PREDICTION MODEL TRAINING")
    print("=" * 80)
    print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")
    
    try:
        # Initialize DuckDB engine
        print("🔧 Initializing DuckDB engine...")
        engine = DuckDBEngine(
            db_path=os.getenv('DUCKDB_PATH', 'warehouse/data/warehouse.db'),
            use_glue_catalog=os.getenv('USE_GLUE_CATALOG', 'true').lower() == 'true',
            account_id=os.getenv('AWS_ACCOUNT_ID'),
            role_arn=os.getenv('DUCKDB_ROLE_ARN'),
            region=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        # Load data
        df = load_training_data(engine)
        
        # Prepare features
        X_train, X_test, y_train, y_test, feature_names = prepare_features(df)
        
        # Train model
        model, metrics = train_model(X_train, y_train, X_test, y_test)
        
        # Save model
        save_model(model, metrics, feature_names)
        
        # Success
        print("\n" + "=" * 80)
        print("✅ MODEL TRAINING COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"📊 Final AUC: {metrics['auc']:.4f}")
        print(f"📊 Final F1:  {metrics['f1']:.4f}")
        print(f"⏱️  End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Cleanup
        engine.close()
        
        return 0
        
    except Exception as e:
        print(f"\n❌ TRAINING FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
