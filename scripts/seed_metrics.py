"""
Seed example business metrics into MongoDB

This replaces creating YAML files - metrics stored as data in MongoDB
"""

import os
import sys
from pymongo import MongoClient
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def seed_metrics():
    """Seed example metrics into MongoDB"""
    
    # Connect to MongoDB
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://admin:admin123@localhost:27017")
    client = MongoClient(mongodb_uri)
    db = client['instacart_metadata']
    metrics = db['metrics']
    
    # Clear existing metrics (optional - comment out to preserve)
    # metrics.delete_many({})
    
    example_metrics = [
        {
            "metric_name": "avg_basket_size_by_hour",
            "display_name": "Average Basket Size by Hour",
            "description": "Average number of products per order by hour of day. Helps identify peak ordering patterns.",
            "owner": "analytics-team",
            "sql_template": """
                SELECT 
                    order_hour_of_day,
                    ROUND(AVG(products_per_order), 2) as avg_basket_size,
                    COUNT(*) as order_count
                FROM gold.dim_orders
                GROUP BY order_hour_of_day
                ORDER BY order_hour_of_day
            """,
            "materialization": "table",
            "refresh_schedule": "0 6 * * *",  # Daily at 6am
            "tags": ["basket", "hourly", "behavior"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "top_reordered_products",
            "display_name": "Top Reordered Products",
            "description": "Products with highest reorder rates (parameterized by minimum order count and result limit)",
            "owner": "product-team",
            "sql_template": """
                SELECT 
                    product_name,
                    department,
                    ROUND(reorder_rate * 100, 2) as reorder_percentage,
                    total_order_lines
                FROM gold.mart_product_reorder_rate
                WHERE total_order_lines >= {min_orders}
                ORDER BY reorder_rate DESC
                LIMIT {limit}
            """,
            "materialization": "view",  # View for dynamic refresh
            "refresh_schedule": "0 */4 * * *",  # Every 4 hours
            "tags": ["reorder", "products", "top", "parameterized"],
            "parameters": [
                {
                    "name": "min_orders",
                    "type": "integer",
                    "default": 100,
                    "description": "Minimum order count threshold"
                },
                {
                    "name": "limit",
                    "type": "integer",
                    "default": 20,
                    "description": "Number of top products to return"
                }
            ],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "department_demand_summary",
            "display_name": "Department Demand Summary",
            "description": "Total orders and unique customers by department",
            "owner": "analytics-team",
            "sql_template": """
                SELECT 
                    department,
                    total_orders,
                    unique_customers,
                    ROUND(avg_items_per_order, 2) as avg_items_per_order
                FROM gold.mart_department_demand
                ORDER BY total_orders DESC
            """,
            "materialization": "table",
            "refresh_schedule": "0 8 * * *",  # Daily at 8am
            "tags": ["department", "demand", "summary"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "reorder_vs_new_orders",
            "display_name": "Reorder vs New Purchase Analysis",
            "description": "Compare reordered products vs first-time purchases in order patterns",
            "owner": "product-team",
            "sql_template": """
                SELECT 
                    CASE 
                        WHEN reordered = 1 THEN 'Reorder'
                        ELSE 'New Purchase'
                    END as order_type,
                    COUNT(*) as order_lines,
                    COUNT(DISTINCT order_id) as orders,
                    COUNT(DISTINCT product_id) as unique_products
                FROM gold.fct_order_products
                GROUP BY 
                    CASE 
                        WHEN reordered = 1 THEN 'Reorder'
                        ELSE 'New Purchase'
                    END
            """,
            "materialization": "table",
            "refresh_schedule": "0 10 * * *",  # Daily at 10am
            "tags": ["reorder", "behavior", "comparison"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "products_by_add_to_cart_order",
            "display_name": "Product Priority in Cart",
            "description": "Average position products are added to cart (lower = higher priority)",
            "owner": "product-team",
            "sql_template": """
                SELECT 
                    p.product_name,
                    p.department,
                    ROUND(AVG(op.add_to_cart_order), 2) as avg_cart_position,
                    COUNT(*) as times_ordered
                FROM gold.fct_order_products op
                JOIN gold.dim_product p ON op.product_id = p.product_id
                GROUP BY p.product_name, p.department
                HAVING COUNT(*) >= {min_times_ordered}
                ORDER BY avg_cart_position ASC
                LIMIT {limit}
            """,
            "materialization": "table",
            "refresh_schedule": "0 12 * * *",  # Daily at noon
            "tags": ["cart", "priority", "behavior", "parameterized"],
            "parameters": [
                {
                    "name": "min_times_ordered",
                    "type": "integer",
                    "default": 50,
                    "description": "Minimum order frequency threshold"
                },
                {
                    "name": "limit",
                    "type": "integer",
                    "default": 30,
                    "description": "Number of products to return"
                }
            ],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "order_dow_distribution",
            "display_name": "Order Day of Week Distribution",
            "description": "Order volume by day of week (0=Sunday, 6=Saturday)",
            "owner": "analytics-team",
            "sql_template": """
                SELECT 
                    order_dow,
                    CASE order_dow
                        WHEN 0 THEN 'Sunday'
                        WHEN 1 THEN 'Monday'
                        WHEN 2 THEN 'Tuesday'
                        WHEN 3 THEN 'Wednesday'
                        WHEN 4 THEN 'Thursday'
                        WHEN 5 THEN 'Friday'
                        WHEN 6 THEN 'Saturday'
                    END as day_name,
                    COUNT(*) as order_count,
                    ROUND(AVG(products_per_order), 2) as avg_basket_size
                FROM gold.dim_orders
                GROUP BY order_dow
                ORDER BY order_dow
            """,
            "materialization": "table",
            "refresh_schedule": "0 7 * * 1",  # Weekly on Monday at 7am
            "tags": ["temporal", "dow", "distribution"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        }
    ]
    
    # Insert metrics (update if exists)
    inserted_count = 0
    updated_count = 0
    
    for metric in example_metrics:
        result = metrics.update_one(
            {"metric_name": metric["metric_name"]},
            {"$set": metric},
            upsert=True
        )
        
        if result.upserted_id:
            inserted_count += 1
            print(f"✨ Created: {metric['metric_name']}")
        else:
            updated_count += 1
            print(f"✅ Updated: {metric['metric_name']}")
    
    print(f"\n{'='*60}")
    print(f"Seeding complete!")
    print(f"  Inserted: {inserted_count}")
    print(f"  Updated:  {updated_count}")
    print(f"  Total:    {len(example_metrics)}")
    print(f"{'='*60}\n")
    
    # Show summary
    print("Available metrics:")
    for metric in metrics.find({}, {"metric_name": 1, "display_name": 1, "tags": 1, "_id": 0}):
        tags = ", ".join(metric.get("tags", []))
        print(f"  - {metric['metric_name']}: {metric.get('display_name', 'N/A')} [{tags}]")
    
    client.close()


if __name__ == "__main__":
    print("🌱 Seeding business metrics into MongoDB...")
    print("This replaces creating YAML configuration files\n")
    
    seed_metrics()
    
    print("\n✅ Done! Metrics are now available via API:")
    print("   GET  /metrics                    - List all metrics")
    print("   GET  /metrics/{name}             - Get metric details")
    print("   POST /metrics/{name}/execute     - Execute metric")
    print("   POST /metrics/register           - Create new metric")
    print("   PUT  /metrics/{name}             - Update metric")
