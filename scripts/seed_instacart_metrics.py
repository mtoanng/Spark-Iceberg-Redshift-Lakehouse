"""
Seed Instacart-specific business metrics into MongoDB

Based on common patterns from Kaggle notebooks and Instacart business domain
"""

import os
import sys
from pymongo import MongoClient
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def seed_instacart_metrics():
    """Seed comprehensive Instacart business metrics"""
    
    # Connect to MongoDB
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://admin:admin123@localhost:27017")
    client = MongoClient(mongodb_uri)
    db = client['instacart_metadata']
    metrics = db['metrics']
    
    print("🛒 Seeding Instacart-specific business metrics...")
    print("=" * 60)
    
    instacart_metrics = [
        # ============ REORDER BEHAVIOR METRICS ============
        {
            "metric_name": "product_reorder_rate",
            "display_name": "Product Reorder Rate",
            "description": "Reorder rate by product with order count filter. High reorder rate = customer loyalty for that product.",
            "owner": "product-team",
            "category": "reorder_behavior",
            "sql_template": """
                SELECT 
                    product_name,
                    department,
                    aisle,
                    total_order_lines,
                    reorder_count,
                    ROUND(reorder_rate * 100, 2) as reorder_percentage
                FROM gold.mart_product_reorder_rate
                WHERE total_order_lines >= {min_orders}
                ORDER BY reorder_rate DESC
                LIMIT {limit}
            """,
            "materialization": "table",
            "refresh_schedule": "0 6 * * *",
            "tags": ["reorder", "product", "kpi", "daily"],
            "parameters": [
                {"name": "min_orders", "type": "integer", "default": 100, "description": "Minimum order count threshold"},
                {"name": "limit", "type": "integer", "default": 50, "description": "Number of products to return"}
            ],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "department_reorder_rate",
            "display_name": "Department Reorder Rate Analysis",
            "description": "Average reorder rate by department. Identifies which departments have strongest customer loyalty.",
            "owner": "category-team",
            "category": "reorder_behavior",
            "sql_template": """
                SELECT 
                    department,
                    COUNT(DISTINCT product_id) as product_count,
                    AVG(reorder_rate) * 100 as avg_reorder_percentage,
                    SUM(total_order_lines) as total_orders
                FROM gold.mart_product_reorder_rate
                GROUP BY department
                ORDER BY avg_reorder_percentage DESC
            """,
            "materialization": "table",
            "refresh_schedule": "0 7 * * *",
            "tags": ["reorder", "department", "kpi", "daily"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "reorder_vs_first_time",
            "display_name": "Reordered vs First-Time Purchase Split",
            "description": "Percentage breakdown of reordered items vs first-time purchases. Key metric for customer retention.",
            "owner": "analytics-team",
            "category": "reorder_behavior",
            "sql_template": """
                SELECT 
                    CASE 
                        WHEN reordered = 1 THEN 'Reordered'
                        ELSE 'First Time'
                    END as purchase_type,
                    COUNT(*) as order_lines,
                    COUNT(DISTINCT order_id) as unique_orders,
                    COUNT(DISTINCT product_id) as unique_products,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                FROM gold.fct_order_products
                GROUP BY reordered
            """,
            "materialization": "table",
            "refresh_schedule": "0 8 * * *",
            "tags": ["reorder", "behavior", "kpi", "daily"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        # ============ BASKET ANALYSIS METRICS ============
        {
            "metric_name": "basket_size_by_hour",
            "display_name": "Average Basket Size by Hour of Day",
            "description": "Average number of products per order by hour. Identifies peak shopping times and basket patterns.",
            "owner": "ops-team",
            "category": "basket_analysis",
            "sql_template": """
                SELECT 
                    order_hour_of_day as hour,
                    ROUND(AVG(products_per_order), 2) as avg_basket_size,
                    COUNT(*) as order_count,
                    ROUND(AVG(products_per_order) * 100.0 / 
                        AVG(AVG(products_per_order)) OVER(), 2) as index_vs_average
                FROM gold.dim_orders
                GROUP BY order_hour_of_day
                ORDER BY order_hour_of_day
            """,
            "materialization": "table",
            "refresh_schedule": "0 6 * * *",
            "tags": ["basket", "hourly", "behavior", "daily"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "basket_size_by_dow",
            "display_name": "Average Basket Size by Day of Week",
            "description": "Basket size patterns across days of week. Shows weekly shopping behavior patterns.",
            "owner": "ops-team",
            "category": "basket_analysis",
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
                    ROUND(AVG(products_per_order), 2) as avg_basket_size,
                    COUNT(*) as order_count
                FROM gold.dim_orders
                GROUP BY order_dow
                ORDER BY order_dow
            """,
            "materialization": "table",
            "refresh_schedule": "0 9 * * 1",
            "tags": ["basket", "dow", "behavior", "weekly"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "basket_size_distribution",
            "display_name": "Basket Size Distribution",
            "description": "Distribution of order sizes (1-5 items, 6-10, 11-20, 21+). Understanding customer shopping patterns.",
            "owner": "analytics-team",
            "category": "basket_analysis",
            "sql_template": """
                SELECT 
                    CASE 
                        WHEN products_per_order BETWEEN 1 AND 5 THEN '1-5 items'
                        WHEN products_per_order BETWEEN 6 AND 10 THEN '6-10 items'
                        WHEN products_per_order BETWEEN 11 AND 20 THEN '11-20 items'
                        WHEN products_per_order >= 21 THEN '21+ items'
                    END as basket_size_range,
                    COUNT(*) as order_count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                FROM gold.dim_orders
                GROUP BY 
                    CASE 
                        WHEN products_per_order BETWEEN 1 AND 5 THEN '1-5 items'
                        WHEN products_per_order BETWEEN 6 AND 10 THEN '6-10 items'
                        WHEN products_per_order BETWEEN 11 AND 20 THEN '11-20 items'
                        WHEN products_per_order >= 21 THEN '21+ items'
                    END
                ORDER BY MIN(products_per_order)
            """,
            "materialization": "table",
            "refresh_schedule": "0 8 * * *",
            "tags": ["basket", "distribution", "analytics", "daily"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        # ============ PRODUCT PERFORMANCE METRICS ============
        {
            "metric_name": "top_products_by_orders",
            "display_name": "Top Products by Order Count",
            "description": "Most frequently ordered products. Core KPI for product popularity.",
            "owner": "product-team",
            "category": "product_performance",
            "sql_template": """
                SELECT 
                    product_name,
                    department,
                    aisle,
                    total_order_lines as order_count,
                    reorder_count,
                    ROUND(reorder_rate * 100, 2) as reorder_percentage
                FROM gold.mart_product_reorder_rate
                ORDER BY total_order_lines DESC
                LIMIT {limit}
            """,
            "materialization": "view",
            "refresh_schedule": "0 */6 * * *",
            "tags": ["product", "top", "kpi", "popular"],
            "parameters": [
                {"name": "limit", "type": "integer", "default": 100, "description": "Number of top products"}
            ],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "products_by_cart_priority",
            "display_name": "Products by Add-to-Cart Priority",
            "description": "Products ordered by average cart position. Lower position = higher priority for customers.",
            "owner": "product-team",
            "category": "product_performance",
            "sql_template": """
                SELECT 
                    p.product_name,
                    p.department,
                    p.aisle,
                    ROUND(AVG(op.add_to_cart_order), 2) as avg_cart_position,
                    COUNT(*) as times_ordered,
                    SUM(op.reordered) as reorder_count
                FROM gold.fct_order_products op
                JOIN gold.dim_product p ON op.product_id = p.product_id
                GROUP BY p.product_name, p.department, p.aisle
                HAVING COUNT(*) >= {min_orders}
                ORDER BY avg_cart_position ASC
                LIMIT {limit}
            """,
            "materialization": "table",
            "refresh_schedule": "0 10 * * *",
            "tags": ["product", "cart", "priority", "behavior"],
            "parameters": [
                {"name": "min_orders", "type": "integer", "default": 100},
                {"name": "limit", "type": "integer", "default": 50}
            ],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        # ============ DEPARTMENT PERFORMANCE METRICS ============
        {
            "metric_name": "department_performance_summary",
            "display_name": "Department Performance Summary",
            "description": "Complete department-level KPIs: orders, customers, demand patterns.",
            "owner": "category-team",
            "category": "department_performance",
            "sql_template": """
                SELECT 
                    department,
                    total_orders,
                    unique_customers,
                    ROUND(avg_items_per_order, 2) as avg_items_per_order,
                    order_line_count,
                    ROUND(order_line_count * 100.0 / SUM(order_line_count) OVER(), 2) as market_share_pct
                FROM gold.mart_department_demand
                ORDER BY total_orders DESC
            """,
            "materialization": "table",
            "refresh_schedule": "0 7 * * *",
            "tags": ["department", "kpi", "performance", "daily"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "department_demand_by_hour",
            "display_name": "Department Demand by Hour",
            "description": "Department order patterns by hour of day. Identifies peak times per category.",
            "owner": "ops-team",
            "category": "department_performance",
            "sql_template": """
                SELECT 
                    department,
                    order_hour_of_day as hour,
                    order_line_count,
                    ROUND(order_line_count * 100.0 / SUM(order_line_count) OVER(PARTITION BY department), 2) as pct_of_dept
                FROM gold.mart_department_demand
                WHERE department = {department}
                ORDER BY order_line_count DESC
                LIMIT {limit}
            """,
            "materialization": "view",
            "refresh_schedule": "0 8 * * *",
            "tags": ["department", "hourly", "demand", "temporal"],
            "parameters": [
                {"name": "department", "type": "string", "default": "'produce'", "description": "Department name"},
                {"name": "limit", "type": "integer", "default": 24}
            ],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        # ============ TEMPORAL ANALYSIS METRICS ============
        {
            "metric_name": "order_volume_by_hour",
            "display_name": "Order Volume by Hour of Day",
            "description": "Total order count by hour. Core metric for capacity planning and staffing.",
            "owner": "ops-team",
            "category": "temporal_analysis",
            "sql_template": """
                SELECT 
                    order_hour_of_day as hour,
                    COUNT(*) as order_count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
                    ROUND(AVG(products_per_order), 2) as avg_basket_size
                FROM gold.dim_orders
                GROUP BY order_hour_of_day
                ORDER BY order_hour_of_day
            """,
            "materialization": "table",
            "refresh_schedule": "0 */6 * * *",
            "tags": ["temporal", "hourly", "volume", "ops"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        {
            "metric_name": "order_volume_by_dow",
            "display_name": "Order Volume by Day of Week",
            "description": "Weekly order patterns. Identifies busy days for capacity planning.",
            "owner": "ops-team",
            "category": "temporal_analysis",
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
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                FROM gold.dim_orders
                GROUP BY order_dow
                ORDER BY order_dow
            """,
            "materialization": "table",
            "refresh_schedule": "0 9 * * 1",
            "tags": ["temporal", "dow", "volume", "ops", "weekly"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        # ============ AISLE PERFORMANCE METRICS ============
        {
            "metric_name": "top_aisles_by_volume",
            "display_name": "Top Aisles by Order Volume",
            "description": "Most popular aisles by order count. Granular category performance.",
            "owner": "category-team",
            "category": "aisle_performance",
            "sql_template": """
                SELECT 
                    p.aisle,
                    p.department,
                    COUNT(DISTINCT op.order_id) as order_count,
                    COUNT(*) as order_line_count,
                    COUNT(DISTINCT op.product_id) as product_count,
                    SUM(op.reordered) as reorder_count,
                    ROUND(SUM(op.reordered) * 100.0 / COUNT(*), 2) as reorder_rate_pct
                FROM gold.fct_order_products op
                JOIN gold.dim_product p ON op.product_id = p.product_id
                GROUP BY p.aisle, p.department
                ORDER BY order_line_count DESC
                LIMIT {limit}
            """,
            "materialization": "table",
            "refresh_schedule": "0 10 * * *",
            "tags": ["aisle", "performance", "volume", "daily"],
            "parameters": [
                {"name": "limit", "type": "integer", "default": 50}
            ],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        # ============ CUSTOMER BEHAVIOR METRICS ============
        {
            "metric_name": "order_size_patterns",
            "display_name": "Order Size Patterns",
            "description": "Statistical summary of order sizes. Min, max, avg, median basket size.",
            "owner": "analytics-team",
            "category": "customer_behavior",
            "sql_template": """
                SELECT 
                    COUNT(*) as total_orders,
                    ROUND(AVG(products_per_order), 2) as avg_basket_size,
                    MIN(products_per_order) as min_basket_size,
                    MAX(products_per_order) as max_basket_size,
                    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY products_per_order), 2) as median_basket_size,
                    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY products_per_order), 2) as p75_basket_size,
                    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY products_per_order), 2) as p90_basket_size
                FROM gold.dim_orders
            """,
            "materialization": "table",
            "refresh_schedule": "0 8 * * *",
            "tags": ["customer", "behavior", "statistics", "daily"],
            "parameters": [],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        },
        
        # ============ COHORT METRICS ============
        {
            "metric_name": "products_reordered_most",
            "display_name": "Products with Highest Reorder Counts",
            "description": "Products that customers reorder most frequently (absolute count). Different from reorder rate.",
            "owner": "product-team",
            "category": "reorder_behavior",
            "sql_template": """
                SELECT 
                    product_name,
                    department,
                    aisle,
                    reorder_count,
                    total_order_lines,
                    ROUND(reorder_rate * 100, 2) as reorder_percentage
                FROM gold.mart_product_reorder_rate
                WHERE reorder_count >= {min_reorders}
                ORDER BY reorder_count DESC
                LIMIT {limit}
            """,
            "materialization": "table",
            "refresh_schedule": "0 6 * * *",
            "tags": ["reorder", "product", "absolute", "daily"],
            "parameters": [
                {"name": "min_reorders", "type": "integer", "default": 100},
                {"name": "limit", "type": "integer", "default": 100}
            ],
            "enabled": True,
            "created_at": datetime.utcnow(),
            "version": "1.0.0",
            "last_run_status": "pending",
            "execution_count": 0
        }
    ]
    
    # Insert metrics
    inserted_count = 0
    updated_count = 0
    
    for metric in instacart_metrics:
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
    print(f"✅ Seeding complete!")
    print(f"  Inserted: {inserted_count}")
    print(f"  Updated:  {updated_count}")
    print(f"  Total:    {len(instacart_metrics)} metrics")
    print(f"{'='*60}\n")
    
    # Summary by category
    print("📊 Metrics by Category:")
    categories = {}
    for metric in metrics.find({}, {"category": 1, "metric_name": 1, "_id": 0}):
        cat = metric.get("category", "uncategorized")
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(metric['metric_name'])
    
    for cat, metric_list in sorted(categories.items()):
        print(f"\n  {cat} ({len(metric_list)} metrics):")
        for m in metric_list:
            print(f"    - {m}")
    
    client.close()


if __name__ == "__main__":
    print("🛒 Instacart Metrics Seeder")
    print("=" * 60)
    print("Based on common patterns from Kaggle notebooks\n")
    
    seed_instacart_metrics()
    
    print("\n✅ All metrics ready!")
    print("\n📡 Available via API:")
    print("   GET  /metrics                    - List all metrics")
    print("   GET  /metrics?category=reorder   - Filter by category")
    print("   POST /metrics/{name}/execute     - Execute metric")
    print()
