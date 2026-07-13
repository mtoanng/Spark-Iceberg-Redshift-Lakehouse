{{
  config(
    materialized='table',
    schema='gold',
    tags=['ml', 'features']
  )
}}

/*
Feature engineering for reorder prediction model.
Inspired by archd3sai/Instacart-Market-Basket-Analysis (re-implemented in SQL).

CRITICAL FIXES APPLIED:
1. Removed MODE() function (user_favorite_dow) - not standard in Spark SQL
2. Target labels ONLY from eval_set='train' orders via separate train_labels CTE
3. LEFT JOIN to get NULL for user-products not in training set

Target column behavior:
- target_reordered IS NOT NULL: Training samples (from eval_set='train')
- target_reordered IS NULL: Prediction samples (not in training set)
*/

WITH user_stats AS (
    -- User-level aggregates
    SELECT
        user_id,
        COUNT(DISTINCT order_id) as user_total_orders,
        AVG(days_since_prior_order) as user_avg_days_between_orders,
        AVG(order_hour_of_day) as user_avg_order_hour
        -- NOTE: user_favorite_dow removed (MODE() not standard in Spark SQL)
    FROM {{ ref('dim_orders') }}
    GROUP BY user_id
),

product_stats AS (
    -- Product-level aggregates
    SELECT
        product_id,
        COUNT(DISTINCT order_id) as product_total_orders,
        CAST(SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) AS DOUBLE) / 
            NULLIF(COUNT(*), 0) as product_reorder_rate,
        AVG(cart_sequence) as product_avg_cart_position
    FROM {{ ref('fct_order_products') }}
    GROUP BY product_id
),

user_product_stats AS (
    -- User-product interaction
    SELECT
        user_id,
        product_id,
        COUNT(*) as user_product_order_count,
        SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) as user_product_reorder_count,
        AVG(cart_sequence) as user_product_avg_cart_position,
        MAX(order_number) as user_product_last_order_number
    FROM {{ ref('fct_order_products') }}
    GROUP BY user_id, product_id
),

train_labels AS (
    -- CRITICAL FIX: Extract ONLY training labels from eval_set='train'
    -- Returns NULL for user-products not in training set (not 0!)
    SELECT 
        user_id,
        product_id,
        reordered as target_reordered
    FROM {{ ref('fct_order_products') }}
    WHERE eval_set = 'train'
),

final_features AS (
    SELECT
        up.user_id,
        up.product_id,
        
        -- User features (removed user_favorite_dow)
        us.user_total_orders,
        us.user_avg_days_between_orders,
        us.user_avg_order_hour,
        
        -- Product features
        ps.product_total_orders,
        ps.product_reorder_rate,
        ps.product_avg_cart_position,
        
        -- User-product features
        up.user_product_order_count,
        up.user_product_reorder_count,
        up.user_product_avg_cart_position,
        up.user_product_last_order_number,
        
        -- Derived features
        us.user_total_orders - up.user_product_last_order_number as orders_since_last_purchase,
        CAST(up.user_product_reorder_count AS DOUBLE) / NULLIF(up.user_product_order_count, 0) as user_product_reorder_rate,
        
        -- Target (NULL if not in training set - CRITICAL FIX with LEFT JOIN)
        tl.target_reordered
        
    FROM user_product_stats up
    INNER JOIN user_stats us ON up.user_id = us.user_id
    INNER JOIN product_stats ps ON up.product_id = ps.product_id
    LEFT JOIN train_labels tl 
        ON up.user_id = tl.user_id 
        AND up.product_id = tl.product_id
)

SELECT * FROM final_features
WHERE user_product_order_count > 0  -- Only users who ordered this product before

/*
VERIFICATION NOTES:
- Total rows: All user-product pairs with orders
- Training rows (target_reordered IS NOT NULL): Only pairs in eval_set='train'
- Prediction rows (target_reordered IS NULL): Pairs for recommendation generation

After fix, expect significant reduction in training sample count vs before
(before fix: all rows had target=0 or 1, none NULL - incorrect!)

Feature count: 12 features (removed user_favorite_dow from original 13)
*/
