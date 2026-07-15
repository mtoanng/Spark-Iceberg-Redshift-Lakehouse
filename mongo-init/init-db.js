// MongoDB initialization script
// Creates the database, collections, indexes, and seeds sample metadata
// for the Instacart Market Basket Analytics Platform.
//
// Per the cutting-back principle: "ghi tay 1 document mau la du chung minh khai niem"
// (write 1 sample document manually to prove the concept). We seed one document
// per gold-layer table to demonstrate the metadata catalog pattern.

db = db.getSiblingDB('instacart_metadata');

// Create collections
db.createCollection('datasets');
db.createCollection('schemas');
db.createCollection('statistics');
db.createCollection('quality_metrics');
db.createCollection('lineage');
db.createCollection('metrics');
db.createCollection('query_history');

// Create indexes
db.datasets.createIndex({ "dataset_id": 1 }, { unique: true });
db.datasets.createIndex({ "schema_name": 1 });
db.datasets.createIndex({ "table_name": 1 });
db.datasets.createIndex({ "updated_at": -1 });

db.metrics.createIndex({ "metric_name": 1 }, { unique: true });
db.metrics.createIndex({ "tags": 1 });
db.metrics.createIndex({ "owner": 1 });
db.metrics.createIndex({ "last_run_status": 1 });

db.query_history.createIndex({ "executed_at": -1 });
db.query_history.createIndex({ "duration_ms": 1 });

// =============================================================
// Seed sample metadata documents (gold layer)
// =============================================================

// --- fct_order_products (core fact table) ---
db.datasets.insertOne({
    dataset_id: "gold.fct_order_products",
    schema_name: "gold",
    table_name: "fct_order_products",
    description: "Grain: 1 row per (order_id, product_id). Source: Instacart Market Basket dataset.",
    owner: "data-team",
    tags: ["market-basket", "core-fact"],
    quality_score: 0.97,
    row_count: 33819106,
    location: "s3://instacart-lakehouse/gold/fct_order_products",
    table_format: "iceberg",
    columns: [
        { name: "order_product_key", type: "string", description: "Surrogate key (order_id + product_id)" },
        { name: "order_id", type: "int", description: "FK to dim_orders" },
        { name: "product_id", type: "int", description: "FK to dim_product" },
        { name: "user_id", type: "int", description: "User identifier" },
        { name: "reordered", type: "int", description: "1 if reordered, 0 otherwise" },
        { name: "order_dow", type: "int", description: "Day of week (0-6)" },
        { name: "order_hour_of_day", type: "int", description: "Hour of day (0-23)" },
        { name: "department_id", type: "int", description: "Department FK" },
        { name: "aisle_id", type: "int", description: "Aisle FK" }
    ],
    last_refresh: new Date("2026-07-01T02:00:00Z"),
    saved_queries: [],
    created_at: new Date(),
    updated_at: new Date()
});

// --- dim_product ---
db.datasets.insertOne({
    dataset_id: "gold.dim_product",
    schema_name: "gold",
    table_name: "dim_product",
    description: "Product dimension with aisle + department hierarchy. SCD Type 1.",
    owner: "data-team",
    tags: ["dimension", "product"],
    quality_score: 1.0,
    row_count: 49688,
    location: "s3://instacart-lakehouse/gold/dim_product",
    table_format: "iceberg",
    columns: [
        { name: "product_key", type: "string", description: "Surrogate key" },
        { name: "product_id", type: "int", description: "Natural key" },
        { name: "product_name", type: "string", description: "Product display name" },
        { name: "department", type: "string", description: "Department name" },
        { name: "aisle", type: "string", description: "Aisle name" }
    ],
    last_refresh: new Date("2026-07-01T02:00:00Z"),
    saved_queries: [],
    created_at: new Date(),
    updated_at: new Date()
});

// --- dim_orders ---
db.datasets.insertOne({
    dataset_id: "gold.dim_orders",
    schema_name: "gold",
    table_name: "dim_orders",
    description: "Order dimension — 1 row per order. SCD Type 1.",
    owner: "data-team",
    tags: ["dimension", "order"],
    quality_score: 1.0,
    row_count: 3421083,
    location: "s3://instacart-lakehouse/gold/dim_orders",
    table_format: "iceberg",
    columns: [
        { name: "order_key", type: "string", description: "Surrogate key" },
        { name: "order_id", type: "int", description: "Natural key" },
        { name: "user_id", type: "int", description: "User identifier" },
        { name: "order_number", type: "int", description: "Sequential order number for user" },
        { name: "order_dow", type: "int", description: "Day of week (0-6)" },
        { name: "order_hour_of_day", type: "int", description: "Hour of day (0-23)" },
        { name: "days_since_prior_order", type: "double", description: "Days since last order (null for first order)" }
    ],
    last_refresh: new Date("2026-07-01T02:00:00Z"),
    saved_queries: [],
    created_at: new Date(),
    updated_at: new Date()
});

// --- mart_product_reorder_rate ---
db.datasets.insertOne({
    dataset_id: "gold.mart_product_reorder_rate",
    schema_name: "gold",
    table_name: "mart_product_reorder_rate",
    description: "Product-level reorder rate. Aggregated from fct_order_products.",
    owner: "data-team",
    tags: ["mart", "analytics"],
    quality_score: 0.95,
    row_count: 49688,
    location: "s3://instacart-lakehouse/gold/mart_product_reorder_rate",
    table_format: "iceberg",
    last_refresh: new Date("2026-07-01T02:00:00Z"),
    saved_queries: [],
    created_at: new Date(),
    updated_at: new Date()
});

// --- mart_department_demand ---
db.datasets.insertOne({
    dataset_id: "gold.mart_department_demand",
    schema_name: "gold",
    table_name: "mart_department_demand",
    description: "Department demand volume by day of week and hour of day.",
    owner: "data-team",
    tags: ["mart", "analytics"],
    quality_score: 0.95,
    row_count: 21,
    location: "s3://instacart-lakehouse/gold/mart_department_demand",
    table_format: "iceberg",
    last_refresh: new Date("2026-07-01T02:00:00Z"),
    saved_queries: [],
    created_at: new Date(),
    updated_at: new Date()
});

// =============================================================
// Seed data contracts (gold layer)
// =============================================================

db.createCollection('data_contracts');
db.data_contracts.createIndex({ "table": 1 }, { unique: true });

db.data_contracts.insertMany([
    {
        table: "gold.fct_order_products",
        expectations: {
            not_null: ["order_id", "product_id"],
            unique: ["order_id", "product_id"]
        },
        created_at: new Date(),
        updated_at: new Date()
    },
    {
        table: "gold.dim_product",
        expectations: {
            not_null: ["product_id"],
            unique: ["product_id"]
        },
        created_at: new Date(),
        updated_at: new Date()
    },
    {
        table: "gold.dim_orders",
        expectations: {
            not_null: ["order_id"],
            unique: ["order_id"]
        },
        created_at: new Date(),
        updated_at: new Date()
    }
]);

print("MongoDB initialized successfully");
print("Collections created: datasets, schemas, statistics, quality_metrics, lineage, metrics, query_history, data_contracts");
print("Indexes created on datasets, metrics, and query_history collections");
print("Sample metadata seeded for 5 gold-layer tables");
print("Data contracts seeded for 3 gold-layer tables");
print("✨ Metrics collection ready for business logic definitions");
