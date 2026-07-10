// MongoDB initialization script
// This creates the database and collections on first start

db = db.getSiblingDB('instacart_metadata');

// Create collections
db.createCollection('datasets');
db.createCollection('schemas');
db.createCollection('statistics');
db.createCollection('quality_metrics');
db.createCollection('lineage');

// Create indexes
db.datasets.createIndex({ "dataset_id": 1 }, { unique: true });
db.datasets.createIndex({ "schema_name": 1 });
db.datasets.createIndex({ "table_name": 1 });
db.datasets.createIndex({ "updated_at": -1 });

// Insert sample metadata (optional)
db.datasets.insertOne({
    dataset_id: "gold.example",
    schema_name: "gold",
    table_name: "example",
    description: "Example dataset for testing",
    row_count: 0,
    location: "s3://bucket/gold/example",
    table_format: "iceberg",
    created_at: new Date(),
    updated_at: new Date()
});

print("✅ MongoDB initialized successfully");
print("✅ Collections created: datasets, schemas, statistics, quality_metrics, lineage");
print("✅ Indexes created");
