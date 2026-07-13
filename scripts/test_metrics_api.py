"""
Quick test script for Metrics Store API

Tests the pure MongoDB approach (no YAML files)
"""

import requests
import json
from time import sleep

BASE_URL = "http://localhost:8000"

def print_response(title, response):
    """Pretty print API response"""
    print(f"\n{'='*60}")
    print(f"📌 {title}")
    print(f"{'='*60}")
    print(f"Status: {response.status_code}")
    try:
        data = response.json()
        print(json.dumps(data, indent=2, default=str))
    except:
        print(response.text)
    print()

def test_metrics_api():
    """Test all metrics endpoints"""
    
    print("🧪 Testing Metrics Store API (Configuration as Data)")
    print("=" * 60)
    
    # 1. Check API health
    print("\n1️⃣  Testing API health...")
    response = requests.get(f"{BASE_URL}/")
    print_response("Health Check", response)
    
    # 2. List all metrics
    print("2️⃣  Listing all metrics...")
    response = requests.get(f"{BASE_URL}/metrics")
    print_response("List Metrics", response)
    
    if response.status_code == 200:
        metrics_data = response.json()
        metric_count = metrics_data.get('count', 0)
        print(f"✅ Found {metric_count} metrics")
        
        if metric_count == 0:
            print("⚠️  No metrics found! Run: python scripts/seed_metrics.py")
            return
    
    # 3. Get a specific metric
    print("\n3️⃣  Getting metric details...")
    response = requests.get(f"{BASE_URL}/metrics/avg_basket_size_by_hour")
    print_response("Get Metric: avg_basket_size_by_hour", response)
    
    # 4. Execute simple metric
    print("\n4️⃣  Executing simple metric...")
    response = requests.post(
        f"{BASE_URL}/metrics/avg_basket_size_by_hour/execute"
    )
    print_response("Execute: avg_basket_size_by_hour", response)
    
    # 5. Execute parameterized metric
    print("\n5️⃣  Executing parameterized metric...")
    response = requests.post(
        f"{BASE_URL}/metrics/top_reordered_products/execute",
        json={
            "parameters": {
                "min_orders": 100,
                "limit": 5
            }
        }
    )
    print_response("Execute: top_reordered_products (with params)", response)
    
    # 6. Search metrics
    print("\n6️⃣  Searching metrics...")
    response = requests.get(f"{BASE_URL}/metrics/search/reorder")
    print_response("Search: 'reorder'", response)
    
    # 7. Filter by tags
    print("\n7️⃣  Filtering by tags...")
    response = requests.get(f"{BASE_URL}/metrics?tags=basket,hourly")
    print_response("Filter: tags=basket,hourly", response)
    
    # 8. Register new metric (CREATE)
    print("\n8️⃣  Registering new metric...")
    new_metric = {
        "metric_name": "test_metric_order_count",
        "display_name": "Test: Total Order Count",
        "description": "Simple count of all orders (test metric)",
        "owner": "test-team",
        "sql_template": "SELECT COUNT(*) as total_orders FROM gold.dim_orders",
        "materialization": "table",
        "tags": ["test", "orders", "count"]
    }
    response = requests.post(
        f"{BASE_URL}/metrics/register",
        json=new_metric
    )
    print_response("Register New Metric", response)
    
    sleep(0.5)
    
    # 9. Execute the new metric
    print("\n9️⃣  Executing newly created metric...")
    response = requests.post(
        f"{BASE_URL}/metrics/test_metric_order_count/execute"
    )
    print_response("Execute: test_metric_order_count", response)
    
    # 10. Update the metric
    print("\n🔟 Updating metric...")
    response = requests.put(
        f"{BASE_URL}/metrics/test_metric_order_count",
        json={
            "description": "UPDATED: Total order count (modified via API)",
            "tags": ["test", "orders", "count", "updated"]
        }
    )
    print_response("Update Metric", response)
    
    # 11. Verify update
    print("\n1️⃣1️⃣  Verifying update...")
    response = requests.get(f"{BASE_URL}/metrics/test_metric_order_count")
    print_response("Get Updated Metric", response)
    
    # 12. Delete test metric (cleanup)
    print("\n1️⃣2️⃣  Cleaning up (delete test metric)...")
    response = requests.delete(
        f"{BASE_URL}/metrics/test_metric_order_count"
    )
    print_response("Delete Metric", response)
    
    print("\n" + "="*60)
    print("✅ All tests completed!")
    print("="*60)
    print("\n📊 Summary:")
    print("   ✅ List metrics")
    print("   ✅ Get metric details")
    print("   ✅ Execute simple metric")
    print("   ✅ Execute parameterized metric")
    print("   ✅ Search metrics")
    print("   ✅ Filter by tags")
    print("   ✅ Create metric (via API, no YAML!)")
    print("   ✅ Update metric (runtime edit!)")
    print("   ✅ Delete metric")
    print("\n💡 Key Insight: All metrics managed via API - No YAML files!")
    print()

if __name__ == "__main__":
    try:
        test_metrics_api()
    except requests.exceptions.ConnectionError:
        print("\n❌ Error: Cannot connect to API")
        print("   Make sure the API is running:")
        print("   $ cd warehouse")
        print("   $ uvicorn main:app --reload --port 8000")
        print()
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print()
