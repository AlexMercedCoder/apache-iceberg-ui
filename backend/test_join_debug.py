import requests

BASE_URL = "http://localhost:8000"

# Test just the join query
sql = """
SELECT u.name, o.amount 
FROM dremio_1.test_ns_phase4_v2.users u 
JOIN dremio_2.test_ns_phase4_v2.orders o ON u.id = o.user_id
"""

res = requests.post(f"{BASE_URL}/query", json={"sql": sql, "catalog": "default"})
print("Join Result:", res.json())
