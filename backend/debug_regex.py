import re

sql = """
    SELECT u.name, o.amount 
    FROM dremio_1.test_ns_phase4_v2.users u 
    JOIN dremio_2.test_ns_phase4_v2.orders o ON u.id = o.user_id
    """

table_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)+)(?:\$([a-zA-Z0-9_]+))?\b'
matches = re.findall(table_pattern, sql, re.IGNORECASE)

print(f"SQL: {sql}")
print(f"Matches: {matches}")
