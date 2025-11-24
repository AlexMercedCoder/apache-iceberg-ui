import re

sql = "SELECT * FROM dremio_1.test_ns_phase4_v2.users u"
target = "dremio_1.test_ns_phase4_v2.users"
alias = "dremio_1_test_ns_phase4_v2_users"

# Current approach (with \b)
result1 = re.sub(rf'\b{re.escape(target)}\b', alias, sql, flags=re.IGNORECASE)
print(f"With \\b: {result1}")

# Without \b
result2 = re.sub(rf'{re.escape(target)}', alias, sql, flags=re.IGNORECASE)
print(f"Without \\b: {result2}")
