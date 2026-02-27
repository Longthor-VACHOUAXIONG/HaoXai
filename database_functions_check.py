# Check Database Functions Available
print("=== Database Functions Check ===")

# Check what database functions are available
print("Database functions:")
print("get_tables:", 'get_tables' in globals())
print("conn:", 'conn' in globals())
print("preview_table:", 'preview_table' in globals())
print("explore_data:", 'explore_data' in globals())

# Test database connection
if 'get_tables' in globals():
    try:
        tables = get_tables()
        print(f"\n✅ Found {len(tables)} tables:")
        for i, table in enumerate(tables[:10], 1):
            print(f"  {i}. {table}")
    except Exception as e:
        print(f"❌ get_tables error: {e}")
else:
    print("❌ get_tables not available")

# Test if we can do basic SQL queries
if 'conn' in globals():
    try:
        # Try a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        print(f"\n✅ Database connection works: {result}")
    except Exception as e:
        print(f"❌ Database connection error: {e}")
else:
    print("❌ conn not available")

# Test what we can do without pandas
print("\n=== Basic Analysis Without Pandas ===")

# Create sample data analysis using basic Python
sample_data = [
    {'name': 'Sample1', 'value': 10, 'category': 'A'},
    {'name': 'Sample2', 'value': 15, 'category': 'B'},
    {'name': 'Sample3', 'value': 8, 'category': 'A'},
    {'name': 'Sample4', 'value': 12, 'category': 'C'},
]

print("Sample data analysis:")
total_value = sum(item['value'] for item in sample_data)
avg_value = total_value / len(sample_data)
categories = set(item['category'] for item in sample_data)

print(f"Total value: {total_value}")
print(f"Average value: {avg_value:.2f}")
print(f"Categories: {categories}")

# Group by category
category_totals = {}
for item in sample_data:
    cat = item['category']
    if cat not in category_totals:
        category_totals[cat] = 0
    category_totals[cat] += item['value']

print("Category totals:")
for cat, total in category_totals.items():
    print(f"  {cat}: {total}")

print("\n=== Basic Analysis Works ===")
print("We can do analysis with basic Python + SQL!")
