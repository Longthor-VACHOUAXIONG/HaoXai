# Environment Check - What's Actually Available
# This will show us what functions and variables are available

# Cell 1: Check Built-in Functions
print("=== Built-in Functions ===")
import builtins
print("Available built-ins:", [f for f in dir(builtins) if not f.startswith('_')][:10])

# Cell 2: Check Global Variables
print("\n=== Global Variables ===")
print("Available globals:", list(globals().keys()))

# Cell 3: Check Available Modules
print("\n=== Available Modules ===")
try:
    print("pandas:", 'pd' in globals())
    if 'pd' in globals():
        print("✅ pandas available")
    else:
        print("❌ pandas not available")
except:
    print("❌ pandas check failed")

try:
    print("matplotlib:", 'plt' in globals())
    if 'plt' in globals():
        print("✅ matplotlib available")
    else:
        print("❌ matplotlib not available")
except:
    print("❌ matplotlib check failed")

try:
    print("numpy:", 'np' in globals())
    if 'np' in globals():
        print("✅ numpy available")
    else:
        print("❌ numpy not available")
except:
    print("❌ numpy check failed")

# Cell 4: Check Database Functions
print("\n=== Database Functions ===")
print("get_tables:", 'get_tables' in globals())
print("conn:", 'conn' in globals())

if 'get_tables' in globals():
    try:
        tables = get_tables()
        print(f"✅ get_tables() works: {len(tables)} tables")
    except Exception as e:
        print(f"❌ get_tables() error: {e}")
else:
    print("❌ get_tables() not available")

# Cell 5: Test Simple Operations
print("\n=== Simple Operations ===")
try:
    # Test basic math
    result = 2 + 3
    print(f"✅ Math works: 2 + 3 = {result}")
except Exception as e:
    print(f"❌ Math error: {e}")

try:
    # Test string operations
    text = "Hello " + "World"
    print(f"✅ Strings work: {text}")
except Exception as e:
    print(f"❌ String error: {e}")

try:
    # Test list operations
    numbers = [1, 2, 3, 4, 5]
    total = sum(numbers)
    print(f"✅ Lists work: sum({numbers}) = {total}")
except Exception as e:
    print(f"❌ List error: {e}")

# Cell 6: Test If We Can Create Functions
print("\n=== Function Creation ===")
try:
    def test_function():
        return "Function works!"
    
    result = test_function()
    print(f"✅ Function creation works: {result}")
except Exception as e:
    print(f"❌ Function error: {e}")

# Cell 7: Test If We Can Import Standard Modules
print("\n=== Standard Module Imports ===")
try:
    import math
    print(f"✅ Math module works: sqrt(16) = {math.sqrt(16)}")
except Exception as e:
    print(f"❌ Math module error: {e}")

try:
    import datetime
    now = datetime.datetime.now()
    print(f"✅ Datetime works: {now.strftime('%Y-%m-%d')}")
except Exception as e:
    print(f"❌ Datetime error: {e}")

# Cell 8: Test Simple Plot (if matplotlib available)
print("\n=== Plot Test ===")
if 'plt' in globals():
    try:
        plt.figure(figsize=(6, 4))
        plt.plot([1, 2, 3], [2, 4, 1])
        plt.title('Test Plot')
        plt.show()
        print("✅ Plot works")
    except Exception as e:
        print(f"❌ Plot error: {e}")
else:
    print("❌ matplotlib not available for plotting")

# Cell 9: Test Database Query (if available)
print("\n=== Database Query Test ===")
if 'conn' in globals() and 'pd' in globals():
    try:
        # Try a very simple query
        test_df = pd.read_sql_query("SELECT 1 as test", conn)
        print(f"✅ Database query works: {test_df.shape}")
    except Exception as e:
        print(f"❌ Database query error: {e}")
else:
    print("❌ Database or pandas not available")

print("\n=== Environment Check Complete ===")
print("This shows exactly what's available in your Python environment!")
