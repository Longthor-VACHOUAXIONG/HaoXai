# Example: Query Database Data with Python
# Copy this code into a Python cell in the notebook

import pandas as pd
import sqlite3

# Connect to database (adjust connection as needed)
conn = sqlite3.connect('your_database.db')  # or use your database connection

# Example 1: Query all data from a table
df_samples = pd.read_sql_query("SELECT * FROM samples LIMIT 10", conn)
print("Sample data:")
print(df_samples)

# Example 2: Query with conditions
df_filtered = pd.read_sql_query("""
    SELECT Id, Notes, 'RACK No_RowColumn' 
    FROM samples 
    WHERE Notes LIKE '%CANB_ANAL22%'
    LIMIT 5
""", conn)
print("\nFiltered data:")
print(df_filtered)

# Example 3: Aggregate query
df_counts = pd.read_sql_query("""
    SELECT 
        Notes,
        COUNT(*) as count,
        'RACK No_RowColumn'
    FROM samples 
    GROUP BY Notes
    ORDER BY count DESC
    LIMIT 10
""", conn)
print("\nAggregated data:")
print(df_counts)

conn.close()
