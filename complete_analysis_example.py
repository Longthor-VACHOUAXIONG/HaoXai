# Complete Data Analysis Example
# Step-by-step workflow in HaoXai Notebook

# Step 1: Install required packages (run this first)
import subprocess
import sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "seaborn"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "matplotlib"])

# Step 2: Import libraries
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Step 3: Connect to database and query data
conn = sqlite3.connect('your_database.db')

# Query sample data with joins
query = """
SELECT 
    s.Id,
    s.Notes,
    s.'RACK No_RowColumn',
    l.location_name,
    h.host_name
FROM samples s
LEFT JOIN locations l ON s.location_id = l.id
LEFT JOIN hosts h ON s.host_id = h.id
WHERE s.Notes LIKE '%CANB%'
LIMIT 100
"""

df = pd.read_sql_query(query, conn)
print("Data shape:", df.shape)
print("\nFirst 5 rows:")
print(df.head())

# Step 4: Data analysis
print("\nData Analysis:")
print(f"Total samples: {len(df)}")
print(f"Unique locations: {df['location_name'].nunique()}")
print(f"Unique hosts: {df['host_name'].nunique()}")

# Step 5: Create visualizations
plt.figure(figsize=(12, 8))

# Plot 1: Samples by location
plt.subplot(2, 2, 1)
df['location_name'].value_counts().head(10).plot(kind='bar')
plt.title('Samples by Location')
plt.xticks(rotation=45)

# Plot 2: Samples by host
plt.subplot(2, 2, 2)
df['host_name'].value_counts().head(10).plot(kind='bar')
plt.title('Samples by Host')
plt.xticks(rotation=45)

# Plot 3: Rack distribution
plt.subplot(2, 2, 3)
df['RACK No_RowColumn'].str.extract(r'RACK(\d+)')[0].value_counts().head(10).plot(kind='bar')
plt.title('Samples by Rack Number')
plt.xticks(rotation=45)

# Plot 4: Notes pattern analysis
plt.subplot(2, 2, 4)
df['Notes'].str.extract(r'ANAL(\d+)')[0].value_counts().head(10).plot(kind='bar')
plt.title('Samples by Analysis Number')
plt.xticks(rotation=45)

plt.tight_layout()
plt.show()

# Step 6: Export results
df.to_excel('analysis_results.xlsx', index=False)
print("\nResults exported to 'analysis_results.xlsx'")

conn.close()
