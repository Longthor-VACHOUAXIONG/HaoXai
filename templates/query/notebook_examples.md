# SQL & Python Notebook - Quick Start Guide

## 🚀 Getting Started

Welcome to the SQL & Python Notebook! This Jupyter-style interface allows you to combine SQL queries with Python data analysis.

## 📚 Basic Examples

### 1. Explore Available Tables
```python
# Get all available tables in the database
tables = get_tables()
print("Available tables:")
for table in tables:
    print(f"  - {table}")
```

### 2. Preview a Table Structure
```python
# Preview the structure of a specific table
df = preview_table('samples')  # Change 'samples' to any table name
```

### 3. Load Data with SQL
```python
# Load data using SQL query
query = "SELECT * FROM samples LIMIT 100"
df = pd.read_sql_query(query, conn)

# Explore the data structure
explore_data(df, "samples")
```

### 4. Basic Data Analysis
```python
# After loading data, check what columns are available
print("Available columns:", df.columns.tolist())

# Get basic statistics
print("\nBasic statistics:")
print(df.describe())

# Check for missing values
print("\nMissing values:")
print(df.isnull().sum())
```

### 5. Create Visualizations
```python
# Create a simple bar chart
# First, check what columns might be good for visualization
print("Columns suitable for counting:")
for col in df.columns:
    if df[col].dtype == 'object' or df[col].nunique() < 20:
        print(f"  - {col} ({df[col].nunique()} unique values)")

# Example: Create a bar chart (replace 'column_name' with an actual column)
if 'column_name' in df.columns:
    plt.figure(figsize=(10, 6))
    df['column_name'].value_counts().head(10).plot(kind='bar')
    plt.title('Top 10 Values in column_name')
    plt.xlabel('column_name')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
```

### 6. Advanced SQL with Python
```python
# Complex SQL query with Python analysis
complex_query = """
SELECT 
    h.host_id,
    h.species,
    COUNT(s.sample_id) as sample_count,
    s.collection_date
FROM hosts h
LEFT JOIN samples s ON h.host_id = s.host_id
WHERE h.species IS NOT NULL
GROUP BY h.host_id, h.species, s.collection_date
ORDER BY sample_count DESC
LIMIT 50
"""

result_df = pd.read_sql_query(complex_query, conn)
explore_data(result_df, "host_sample_analysis")

# Create visualization
if not result_df.empty:
    plt.figure(figsize=(12, 6))
    result_df.head(10).plot(kind='bar', x='species', y='sample_count')
    plt.title('Sample Count by Species (Top 10)')
    plt.xlabel('Species')
    plt.ylabel('Sample Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
```

## 🛠️ Helper Functions Available

### `get_tables()`
Returns a list of all available tables in the database.

### `preview_table(table_name, limit=5)`
Shows the structure and sample data for a specific table.

### `explore_data(df, name="data")`
Provides detailed information about a DataFrame including:
- Shape and columns
- First few rows
- Data types
- Missing values
- Sample values for each column

## 🎯 Common Workflows

### Workflow 1: Data Exploration
1. Use `get_tables()` to see available tables
2. Use `preview_table()` to understand table structure
3. Load specific data with SQL queries
4. Use `explore_data()` to understand your dataset
5. Create visualizations with matplotlib

### Workflow 2: Statistical Analysis
1. Load data with SQL
2. Clean and prepare data with pandas
3. Perform statistical analysis
4. Create plots and charts
5. Export results if needed

### Workflow 3: Report Generation
1. Query multiple tables
2. Merge and transform data
3. Calculate summary statistics
4. Create comprehensive visualizations
5. Generate insights

## 🔧 Available Libraries

- **pandas** (as `pd`) - Data manipulation and analysis
- **numpy** (as `np`) - Numerical computing
- **matplotlib.pyplot** (as `plt`) - Plotting and visualization
- **datetime** - Date and time handling
- **re** - Regular expressions
- **json** - JSON handling
- **math**, `statistics` - Mathematical functions
- **collections**, `itertools`, `functools` - Utility functions

## ⚠️ Important Notes

- All Python code runs in a secure sandboxed environment
- Database connection is available as `conn`
- Use `pd.read_sql_query()` for SQL queries in Python
- Plots are automatically displayed when created
- DataFrames are automatically shown as tables when created

## 🚨 Troubleshooting

### KeyError: 'column_name'
This error means the column doesn't exist in your DataFrame. Use:
```python
print(df.columns.tolist())  # See all available columns
```

### Empty Results
Check if your SQL query returns data:
```python
# Test with a simple query first
test_df = pd.read_sql_query("SELECT COUNT(*) as total FROM your_table", conn)
print(test_df)
```

### Plot Not Showing
Make sure to call `plt.show()` after creating your plot.

## 💡 Pro Tips

1. **Start Small**: Begin with simple queries and gradually build complexity
2. **Use Helper Functions**: `explore_data()` is your best friend for understanding data
3. **Check Column Names**: Always verify column names before using them
4. **Limit Results**: Use `LIMIT` in SQL queries to avoid loading too much data
5. **Iterate**: Test each cell individually before combining complex operations

Happy data analysis! 🎉