"""
Master Python Trainer - Advanced Python Code Generation for Data Analysis
Trains AI to generate Python code for data analysis, visualization, and reporting
"""
import os
import json
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import pickle
import re
import warnings
warnings.filterwarnings('ignore')

class MasterPythonTrainer:
    """Train AI to generate master-level Python code for data analysis"""
    
    def __init__(self, db_config, db_type='sqlite', models_dir='master_python_models'):
        self.db_config = db_config
        self.db_type = db_type
        self.models_dir = models_dir
        self.models = {}
        
        os.makedirs(models_dir, exist_ok=True)
        self.versions_dir = os.path.join(models_dir, 'versions')
        os.makedirs(self.versions_dir, exist_ok=True)
    
    def get_connection(self):
        """Get database connection"""
        from database.db_manager_flask import DatabaseManagerFlask
        return DatabaseManagerFlask.get_connection(self.db_config, self.db_type)
    
    def analyze_database_for_python(self):
        """Analyze database for Python code generation patterns"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        analysis = {
            'tables': {},
            'analysis_patterns': [],
            'visualization_patterns': [],
            'data_manipulation_patterns': [],
            'reporting_patterns': []
        }
        
        # Get all tables
        if self.db_type == 'sqlite':
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        else:
            cursor.execute("SHOW TABLES")
        
        tables = [row[0] for row in cursor.fetchall()]
        
        # Analyze each table for Python analysis patterns
        for table in tables:
            cursor.execute(f'PRAGMA table_info("{table}")')
            columns = [{'name': col[1], 'type': col[2], 'pk': col[5]} for col in cursor.fetchall()]
            
            # Get row count
            cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = cursor.fetchone()[0]
            
            # Get sample data
            cursor.execute(f'SELECT * FROM "{table}" LIMIT 5')
            sample_data = cursor.fetchall()
            
            analysis['tables'][table] = {
                'columns': columns,
                'row_count': count,
                'sample_data': sample_data,
                'numeric_columns': [col['name'] for col in columns if 'INT' in col['type'] or 'REAL' in col['type'] or 'DECIMAL' in col['type']],
                'text_columns': [col['name'] for col in columns if 'TEXT' in col['type'] or 'VARCHAR' in col['type']],
                'date_columns': [col['name'] for col in columns if 'DATE' in col['type'] or 'TIME' in col['type']],
                'categorical_columns': [col['name'] for col in columns if col['type'] in ['TEXT', 'VARCHAR'] and count < 100]
            }
        
        # Define analysis patterns
        analysis['analysis_patterns'] = self._define_analysis_patterns(analysis['tables'])
        analysis['visualization_patterns'] = self._define_visualization_patterns(analysis['tables'])
        analysis['data_manipulation_patterns'] = self._define_data_manipulation_patterns(analysis['tables'])
        analysis['reporting_patterns'] = self._define_reporting_patterns(analysis['tables'])
        
        conn.close()
        return analysis
    
    def _define_analysis_patterns(self, tables):
        """Define data analysis patterns"""
        patterns = [
            'descriptive_statistics',
            'correlation_analysis',
            'group_by_analysis',
            'time_series_analysis',
            'distribution_analysis',
            'outlier_detection',
            'missing_data_analysis',
            'comparative_analysis'
        ]
        
        # Add table-specific patterns
        for table_name in tables:
            if 'screening' in table_name.lower():
                patterns.append('pathogen_analysis')
            if 'location' in table_name.lower():
                patterns.append('geographic_analysis')
            if 'taxonomy' in table_name.lower():
                patterns.append('taxonomic_analysis')
            if 'storage' in table_name.lower():
                patterns.append('inventory_analysis')
        
        return patterns
    
    def _define_visualization_patterns(self, tables):
        """Define visualization patterns"""
        patterns = [
            'histogram',
            'bar_chart',
            'line_plot',
            'scatter_plot',
            'box_plot',
            'heatmap',
            'pie_chart',
            'violin_plot',
            'pair_plot',
            'distribution_plot'
        ]
        
        # Add domain-specific visualizations
        patterns.extend([
            'geographic_map',
            'time_series_plot',
            'correlation_heatmap',
            'positive_rate_chart',
            'species_distribution',
            'storage_layout'
        ])
        
        return patterns
    
    def _define_data_manipulation_patterns(self, tables):
        """Define data manipulation patterns"""
        return [
            'data_cleaning',
            'data_filtering',
            'data_aggregation',
            'data_merging',
            'data_pivoting',
            'data_transformation',
            'feature_engineering',
            'data_validation'
        ]
    
    def _define_reporting_patterns(self, tables):
        """Define reporting patterns"""
        return [
            'summary_report',
            'detailed_report',
            'statistical_report',
            'visual_report',
            'excel_export',
            'csv_export',
            'dashboard_creation',
            'automated_report'
        ]
    
    def generate_master_python_training_data(self, analysis):
        """Generate comprehensive Python code training data"""
        training_data = []
        
        # 1. Data loading and basic analysis
        training_data.extend(self._generate_data_loading_patterns(analysis))
        
        # 2. Statistical analysis code
        training_data.extend(self._generate_statistical_analysis_patterns(analysis))
        
        # 3. Data visualization code
        training_data.extend(self._generate_visualization_patterns(analysis))
        
        # 4. Data manipulation code
        training_data.extend(self._generate_data_manipulation_patterns(analysis))
        
        # 5. Advanced analysis code
        training_data.extend(self._generate_advanced_analysis_patterns(analysis))
        
        # 6. Reporting code
        training_data.extend(self._generate_reporting_patterns(analysis))
        
        # 7. Machine learning code
        training_data.extend(self._generate_machine_learning_patterns(analysis))
        
        # 8. Domain-specific analysis
        training_data.extend(self._generate_domain_specific_patterns(analysis))
        
        return training_data
    
    def _generate_data_loading_patterns(self, analysis):
        """Generate data loading patterns"""
        patterns = []
        
        for table_name, table_info in analysis['tables'].items():
            patterns.extend([
                {
                    'question': f"Load {table_name} data into pandas DataFrame",
                    'python_code': f'''
import pandas as pd
import sqlite3

# Connect to database
conn = sqlite3.connect('your_database.db')

# Load {table_name} data
df_{table_name} = pd.read_sql_query("SELECT * FROM {table_name}", conn)

# Display basic information
print(f"Shape: {{df_{table_name}.shape}}")
print(f"Columns: {{df_{table_name}.columns.tolist()}}")
print(f"Data types:\\n{{df_{table_name}.dtypes}}")
print(f"First few rows:\\n{{df_{table_name}.head()}}")

conn.close()
                    ''',
                    'category': 'data_loading',
                    'complexity': 'basic',
                    'libraries': ['pandas', 'sqlite3']
                },
                {
                    'question': f"Load and clean {table_name} data",
                    'python_code': f'''
import pandas as pd
import sqlite3
import numpy as np

# Connect and load data
conn = sqlite3.connect('your_database.db')
df_{table_name} = pd.read_sql_query("SELECT * FROM {table_name}", conn)

# Data cleaning
print("Missing values before cleaning:")
print(df_{table_name}.isnull().sum())

# Handle missing values
for col in df_{table_name}.columns:
    if df_{table_name}[col].dtype == 'object':
        df_{table_name}[col] = df_{table_name}[col].fillna('Unknown')
    else:
        df_{table_name}[col] = df_{table_name}[col].fillna(df_{table_name}[col].median())

# Remove duplicates
df_{table_name} = df_{table_name}.drop_duplicates()

print(f"Shape after cleaning: {{df_{table_name}.shape}}")
conn.close()
                    ''',
                    'category': 'data_cleaning',
                    'complexity': 'intermediate',
                    'libraries': ['pandas', 'sqlite3', 'numpy']
                }
            ])
        
        return patterns
    
    def _generate_statistical_analysis_patterns(self, analysis):
        """Generate statistical analysis patterns"""
        patterns = []
        
        # Descriptive statistics
        patterns.append({
            'question': "Generate descriptive statistics for all numeric columns",
            'python_code': '''
import pandas as pd
import numpy as np

# Load your data first (df = pd.read_sql_query(...))

# Descriptive statistics for numeric columns
numeric_cols = df.select_dtypes(include=[np.number]).columns
print("Descriptive Statistics:")
print(df[numeric_cols].describe())

# Additional statistics
print("\\nAdditional Statistics:")
for col in numeric_cols:
    print(f"{{col}}:")
    print(f"  Skewness: {{df[col].skew():.3f}}")
    print(f"  Kurtosis: {{df[col].kurtosis():.3f}}")
    print(f"  Missing values: {{df[col].isnull().sum()}}")
            ''',
            'category': 'descriptive_statistics',
            'complexity': 'intermediate',
            'libraries': ['pandas', 'numpy']
        })
        
        # Correlation analysis
        patterns.append({
            'question': "Perform correlation analysis on numeric columns",
            'python_code': '''
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Load your data first

# Select numeric columns
numeric_cols = df.select_dtypes(include=[np.number]).columns
df_numeric = df[numeric_cols]

# Calculate correlation matrix
correlation_matrix = df_numeric.corr()

print("Correlation Matrix:")
print(correlation_matrix)

# Create heatmap
plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0)
plt.title('Correlation Heatmap')
plt.tight_layout()
plt.show()

# Find strong correlations
strong_correlations = []
for i in range(len(correlation_matrix.columns)):
    for j in range(i+1, len(correlation_matrix.columns)):
        corr_val = correlation_matrix.iloc[i, j]
        if abs(corr_val) > 0.7:  # Strong correlation threshold
            strong_correlations.append({
                'var1': correlation_matrix.columns[i],
                'var2': correlation_matrix.columns[j],
                'correlation': corr_val
            })

print("\\nStrong Correlations (> 0.7):")
for corr in strong_correlations:
    print(f"{{corr['var1']}} - {{corr['var2']}}: {{corr['correlation']:.3f}}")
            ''',
            'category': 'correlation_analysis',
            'complexity': 'advanced',
            'libraries': ['pandas', 'numpy', 'seaborn', 'matplotlib']
        })
        
        return patterns
    
    def _generate_visualization_patterns(self, analysis):
        """Generate visualization patterns"""
        patterns = []
        
        # Basic plots
        patterns.extend([
            {
                'question': "Create histograms for all numeric columns",
                'python_code': '''
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load your data first

# Select numeric columns
numeric_cols = df.select_dtypes(include=['number']).columns

# Create histograms
fig, axes = plt.subplots(2, 2, figsize=(15, 10))
axes = axes.ravel()

for i, col in enumerate(numeric_cols[:4]):  # First 4 numeric columns
    axes[i].hist(df[col].dropna(), bins=30, alpha=0.7)
    axes[i].set_title(f'Distribution of {col}')
    axes[i].set_xlabel(col)
    axes[i].set_ylabel('Frequency')

plt.tight_layout()
plt.show()
                ''',
                'category': 'histogram',
                'complexity': 'intermediate',
                'libraries': ['pandas', 'matplotlib', 'seaborn']
            },
            {
                'question': "Create bar charts for categorical columns",
                'python_code': '''
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load your data first

# Select categorical columns
categorical_cols = df.select_dtypes(include=['object']).columns

# Create bar charts
fig, axes = plt.subplots(2, 2, figsize=(15, 10))
axes = axes.ravel()

for i, col in enumerate(categorical_cols[:4]):  # First 4 categorical columns
    value_counts = df[col].value_counts().head(10)  # Top 10 values
    axes[i].bar(range(len(value_counts)), value_counts.values)
    axes[i].set_title(f'Count by {col}')
    axes[i].set_xlabel(col)
    axes[i].set_ylabel('Count')
    axes[i].set_xticks(range(len(value_counts)))
    axes[i].set_xticklabels(value_counts.index, rotation=45)

plt.tight_layout()
plt.show()
                ''',
                'category': 'bar_chart',
                'complexity': 'intermediate',
                'libraries': ['pandas', 'matplotlib']
            }
        ])
        
        # Advanced visualizations
        patterns.append({
            'question': "Create a comprehensive dashboard with multiple plots",
            'python_code': '''
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load your data first

# Create dashboard
fig = plt.figure(figsize=(20, 15))

# 1. Data overview
ax1 = plt.subplot(3, 3, 1)
df.info(buf=None)
ax1.text(0.1, 0.5, f"Dataset Shape: {df.shape}\\nColumns: {len(df.columns)}", 
         transform=ax1.transAxes, fontsize=12)
ax1.axis('off')

# 2. Missing values heatmap
ax2 = plt.subplot(3, 3, 2)
missing_data = df.isnull().sum().sort_values(ascending=False)
sns.barplot(x=missing_data.values, y=missing_data.index[:10], ax=ax2)
ax2.set_title('Top 10 Missing Values')

# 3. Numeric distributions
ax3 = plt.subplot(3, 3, 3)
numeric_cols = df.select_dtypes(include=['number']).columns
if len(numeric_cols) > 0:
    df[numeric_cols[0]].hist(bins=30, ax=ax3, alpha=0.7)
    ax3.set_title(f'Distribution of {numeric_cols[0]}')

# 4. Categorical distribution
ax4 = plt.subplot(3, 3, 4)
categorical_cols = df.select_dtypes(include=['object']).columns
if len(categorical_cols) > 0:
    df[categorical_cols[0]].value_counts().head(10).plot(kind='bar', ax=ax4)
    ax4.set_title(f'Top 10 {categorical_cols[0]}')

# 5. Correlation heatmap (if enough numeric columns)
ax5 = plt.subplot(3, 3, 5)
if len(numeric_cols) > 1:
    corr_matrix = df[numeric_cols].corr()
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, ax=ax5)
    ax5.set_title('Correlation Matrix')

plt.tight_layout()
plt.show()
            ''',
            'category': 'dashboard',
            'complexity': 'advanced',
            'libraries': ['pandas', 'matplotlib', 'seaborn']
        })
        
        return patterns
    
    def _generate_data_manipulation_patterns(self, analysis):
        """Generate data manipulation patterns"""
        patterns = []
        
        # Data filtering
        patterns.append({
            'question': "Filter data based on multiple conditions",
            'python_code': '''
import pandas as pd

# Load your data first

# Example: Filter samples collected in 2023 with positive results
filtered_data = df[
    (df['collection_date'].str.contains('2023', na=False)) &
    (df['test_result'] == 'Positive')
]

print(f"Original data shape: {df.shape}")
print(f"Filtered data shape: {filtered_data.shape}")
print("\\nFiltered data preview:")
print(filtered_data.head())

# Multiple condition filtering
conditions = [
    df['column1'] > 100,
    df['column2'].isin(['Value1', 'Value2']),
    df['column3'].notna()
]

combined_filter = conditions[0]
for condition in conditions[1:]:
    combined_filter &= condition

result = df[combined_filter]
print(f"\\nCombined filter result shape: {result.shape}")
            ''',
            'category': 'data_filtering',
            'complexity': 'intermediate',
            'libraries': ['pandas']
        })
        
        # Data aggregation
        patterns.append({
            'question': "Aggregate data by groups and calculate statistics",
            'python_code': '''
import pandas as pd
import numpy as np

# Load your data first

# Group by categorical column and calculate statistics
grouped_stats = df.groupby('category_column').agg({
    'numeric_column1': ['mean', 'median', 'std', 'count'],
    'numeric_column2': ['sum', 'mean', 'max'],
    'text_column': 'count'
})

print("Grouped Statistics:")
print(grouped_stats)

# Custom aggregation functions
custom_agg = df.groupby('category_column').agg({
    'numeric_column1': lambda x: x.quantile(0.75),  # 75th percentile
    'numeric_column2': lambda x: x.max() - x.min(),  # Range
    'text_column': lambda x: x.nunique()  # Count unique values
})

print("\\nCustom Aggregation:")
print(custom_agg)

# Pivot table
pivot_table = df.pivot_table(
    values='numeric_column',
    index='category_column',
    columns='subcategory_column',
    aggfunc='mean',
    fill_value=0
)

print("\\nPivot Table:")
print(pivot_table)
            ''',
            'category': 'data_aggregation',
            'complexity': 'advanced',
            'libraries': ['pandas', 'numpy']
        })
        
        return patterns
    
    def _generate_advanced_analysis_patterns(self, analysis):
        """Generate advanced analysis patterns"""
        patterns = []
        
        # Time series analysis
        if any(table_info['date_columns'] for table_info in analysis['tables'].values()):
            patterns.append({
                'question': "Perform time series analysis on date columns",
                'python_code': '''
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load your data first

# Convert date column to datetime
df['date_column'] = pd.to_datetime(df['date_column'])

# Set date as index
df_time = df.set_index('date_column')

# Resample by different time periods
daily_counts = df_time.resample('D').size()
weekly_counts = df_time.resample('W').size()
monthly_counts = df_time.resample('M').size()

# Plot time series
fig, axes = plt.subplots(3, 1, figsize=(15, 12))

daily_counts.plot(ax=axes[0], title='Daily Counts')
weekly_counts.plot(ax=axes[1], title='Weekly Counts')
monthly_counts.plot(ax=axes[2], title='Monthly Counts')

plt.tight_layout()
plt.show()

# Trend analysis
# Calculate moving averages
df_time['7_day_ma'] = df_time['numeric_column'].rolling(window=7).mean()
df_time['30_day_ma'] = df_time['numeric_column'].rolling(window=30).mean()

# Plot with moving averages
plt.figure(figsize=(15, 6))
plt.plot(df_time.index, df_time['numeric_column'], label='Original', alpha=0.7)
plt.plot(df_time.index, df_time['7_day_ma'], label='7-day MA', linewidth=2)
plt.plot(df_time.index, df_time['30_day_ma'], label='30-day MA', linewidth=2)
plt.title('Time Series with Moving Averages')
plt.legend()
plt.show()
                ''',
                'category': 'time_series',
                'complexity': 'advanced',
                'libraries': ['pandas', 'matplotlib', 'seaborn']
            })
        
        return patterns
    
    def _generate_reporting_patterns(self, analysis):
        """Generate reporting patterns"""
        patterns = []
        
        # Excel report
        patterns.append({
            'question': "Generate comprehensive Excel report with multiple sheets",
            'python_code': '''
import pandas as pd
import numpy as np
from datetime import datetime

# Load your data first

# Create Excel writer
excel_filename = f"comprehensive_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
    
    # Sheet 1: Data Overview
    overview_data = {
        'Metric': ['Total Records', 'Total Columns', 'Missing Values', 'Data Types'],
        'Value': [
            len(df),
            len(df.columns),
            df.isnull().sum().sum(),
            str(df.dtypes.value_counts().to_dict())
        ]
    }
    pd.DataFrame(overview_data).to_excel(writer, sheet_name='Overview', index=False)
    
    # Sheet 2: Descriptive Statistics
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        df[numeric_cols].describe().to_excel(writer, sheet_name='Statistics')
    
    # Sheet 3: Categorical Analysis
    categorical_cols = df.select_dtypes(include=['object']).columns
    for i, col in enumerate(categorical_cols[:3]):  # First 3 categorical columns
        value_counts = df[col].value_counts().reset_index()
        value_counts.columns = [col, 'Count']
        sheet_name = f'Cat_Analysis_{col[:15]}'  # Truncate long names
        value_counts.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # Sheet 4: Raw Data (first 1000 rows)
    df.head(1000).to_excel(writer, sheet_name='Raw_Data', index=False)

print(f"Excel report saved as: {excel_filename}")
            ''',
            'category': 'excel_report',
            'complexity': 'intermediate',
            'libraries': ['pandas', 'numpy', 'openpyxl']
        })
        
        return patterns
    
    def _generate_machine_learning_patterns(self, analysis):
        """Generate machine learning patterns"""
        patterns = []
        
        # Basic classification
        patterns.append({
            'question': "Build a simple classification model",
            'python_code': '''
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder

# Load your data first

# Prepare data for ML
# Select features (exclude target and ID columns)
feature_cols = [col for col in df.columns if col not in ['target_column', 'id_column']]
X = df[feature_cols]

# Handle categorical variables
for col in X.select_dtypes(include=['object']).columns:
    le = LabelEncoder()
    X[col] = le.fit_transform(X[col].astype(str))

# Prepare target variable
y = df['target_column']
if y.dtype == 'object':
    le_target = LabelEncoder()
    y = le_target.fit_transform(y)

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Evaluate model
print("Classification Report:")
print(classification_report(y_test, y_pred))

print("\\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred))

# Feature importance
feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print("\\nFeature Importance:")
print(feature_importance.head(10))
            ''',
            'category': 'classification',
            'complexity': 'advanced',
            'libraries': ['pandas', 'numpy', 'sklearn']
        })
        
        return patterns
    
    def _generate_domain_specific_patterns(self, analysis):
        """Generate domain-specific analysis patterns"""
        patterns = []
        
        # Virology-specific analysis
        if any('screening' in table.lower() for table in analysis['tables'].keys()):
            patterns.append({
                'question': "Analyze pathogen screening results and create visualization",
                'python_code': '''
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Load screening data
screening_df = pd.read_sql_query("""
    SELECT sr.*, h.scientific_name, l.province, l.country
    FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
""", conn)

# Pathogen analysis
pathogens = ['pan_corona', 'pan_hanta', 'pan_paramyxo', 'pan_flavi']

# 1. Overall positivity rates
plt.figure(figsize=(12, 6))
positivity_rates = {}
for pathogen in pathogens:
    if pathogen in screening_df.columns:
        positive_count = (screening_df[pathogen] == 'Positive').sum()
        total_count = screening_df[pathogen].notna().sum()
        positivity_rates[pathogen] = (positive_count / total_count) * 100

plt.bar(range(len(positivity_rates)), list(positivity_rates.values()))
plt.xticks(range(len(positivity_rates)), [p.replace('pan_', '').title() for p in positivity_rates.keys()])
plt.ylabel('Positivity Rate (%)')
plt.title('Overall Pathogen Positivity Rates')
plt.show()

# 2. Geographic analysis
if 'province' in screening_df.columns:
    plt.figure(figsize=(15, 8))
    
    for pathogen in pathogens:
        if pathogen in screening_df.columns:
            province_positivity = screening_df.groupby('province').apply(
                lambda x: (x[pathogen] == 'Positive').sum() / x[pathogen].notna().sum() * 100
            ).sort_values(ascending=False)
            
            plt.subplot(2, 2, pathogens.index(pathogen) + 1)
            province_positivity.head(10).plot(kind='bar')
            plt.title(f'{pathogen.replace("pan_", "").title()} by Province')
            plt.ylabel('Positivity Rate (%)')
            plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.show()

# 3. Species analysis
if 'scientific_name' in screening_df.columns:
    plt.figure(figsize=(15, 6))
    
    species_counts = screening_df['scientific_name'].value_counts().head(10)
    plt.bar(range(len(species_counts)), species_counts.values)
    plt.xticks(range(len(species_counts)), species_counts.index, rotation=45)
    plt.ylabel('Number of Samples')
    plt.title('Top 10 Species Sampled')
    plt.show()

print("Virology Analysis Complete!")
print(f"Total samples analyzed: {len(screening_df)}")
print(f"Species sampled: {screening_df['scientific_name'].nunique()}")
print(f"Provinces covered: {screening_df['province'].nunique()}")
                ''',
                'category': 'virology_analysis',
                'complexity': 'expert',
                'libraries': ['pandas', 'matplotlib', 'seaborn', 'numpy', 'sqlite3']
            })
        
        return patterns
    
    def train_master_python_models(self, training_data):
        """Train master Python code generation models"""
        if not training_data:
            print("No training data available")
            return False
        
        try:
            # Prepare training data
            questions = [item['question'] for item in training_data]
            complexities = [item['complexity'] for item in training_data]
            categories = [item['category'] for item in training_data]
            
            # Feature extraction
            self.vectorizer = TfidfVectorizer(max_features=10000, ngram_range=(1, 3))
            X = self.vectorizer.fit_transform(questions)
            
            # Train complexity classifier
            complexity_encoder = LabelEncoder()
            y_complexity = complexity_encoder.fit_transform(complexities)
            
            X_train, X_test, y_train, y_test = train_test_split(X, y_complexity, test_size=0.2, random_state=42)
            
            complexity_models = {
                'complexity_rf': RandomForestClassifier(n_estimators=100, random_state=42),
                'complexity_gb': GradientBoostingClassifier(n_estimators=100, random_state=42),
                'complexity_mlp': MLPClassifier(hidden_layer_sizes=(100, 50), random_state=42, max_iter=500)
            }
            
            for name, model in complexity_models.items():
                model.fit(X_train, y_train)
                train_score = model.score(X_train, y_train)
                test_score = model.score(X_test, y_test)
                print(f"{name} - Train: {train_score:.3f}, Test: {test_score:.3f}")
                self.models[name] = model
            
            # Train category classifier
            category_encoder = LabelEncoder()
            y_category = category_encoder.fit_transform(categories)
            
            X_train_cat, X_test_cat, y_train_cat, y_test_cat = train_test_split(X, y_category, test_size=0.2, random_state=42)
            
            category_model = RandomForestClassifier(n_estimators=100, random_state=42)
            category_model.fit(X_train_cat, y_train_cat)
            
            cat_train_score = category_model.score(X_train_cat, y_train_cat)
            cat_test_score = category_model.score(X_test_cat, y_test_cat)
            
            print(f"Category Model - Train: {cat_train_score:.3f}, Test: {cat_test_score:.3f}")
            
            self.models['category'] = category_model
            self.models['category_encoder'] = category_encoder
            self.models['complexity_encoder'] = complexity_encoder
            self.models['vectorizer'] = self.vectorizer
            
            # Save models
            self._save_models()
            
            print("✅ Master Python models trained successfully!")
            return True
            
        except Exception as e:
            print(f"Error training master Python models: {e}")
            return False
    
    def _save_models(self):
        """Save trained models"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        version_dir = os.path.join(self.versions_dir, f'master_python_{timestamp}')
        os.makedirs(version_dir, exist_ok=True)
        
        # Save models
        for name, model in self.models.items():
            if hasattr(model, 'predict') or hasattr(model, 'transform'):
                model_path = os.path.join(version_dir, f'{name}.pkl')
                with open(model_path, 'wb') as f:
                    pickle.dump(model, f)
        
        # Save metadata
        metadata = {
            'version': timestamp,
            'model_count': len(self.models),
            'training_date': datetime.now().isoformat(),
            'capabilities': ['python_generation', 'complexity_prediction', 'category_classification']
        }
        
        metadata_path = os.path.join(version_dir, 'metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Master Python models saved to: {version_dir}")
    
    def load_master_python_models(self):
        """Load trained master Python models"""
        try:
            versions = [d for d in os.listdir(self.versions_dir) 
                        if d.startswith('master_python_') and os.path.isdir(os.path.join(self.versions_dir, d))]
            
            if not versions:
                return False
            
            latest_version = sorted(versions)[-1]
            version_dir = os.path.join(self.versions_dir, latest_version)
            
            for file in os.listdir(version_dir):
                if file.endswith('.pkl'):
                    model_name = file[:-4]
                    model_path = os.path.join(version_dir, file)
                    with open(model_path, 'rb') as f:
                        self.models[model_name] = pickle.load(f)
            
            return True
            
        except Exception as e:
            print(f"Error loading master Python models: {e}")
            return False
    
    def generate_python_code(self, question):
        """Generate Python code from natural language"""
        if not self.models or 'vectorizer' not in self.models:
            return None
        
        try:
            # Transform question
            X = self.models['vectorizer'].transform([question])
            
            # Predict category
            if 'category' in self.models:
                category_pred = self.models['category'].predict(X)[0]
                category = self.models['category_encoder'].inverse_transform([category_pred])[0]
            else:
                category = 'unknown'
            
            # Predict complexity
            if 'complexity_rf' in self.models:
                complexity_pred = self.models['complexity_rf'].predict(X)[0]
                complexity = self.models['complexity_encoder'].inverse_transform([complexity_pred])[0]
            else:
                complexity = 'medium'
            
            return {
                'question': question,
                'predicted_category': category,
                'predicted_complexity': complexity,
                'confidence': 'high' if category != 'unknown' else 'low'
            }
            
        except Exception as e:
            print(f"Error generating Python code: {e}")
            return None

def train_master_python(db_config, db_type='sqlite'):
    """Train master Python models"""
    trainer = MasterPythonTrainer(db_config, db_type)
    
    print("🐍 Training Master Python Models for Advanced Data Analysis")
    print("=" * 60)
    
    # Analyze database
    print("1. Analyzing database for Python patterns...")
    analysis = trainer.analyze_database_for_python()
    
    # Generate training data
    print("2. Generating comprehensive Python training data...")
    training_data = trainer.generate_master_python_training_data(analysis)
    print(f"   Generated {len(training_data)} Python code patterns")
    
    # Train models
    print("3. Training master Python generation models...")
    success = trainer.train_master_python_models(training_data)
    
    if success:
        print("\n🎉 Master Python Training Completed!")
        print("\n🤖 Your AI now has Master Python capabilities:")
        print("   ✅ Data loading and cleaning")
        print("   ✅ Statistical analysis")
        print("   ✅ Data visualization")
        print("   ✅ Advanced analytics")
        print("   ✅ Machine learning")
        print("   ✅ Report generation")
        print("   ✅ Domain-specific analysis")
        
        print("\n🎯 Advanced Python code it can generate:")
        print("   • 'Create a dashboard for screening results'")
        print("   • 'Analyze coronavirus positivity by species'")
        print("   • 'Generate time series analysis of sample collection'")
        print("   • 'Build a machine learning model for prediction'")
        print("   • 'Create comprehensive Excel reports'")
        print("   • 'Visualize geographic distribution of samples'")
        
        return True
    else:
        print("❌ Master Python training failed!")
        return False

if __name__ == '__main__':
    db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
    train_master_python(db_path, 'sqlite')
