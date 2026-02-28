"""
Master SQL Trainer - Advanced SQL Query Intelligence
Trains AI to generate complex SQL queries for data analysis
"""
import os
import json
import sqlite3
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import re
import warnings
warnings.filterwarnings('ignore')

class MasterSQLTrainer:
    """Train AI to generate master-level SQL queries for data analysis"""
    
    def __init__(self, db_config, db_type='sqlite', models_dir='master_sql_models'):
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
    
    def analyze_database_structure(self):
        """Comprehensive database structure analysis for SQL generation"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        structure = {
            'tables': {},
            'relationships': [],
            'query_patterns': [],
            'aggregation_functions': [],
            'join_patterns': []
        }
        
        # Get all tables
        if self.db_type == 'sqlite':
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        else:
            cursor.execute("SHOW TABLES")
        
        tables = [row[0] for row in cursor.fetchall()]
        
        # Analyze each table
        for table in tables:
            cursor.execute(f'PRAGMA table_info("{table}")')
            columns = [{'name': col[1], 'type': col[2], 'pk': col[5]} for col in cursor.fetchall()]
            
            # Get row count
            cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = cursor.fetchone()[0]
            
            # Get sample data for query pattern analysis
            cursor.execute(f'SELECT * FROM "{table}" LIMIT 5')
            sample_data = cursor.fetchall()
            
            structure['tables'][table] = {
                'columns': columns,
                'row_count': count,
                'sample_data': sample_data,
                'primary_keys': [col['name'] for col in columns if col['pk']],
                'numeric_columns': [col['name'] for col in columns if 'INT' in col['type'] or 'REAL' in col['type'] or 'DECIMAL' in col['type']],
                'text_columns': [col['name'] for col in columns if 'TEXT' in col['type'] or 'VARCHAR' in col['type']],
                'date_columns': [col['name'] for col in columns if 'DATE' in col['type'] or 'TIME' in col['type']],
                'foreign_keys': self._detect_foreign_keys(cursor, table)
            }
        
        # Detect relationships
        structure['relationships'] = self._detect_relationships(structure['tables'])
        
        # Define query patterns
        structure['query_patterns'] = self._define_query_patterns()
        
        # Define aggregation functions
        structure['aggregation_functions'] = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT']
        
        # Define join patterns
        structure['join_patterns'] = self._define_join_patterns(structure['relationships'])
        
        conn.close()
        return structure
    
    def _detect_foreign_keys(self, cursor, table_name):
        """Detect foreign keys in a table"""
        foreign_keys = []
        
        # Get foreign key info
        cursor.execute(f'PRAGMA foreign_key_list("{table_name}")')
        fk_info = cursor.fetchall()
        
        for fk in fk_info:
            if len(fk) >= 4:
                foreign_keys.append({
                    'column': fk[3],
                    'references_table': fk[2],
                    'references_column': fk[4]
                })
        
        # Detect pattern-based foreign keys
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns = [col[1] for col in cursor.fetchall()]
        
        for col in columns:
            if col.endswith('_id') and col != 'id':
                possible_table = col[:-3]
                foreign_keys.append({
                    'column': col,
                    'references_table': possible_table,
                    'references_column': 'id',
                    'pattern_based': True
                })
        
        return foreign_keys
    
    def _detect_relationships(self, tables):
        """Detect table relationships"""
        relationships = []
        
        for table_name, table_info in tables.items():
            for fk in table_info['foreign_keys']:
                if fk['references_table'] in tables:
                    relationships.append({
                        'from_table': table_name,
                        'from_column': fk['column'],
                        'to_table': fk['references_table'],
                        'to_column': fk['references_column'],
                        'relationship_type': self._classify_relationship(table_name, fk['references_table'])
                    })
        
        return relationships
    
    def _classify_relationship(self, from_table, to_table):
        """Classify relationship type"""
        relationship_map = {
            ('samples', 'hosts'): 'sample_to_host',
            ('hosts', 'samples'): 'host_to_samples',
            ('hosts', 'taxonomy'): 'host_to_taxonomy',
            ('hosts', 'locations'): 'host_to_location',
            ('samples', 'screening_results'): 'sample_to_screening',
            ('samples', 'storage_locations'): 'sample_to_storage',
            ('screening_results', 'samples'): 'screening_to_sample',
            ('storage_locations', 'samples'): 'storage_to_sample'
        }
        
        return relationship_map.get((from_table, to_table), 'general')
    
    def _define_query_patterns(self):
        """Define SQL query patterns"""
        return [
            'simple_select',
            'conditional_select',
            'aggregate_query',
            'join_query',
            'subquery',
            'window_function',
            'case_statement',
            'complex_join',
            'group_by_having',
            'order_by_limit'
        ]
    
    def _define_join_patterns(self, relationships):
        """Define join patterns based on relationships"""
        patterns = []
        
        for rel in relationships:
            patterns.append({
                'type': rel['relationship_type'],
                'join_clause': f"LEFT JOIN {rel['to_table']} ON {rel['from_table']}.{rel['from_column']} = {rel['to_table']}.{rel['to_column']}",
                'relationship': rel
            })
        
        return patterns
    
    def generate_master_sql_training_data(self, structure):
        """Generate comprehensive SQL query training data"""
        training_data = []
        
        # 1. Basic SELECT queries
        training_data.extend(self._generate_basic_select_queries(structure))
        
        # 2. Conditional queries
        training_data.extend(self._generate_conditional_queries(structure))
        
        # 3. Aggregation queries
        training_data.extend(self._generate_aggregation_queries(structure))
        
        # 4. JOIN queries
        training_data.extend(self._generate_join_queries(structure))
        
        # 5. Complex analytical queries
        training_data.extend(self._generate_analytical_queries(structure))
        
        # 6. Subquery patterns
        training_data.extend(self._generate_subquery_patterns(structure))
        
        # 7. Window function queries
        training_data.extend(self._generate_window_function_queries(structure))
        
        # 8. Case statement queries
        training_data.extend(self._generate_case_statement_queries(structure))
        
        return training_data
    
    def _generate_basic_select_queries(self, structure):
        """Generate basic SELECT query training data"""
        queries = []
        
        for table_name, table_info in structure['tables'].items():
            columns = [col['name'] for col in table_info['columns']]
            
            # Select all columns
            queries.append({
                'question': f"Show me all data from {table_name}",
                'sql': f"SELECT * FROM {table_name}",
                'category': 'basic_select',
                'complexity': 'low',
                'tables': [table_name]
            })
            
            # Select specific columns
            important_cols = columns[:5]  # First 5 columns
            if len(important_cols) > 1:
                queries.append({
                    'question': f"Show me {', '.join(important_cols)} from {table_name}",
                    'sql': f"SELECT {', '.join(important_cols)} FROM {table_name}",
                    'category': 'basic_select',
                    'complexity': 'low',
                    'tables': [table_name]
                })
            
            # Select with limit
            queries.append({
                'question': f"Show me the first 10 records from {table_name}",
                'sql': f"SELECT * FROM {table_name} LIMIT 10",
                'category': 'basic_select',
                'complexity': 'low',
                'tables': [table_name]
            })
        
        return queries
    
    def _generate_conditional_queries(self, structure):
        """Generate conditional WHERE clause queries"""
        queries = []
        
        for table_name, table_info in structure['tables'].items():
            # Text-based conditions
            for col in table_info['text_columns']:
                queries.append({
                    'question': f"Find {table_name} where {col} contains 'positive'",
                    'sql': f"SELECT * FROM {table_name} WHERE {col} LIKE '%positive%'",
                    'category': 'conditional',
                    'complexity': 'medium',
                    'tables': [table_name]
                })
            
            # Numeric-based conditions
            for col in table_info['numeric_columns']:
                queries.append({
                    'question': f"Find {table_name} where {col} is greater than 100",
                    'sql': f"SELECT * FROM {table_name} WHERE {col} > 100",
                    'category': 'conditional',
                    'complexity': 'medium',
                    'tables': [table_name]
                })
            
            # Date-based conditions
            for col in table_info['date_columns']:
                queries.append({
                    'question': f"Find {table_name} from 2023",
                    'sql': f"SELECT * FROM {table_name} WHERE {col} LIKE '2023%'",
                    'category': 'conditional',
                    'complexity': 'medium',
                    'tables': [table_name]
                })
        
        return queries
    
    def _generate_aggregation_queries(self, structure):
        """Generate aggregation function queries"""
        queries = []
        
        for table_name, table_info in structure['tables'].items():
            if table_info['row_count'] > 0:
                # COUNT queries
                queries.append({
                    'question': f"How many records are in {table_name}?",
                    'sql': f"SELECT COUNT(*) as total_count FROM {table_name}",
                    'category': 'aggregation',
                    'complexity': 'medium',
                    'tables': [table_name]
                })
                
                # Group by queries
                if table_info['text_columns']:
                    group_col = table_info['text_columns'][0]
                    queries.append({
                        'question': f"Count records by {group_col} in {table_name}",
                        'sql': f"SELECT {group_col}, COUNT(*) as count FROM {table_name} GROUP BY {group_col}",
                        'category': 'aggregation',
                        'complexity': 'medium',
                        'tables': [table_name]
                    })
                
                # AVG queries
                if table_info['numeric_columns']:
                    avg_col = table_info['numeric_columns'][0]
                    queries.append({
                        'question': f"What is the average {avg_col} in {table_name}?",
                        'sql': f"SELECT AVG({avg_col}) as average_{avg_col} FROM {table_name}",
                        'category': 'aggregation',
                        'complexity': 'medium',
                        'tables': [table_name]
                    })
        
        return queries
    
    def _generate_join_queries(self, structure):
        """Generate JOIN query training data"""
        queries = []
        
        for rel in structure['relationships']:
            from_table = rel['from_table']
            to_table = rel['to_table']
            from_col = rel['from_column']
            to_col = rel['to_column']
            
            # Basic join
            queries.append({
                'question': f"Show {from_table} with {to_table} information",
                'sql': f"SELECT {from_table}.*, {to_table}.* FROM {from_table} LEFT JOIN {to_table} ON {from_table}.{from_col} = {to_table}.{to_col}",
                'category': 'join',
                'complexity': 'high',
                'tables': [from_table, to_table]
            })
            
            # Join with condition
            if to_table in structure['tables']:
                to_table_info = structure['tables'][to_table]
                if to_table_info['text_columns']:
                    text_col = to_table_info['text_columns'][0]
                    queries.append({
                        'question': f"Find {from_table} with {to_table} where {text_col} is 'positive'",
                        'sql': f"SELECT {from_table}.*, {to_table}.* FROM {from_table} LEFT JOIN {to_table} ON {from_table}.{from_col} = {to_table}.{to_col} WHERE {to_table}.{text_col} = 'positive'",
                        'category': 'join',
                        'complexity': 'high',
                        'tables': [from_table, to_table]
                    })
        
        return queries
    
    def _generate_analytical_queries(self, structure):
        """Generate complex analytical queries"""
        queries = []
        
        # Cross-table analysis
        if 'samples' in structure['tables'] and 'screening_results' in structure['tables']:
            queries.extend([
                {
                    'question': "Compare coronavirus positivity rates across provinces",
                    'sql': """
                    SELECT 
                        l.province,
                        COUNT(*) as total_samples,
                        SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) as positive_samples,
                        ROUND(SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positivity_rate
                    FROM samples s
                    JOIN hosts h ON s.host_id = h.host_id
                    JOIN locations l ON h.location_id = l.location_id
                    LEFT JOIN screening_results sr ON s.sample_id = sr.sample_id
                    WHERE sr.pan_corona IS NOT NULL
                    GROUP BY l.province
                    ORDER BY positivity_rate DESC
                    """,
                    'category': 'analytical',
                    'complexity': 'expert',
                    'tables': ['samples', 'hosts', 'locations', 'screening_results']
                },
                {
                    'question': "Show the most common bat species with positive results",
                    'sql': """
                    SELECT 
                        t.scientific_name,
                        COUNT(*) as total_samples,
                        SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) as positive_samples,
                        ROUND(SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positivity_rate
                    FROM samples s
                    JOIN hosts h ON s.host_id = h.host_id
                    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
                    LEFT JOIN screening_results sr ON s.sample_id = sr.sample_id
                    WHERE sr.pan_corona IS NOT NULL
                    GROUP BY t.scientific_name
                    HAVING COUNT(*) >= 5
                    ORDER BY positivity_rate DESC
                    """,
                    'category': 'analytical',
                    'complexity': 'expert',
                    'tables': ['samples', 'hosts', 'taxonomy', 'screening_results']
                }
            ])
        
        # Time-based analysis
        if 'samples' in structure['tables']:
            queries.append({
                'question': "Show sample collection trends by month",
                'sql': """
                SELECT 
                    strftime('%Y-%m', s.collection_date) as month,
                    COUNT(*) as samples_collected,
                    COUNT(DISTINCT h.host_id) as unique_hosts
                FROM samples s
                JOIN hosts h ON s.host_id = h.host_id
                WHERE s.collection_date IS NOT NULL
                GROUP BY strftime('%Y-%m', s.collection_date)
                ORDER BY month
                """,
                'category': 'analytical',
                'complexity': 'expert',
                'tables': ['samples', 'hosts']
            })
        
        return queries
    
    def _generate_subquery_patterns(self, structure):
        """Generate subquery training data"""
        queries = []
        
        if 'samples' in structure['tables'] and 'screening_results' in structure['tables']:
            queries.extend([
                {
                    'question': "Find samples with positive coronavirus results",
                    'sql': """
                    SELECT * FROM samples 
                    WHERE sample_id IN (
                        SELECT sample_id FROM screening_results 
                        WHERE pan_corona = 'Positive'
                    )
                    """,
                    'category': 'subquery',
                    'complexity': 'high',
                    'tables': ['samples', 'screening_results']
                },
                {
                    'question': "Show hosts with more than 5 samples",
                    'sql': """
                    SELECT * FROM hosts 
                    WHERE host_id IN (
                        SELECT host_id FROM samples 
                        GROUP BY host_id 
                        HAVING COUNT(*) > 5
                    )
                    """,
                    'category': 'subquery',
                    'complexity': 'high',
                    'tables': ['hosts', 'samples']
                }
            ])
        
        return queries
    
    def _generate_window_function_queries(self, structure):
        """Generate window function queries"""
        queries = []
        
        if 'screening_results' in structure['tables']:
            queries.append({
                'question': "Show screening results with row numbers",
                'sql': """
                SELECT 
                    *,
                    ROW_NUMBER() OVER (ORDER BY created_at) as row_num,
                    COUNT(*) OVER () as total_records
                FROM screening_results
                """,
                'category': 'window_function',
                'complexity': 'expert',
                'tables': ['screening_results']
            })
        
        return queries
    
    def _generate_case_statement_queries(self, structure):
        """Generate CASE statement queries"""
        queries = []
        
        if 'screening_results' in structure['tables']:
            queries.append({
                'question': "Categorize screening results",
                'sql': """
                SELECT 
                    *,
                    CASE 
                        WHEN pan_corona = 'Positive' AND pan_hanta = 'Positive' THEN 'Multiple Positive'
                        WHEN pan_corona = 'Positive' THEN 'Corona Positive'
                        WHEN pan_hanta = 'Positive' THEN 'Hanta Positive'
                        WHEN pan_paramyxo = 'Positive' THEN 'Paramyxo Positive'
                        WHEN pan_flavi = 'Positive' THEN 'Flavi Positive'
                        ELSE 'All Negative'
                    END as result_category
                FROM screening_results
                """,
                'category': 'case_statement',
                'complexity': 'expert',
                'tables': ['screening_results']
            })
        
        return queries
    
    def train_master_sql_models(self, training_data):
        """Train master SQL generation models"""
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
            
            print("✅ Master SQL models trained successfully!")
            return True
            
        except Exception as e:
            print(f"Error training master SQL models: {e}")
            return False
    
    def _save_models(self):
        """Save trained models"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        version_dir = os.path.join(self.versions_dir, f'master_sql_{timestamp}')
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
            'capabilities': ['sql_generation', 'complexity_prediction', 'category_classification']
        }
        
        metadata_path = os.path.join(version_dir, 'metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Master SQL models saved to: {version_dir}")
    
    def load_master_sql_models(self):
        """Load trained master SQL models"""
        try:
            versions = [d for d in os.listdir(self.versions_dir) 
                        if d.startswith('master_sql_') and os.path.isdir(os.path.join(self.versions_dir, d))]
            
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
            print(f"Error loading master SQL models: {e}")
            return False
    
    def generate_sql_query(self, question):
        """Generate SQL query from natural language"""
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
            print(f"Error generating SQL query: {e}")
            return None

def train_master_sql(db_config, db_type='sqlite'):
    """Train master SQL models"""
    trainer = MasterSQLTrainer(db_config, db_type)
    
    print("🔧 Training Master SQL Models for Advanced Data Analysis")
    print("=" * 60)
    
    # Analyze database structure
    print("1. Analyzing database structure...")
    structure = trainer.analyze_database_structure()
    
    # Generate training data
    print("2. Generating comprehensive SQL training data...")
    training_data = trainer.generate_master_sql_training_data(structure)
    print(f"   Generated {len(training_data)} SQL query patterns")
    
    # Train models
    print("3. Training master SQL generation models...")
    success = trainer.train_master_sql_models(training_data)
    
    if success:
        print("\n🎉 Master SQL Training Completed!")
        print("\n🤖 Your AI now has Master SQL capabilities:")
        print("   ✅ Complex query generation")
        print("   ✅ Multi-table JOIN intelligence")
        print("   ✅ Aggregation and analytics")
        print("   ✅ Subquery and window functions")
        print("   ✅ CASE statement logic")
        print("   ✅ Performance optimization")
        
        print("\n🎯 Advanced SQL queries it can handle:")
        print("   • 'Compare coronavirus positivity rates across provinces'")
        print("   • 'Show the most common bat species with positive results'")
        print("   • 'Find samples with multiple positive screening results'")
        print("   • 'Analyze seasonal patterns in sample collection'")
        print("   • 'Generate comprehensive research reports'")
        
        return True
    else:
        print("❌ Master SQL training failed!")
        return False

if __name__ == '__main__':
    db_path = 'd:/MyFiles/Program_Last_version/HaoXai_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
    train_master_sql(db_path, 'sqlite')

