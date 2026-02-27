"""
ML Model Training Module for ViroDB AI Chat
Enables training custom models on database data for improved AI responses
"""
import os
import json
import pickle
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder
import re
import warnings
warnings.filterwarnings('ignore')

class DatabaseTrainer:
    """Train ML models on database data for intelligent chat responses"""
    
    def __init__(self, db_config, db_type='sqlite', models_dir='models'):
        self.db_config = db_config
        self.db_type = db_type
        self.models_dir = models_dir
        self.vectorizer = None
        self.models = {}
        
        # Create models directory if it doesn't exist
        os.makedirs(models_dir, exist_ok=True)
        
        # Create versions subdirectory for model versioning
        self.versions_dir = os.path.join(models_dir, 'versions')
        os.makedirs(self.versions_dir, exist_ok=True)
        
    def get_connection(self):
        """Get database connection"""
        from database.db_manager_flask import DatabaseManagerFlask
        return DatabaseManagerFlask.get_connection(self.db_config, self.db_type)
    
    def collect_training_data(self):
        """Collect training data from database for question-answer pairs"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get database schema
            schema = self._get_schema(cursor)
            
            training_data = []
            
            # Generate training examples from each table
            for table_name, table_info in schema.items():
                if table_name.lower() in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin']:
                    continue
                
                # Get sample data from table
                sample_data = self._get_table_samples(cursor, table_name, limit=20)
                
                for row in sample_data:
                    # Generate question-answer pairs from this data
                    qa_pairs = self._generate_qa_pairs(table_name, row, table_info)
                    training_data.extend(qa_pairs)
            
            conn.close()
            
            # Also add schema-related training data
            schema_training = self._generate_schema_training(schema)
            training_data.extend(schema_training)
            
            print(f"Collected {len(training_data)} training examples")
            return training_data
            
        except Exception as e:
            print(f"Error collecting training data: {e}")
            return []
    
    def _get_schema(self, cursor):
        """Get database schema information"""
        schema = {}
        
        try:
            # Get all tables
            if self.db_type == 'sqlite':
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            else:
                cursor.execute("SHOW TABLES")
            
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{table}")')
                    columns = [{'name': row[1], 'type': row[2]} for row in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{table}`')
                    columns = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
                
                # Get row count
                if self.db_type == 'sqlite':
                    cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                else:
                    cursor.execute(f'SELECT COUNT(*) FROM `{table}`')
                count = cursor.fetchone()[0]
                
                schema[table] = {'columns': columns, 'row_count': count}
            
        except Exception as e:
            print(f"Error getting schema: {e}")
        
        return schema
    
    def _get_table_samples(self, cursor, table_name, limit=20):
        """Get sample data from a table"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            cursor.execute(f'SELECT * FROM {q}{table_name}{q} LIMIT {limit}')
            rows = cursor.fetchall()
            
            if self.db_type == 'sqlite':
                cursor.execute(f'PRAGMA table_info("{table_name}")')
                columns = [c[1] for c in cursor.fetchall()]
            else:
                cursor.execute(f'DESCRIBE `{table_name}`')
                columns = [c[0] for c in cursor.fetchall()]
            
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            print(f"Error getting samples from {table_name}: {e}")
            return []
    
    def _generate_qa_pairs(self, table_name, row_data, table_info):
        """Generate question-answer pairs from table data"""
        qa_pairs = []
        
        try:
            # Get important columns
            columns = [col['name'] for col in table_info['columns']]
            important_cols = self._get_important_columns(columns)
            
            # Generate different types of questions
            row_id = row_data.get('id') or row_data.get('sample_id') or row_data.get(columns[0])
            
            # 1. Find by ID questions (enhanced for sample patterns)
            if row_id:
                # Add multiple variations for sample IDs
                qa_pairs.append({
                    'question': f"Find information for {row_id}",
                    'answer': self._format_row_answer(row_data, important_cols),
                    'category': 'find_by_id',
                    'table': table_name
                })
                
                # Add specific sample ID patterns
                if 'sample' in table_name.lower() or 'sample_id' in row_data:
                    qa_pairs.append({
                        'question': f"{row_id}",
                        'answer': self._format_row_answer(row_data, important_cols),
                        'category': 'find_by_id',
                        'table': table_name
                    })
                    
                    qa_pairs.append({
                        'question': f"Show sample {row_id}",
                        'answer': self._format_row_answer(row_data, important_cols),
                        'category': 'find_by_id',
                        'table': table_name
                    })
            
            # 2. Count questions
            qa_pairs.append({
                'question': f"How many records in {table_name}?",
                'answer': f"There are {table_info['row_count']} records in {table_name}.",
                'category': 'count',
                'table': table_name
            })
            
            # 3. Field-specific questions
            for col in important_cols[:3]:  # Limit to top 3 columns
                if col in row_data and row_data[col]:
                    value = row_data[col]
                    qa_pairs.append({
                        'question': f"What is the {col.replace('_', ' ')} for {row_id}?",
                        'answer': f"The {col.replace('_', ' ')} for {row_id} is {value}.",
                        'category': 'field_query',
                        'table': table_name
                    })
            
            # 4. Filter questions
            if row_data:
                filter_col = important_cols[0] if important_cols else columns[0]
                filter_val = row_data.get(filter_col)
                if filter_val:
                    qa_pairs.append({
                        'question': f"Show records where {filter_col.replace('_', ' ')} is {filter_val}",
                        'answer': f"Found records in {table_name} where {filter_col.replace('_', ' ')} is {filter_val}.",
                        'category': 'filter',
                        'table': table_name
                    })
            
        except Exception as e:
            print(f"Error generating QA pairs for {table_name}: {e}")
        
        return qa_pairs
    
    def _generate_schema_training(self, schema):
        """Generate training data for schema-related questions"""
        training = []
        
        try:
            # Table structure questions
            for table_name, table_info in schema.items():
                columns = [col['name'] for col in table_info['columns']]
                
                training.append({
                    'question': f"What columns are in {table_name}?",
                    'answer': f"The {table_name} table has these columns: {', '.join(columns)}.",
                    'category': 'schema',
                    'table': table_name
                })
                
                training.append({
                    'question': f"Show table structure for {table_name}",
                    'answer': f"{table_name} table structure: {len(columns)} columns, {table_info['row_count']} rows. Columns: {', '.join(columns)}.",
                    'category': 'schema',
                    'table': table_name
                })
            
            # General schema questions
            all_tables = list(schema.keys())
            training.append({
                'question': "What tables are available in the database?",
                'answer': f"The database contains these tables: {', '.join(all_tables)}.",
                'category': 'schema',
                'table': 'general'
            })
            
            training.append({
                'question': "Explain the database schema",
                'answer': f"The database has {len(all_tables)} tables: {', '.join(all_tables)}. Each table contains specific data related to virology research.",
                'category': 'schema',
                'table': 'general'
            })
            
        except Exception as e:
            print(f"Error generating schema training: {e}")
        
        return training
    
    def _get_important_columns(self, columns):
        """Get important columns for generating questions"""
        important = []
        
        # Priority columns
        priority_patterns = ['id', 'name', 'code', 'sample', 'host', 'location', 'date', 'result', 'type', 'status']
        
        for col in columns:
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in priority_patterns):
                important.append(col)
        
        # Add more columns if needed
        if len(important) < 5:
            for col in columns:
                if col not in important:
                    important.append(col)
                    if len(important) >= 5:
                        break
        
        return important[:5]  # Return top 5
    
    def _format_row_answer(self, row_data, important_cols):
        """Format row data as a readable answer"""
        parts = []
        
        for col in important_cols:
            if col in row_data and row_data[col]:
                display_name = col.replace('_', ' ').title()
                parts.append(f"{display_name}: {row_data[col]}")
        
        return " | ".join(parts) if parts else "Data found"
    
    def train_intent_classifier(self, training_data):
        """Train an intent classifier for better question understanding"""
        try:
            if not training_data:
                print("No training data available")
                return False
            
            # Prepare training data
            questions = [item['question'] for item in training_data]
            categories = [item['category'] for item in training_data]
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                questions, categories, test_size=0.2, random_state=42, stratify=categories
            )
            
            # Create pipeline
            pipeline = Pipeline([
                ('tfidf', TfidfVectorizer(
                    max_features=5000,
                    ngram_range=(1, 2),
                    stop_words='english'
                )),
                ('classifier', MultinomialNB())
            ])
            
            # Train model
            pipeline.fit(X_train, y_train)
            
            # Evaluate
            y_pred = pipeline.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            
            print(f"Intent classifier trained with accuracy: {accuracy:.3f}")
            
            # Save model
            model_path = os.path.join(self.models_dir, 'intent_classifier.pkl')
            with open(model_path, 'wb') as f:
                pickle.dump(pipeline, f)
            
            self.models['intent_classifier'] = pipeline
            return True
            
        except Exception as e:
            print(f"Error training intent classifier: {e}")
            return False
    
    def train_table_classifier(self, training_data):
        """Train a table classifier to predict which table to query"""
        try:
            if not training_data:
                return False
            
            # Prepare training data
            questions = [item['question'] for item in training_data]
            tables = [item['table'] for item in training_data]
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                questions, tables, test_size=0.2, random_state=42
            )
            
            # Create pipeline
            pipeline = Pipeline([
                ('tfidf', TfidfVectorizer(
                    max_features=3000,
                    ngram_range=(1, 2),
                    stop_words='english'
                )),
                ('classifier', RandomForestClassifier(n_estimators=100, random_state=42))
            ])
            
            # Train model
            pipeline.fit(X_train, y_train)
            
            # Evaluate
            y_pred = pipeline.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            
            print(f"Table classifier trained with accuracy: {accuracy:.3f}")
            
            # Save model
            model_path = os.path.join(self.models_dir, 'table_classifier.pkl')
            with open(model_path, 'wb') as f:
                pickle.dump(pipeline, f)
            
            self.models['table_classifier'] = pipeline
            return True
            
        except Exception as e:
            print(f"Error training table classifier: {e}")
            return False
    
    def train_response_generator(self, training_data):
        """Train a response generator for common question patterns"""
        try:
            if not training_data:
                return False
            
            # Group training data by category
            category_responses = {}
            for item in training_data:
                category = item['category']
                if category not in category_responses:
                    category_responses[category] = []
                category_responses[category].append(item)
            
            # Save response templates
            templates_path = os.path.join(self.models_dir, 'response_templates.json')
            with open(templates_path, 'w') as f:
                json.dump(category_responses, f, indent=2)
            
            print(f"Saved {len(category_responses)} response templates")
            return True
            
        except Exception as e:
            print(f"Error training response generator: {e}")
            return False
    
    def train_all_models(self):
        """Train all models on the current database with versioning"""
        try:
            print("Starting model training...")
            
            # Collect training data
            training_data = self.collect_training_data()
            
            if not training_data:
                print("No training data available. Cannot train models.")
                return False
            
            # Create new version
            version_id = self._create_new_version()
            
            # Train different models
            results = {
                'intent_classifier': self.train_intent_classifier(training_data),
                'table_classifier': self.train_table_classifier(training_data),
                'response_generator': self.train_response_generator(training_data)
            }
            
            if all(results.values()):
                # Save models to version directory
                self._save_models_to_version(version_id)
                
                # Update current models
                self._update_current_models()
                
                # Save training metadata
                metadata = {
                    'version_id': version_id,
                    'training_date': datetime.now().isoformat(),
                    'db_type': self.db_type,
                    'training_examples': len(training_data),
                    'models_trained': sum(results.values()),
                    'categories': list(set(item['category'] for item in training_data)),
                    'tables': list(set(item['table'] for item in training_data)),
                    'performance': self._evaluate_models(training_data)
                }
                
                metadata_path = os.path.join(self.models_dir, 'training_metadata.json')
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                print(f"Training completed. Models trained: {sum(results.values())}/3 (Version: {version_id})")
                return True
            else:
                print("Training failed for some models")
                return False
            
        except Exception as e:
            print(f"Error in training process: {e}")
            return False
    
    def _create_new_version(self):
        """Create a new version directory"""
        version_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        version_dir = os.path.join(self.versions_dir, version_id)
        os.makedirs(version_dir, exist_ok=True)
        
        # Save version info
        version_info = {
            'version_id': version_id,
            'created_at': datetime.now().isoformat(),
            'db_type': self.db_type,
            'db_config_hash': self._hash_db_config()
        }
        
        with open(os.path.join(version_dir, 'version_info.json'), 'w') as f:
            json.dump(version_info, f, indent=2)
        
        return version_id
    
    def _hash_db_config(self):
        """Create a hash of the database configuration for tracking"""
        import hashlib
        config_str = str(self.db_config) + str(self.db_type)
        return hashlib.md5(config_str.encode()).hexdigest()[:8]
    
    def _save_models_to_version(self, version_id):
        """Save trained models to version directory"""
        version_dir = os.path.join(self.versions_dir, version_id)
        
        for model_name, model in self.models.items():
            if model_name == 'response_templates':
                # Save JSON templates
                model_path = os.path.join(version_dir, f"{model_name}.json")
                with open(model_path, 'w') as f:
                    json.dump(model, f, indent=2)
            else:
                # Save pickle models
                model_path = os.path.join(version_dir, f"{model_name}.pkl")
                with open(model_path, 'wb') as f:
                    pickle.dump(model, f)
    
    def _update_current_models(self):
        """Update current models with latest versions"""
        # Get latest version
        versions = self.get_model_versions()
        if versions:
            latest_version = versions[0]  # Most recent first
            self.load_models_from_version(latest_version['version_id'])
    
    def _evaluate_models(self, training_data):
        """Evaluate model performance on training data"""
        performance = {}
        
        try:
            # Split data for evaluation
            questions = [item['question'] for item in training_data]
            intents = [item['category'] for item in training_data]
            tables = [item['table'] for item in training_data]
            
            # Evaluate intent classifier
            if 'intent_classifier' in self.models:
                intent_predictions = self.models['intent_classifier'].predict(questions)
                intent_accuracy = accuracy_score(intents, intent_predictions)
                performance['intent_accuracy'] = intent_accuracy
            
            # Evaluate table classifier
            if 'table_classifier' in self.models:
                table_predictions = self.models['table_classifier'].predict(questions)
                table_accuracy = accuracy_score(tables, table_predictions)
                performance['table_accuracy'] = table_accuracy
            
        except Exception as e:
            print(f"Error evaluating models: {e}")
            performance['error'] = str(e)
        
        return performance
    
    def get_model_versions(self):
        """Get list of all model versions"""
        versions = []
        
        try:
            if os.path.exists(self.versions_dir):
                for version_name in sorted(os.listdir(self.versions_dir), reverse=True):
                    version_path = os.path.join(self.versions_dir, version_name)
                    if os.path.isdir(version_path):
                        version_info_path = os.path.join(version_path, 'version_info.json')
                        if os.path.exists(version_info_path):
                            with open(version_info_path, 'r') as f:
                                version_info = json.load(f)
                                versions.append(version_info)
        except Exception as e:
            print(f"Error getting model versions: {e}")
        
        return versions
    
    def load_models_from_version(self, version_id):
        """Load models from a specific version"""
        try:
            version_dir = os.path.join(self.versions_dir, version_id)
            
            if not os.path.exists(version_dir):
                print(f"Version {version_id} not found")
                return False
            
            loaded_models = 0
            
            # Load intent classifier
            intent_path = os.path.join(version_dir, 'intent_classifier.pkl')
            if os.path.exists(intent_path):
                with open(intent_path, 'rb') as f:
                    self.models['intent_classifier'] = pickle.load(f)
                loaded_models += 1
            
            # Load table classifier
            table_path = os.path.join(version_dir, 'table_classifier.pkl')
            if os.path.exists(table_path):
                with open(table_path, 'rb') as f:
                    self.models['table_classifier'] = pickle.load(f)
                loaded_models += 1
            
            # Load response templates
            templates_path = os.path.join(version_dir, 'response_templates.json')
            if os.path.exists(templates_path):
                with open(templates_path, 'r') as f:
                    self.models['response_templates'] = json.load(f)
                loaded_models += 1
            
            print(f"Loaded {loaded_models} models from version {version_id}")
            return loaded_models > 0
            
        except Exception as e:
            print(f"Error loading models from version {version_id}: {e}")
            return False
    
    def rollback_to_version(self, version_id):
        """Rollback to a specific model version"""
        try:
            if self.load_models_from_version(version_id):
                # Update current models to this version
                self._update_current_models()
                print(f"Successfully rolled back to version {version_id}")
                return True
            else:
                print(f"Failed to rollback to version {version_id}")
                return False
        except Exception as e:
            print(f"Error rolling back to version {version_id}: {e}")
            return False
    
    def delete_version(self, version_id):
        """Delete a specific model version"""
        try:
            version_dir = os.path.join(self.versions_dir, version_id)
            
            if not os.path.exists(version_dir):
                print(f"Version {version_id} not found")
                return False
            
            # Remove version directory
            import shutil
            shutil.rmtree(version_dir)
            
            print(f"Deleted version {version_id}")
            return True
            
        except Exception as e:
            print(f"Error deleting version {version_id}: {e}")
            return False
    
    def get_version_info(self, version_id):
        """Get detailed information about a specific version"""
        try:
            version_dir = os.path.join(self.versions_dir, version_id)
            version_info_path = os.path.join(version_dir, 'version_info.json')
            
            if os.path.exists(version_info_path):
                with open(version_info_path, 'r') as f:
                    version_info = json.load(f)
                
                # Add model files info
                model_files = []
                for file in os.listdir(version_dir):
                    if file.endswith(('.pkl', '.json')) and file != 'version_info.json':
                        file_path = os.path.join(version_dir, file)
                        stat = os.stat(file_path)
                        model_files.append({
                            'name': file,
                            'size': stat.st_size,
                            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                        })
                
                version_info['model_files'] = model_files
                return version_info
            else:
                return None
                
        except Exception as e:
            print(f"Error getting version info for {version_id}: {e}")
            return None
    
    def load_models(self):
        """Load trained models"""
        try:
            models_loaded = 0
            
            # Load intent classifier
            intent_path = os.path.join(self.models_dir, 'intent_classifier.pkl')
            if os.path.exists(intent_path):
                with open(intent_path, 'rb') as f:
                    self.models['intent_classifier'] = pickle.load(f)
                models_loaded += 1
            
            # Load table classifier
            table_path = os.path.join(self.models_dir, 'table_classifier.pkl')
            if os.path.exists(table_path):
                with open(table_path, 'rb') as f:
                    self.models['table_classifier'] = pickle.load(f)
                models_loaded += 1
            
            # Load response templates
            templates_path = os.path.join(self.models_dir, 'response_templates.json')
            if os.path.exists(templates_path):
                with open(templates_path, 'r') as f:
                    self.models['response_templates'] = json.load(f)
                models_loaded += 1
            
            print(f"Loaded {models_loaded} trained models")
            return models_loaded > 0
            
        except Exception as e:
            print(f"Error loading models: {e}")
            return False
    
    def predict_intent(self, question):
        """Predict the intent of a user question"""
        try:
            if 'intent_classifier' not in self.models:
                return None
            
            classifier = self.models['intent_classifier']
            intent = classifier.predict([question])[0]
            confidence = max(classifier.predict_proba([question])[0])
            
            return {
                'intent': intent,
                'confidence': confidence
            }
            
        except Exception as e:
            print(f"Error predicting intent: {e}")
            return None
    
    def predict_table(self, question):
        """Predict which table to query for a question"""
        try:
            if 'table_classifier' not in self.models:
                return None
            
            classifier = self.models['table_classifier']
            table = classifier.predict([question])[0]
            confidence = max(classifier.predict_proba([question])[0])
            
            return {
                'table': table,
                'confidence': confidence
            }
            
        except Exception as e:
            print(f"Error predicting table: {e}")
            return None
    
    def get_training_status(self):
        """Get the status of trained models"""
        try:
            metadata_path = os.path.join(self.models_dir, 'training_metadata.json')
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    return json.load(f)
            return None
        except Exception as e:
            print(f"Error getting training status: {e}")
            return None
    
    def get_model_info(self):
        """Get information about available models"""
        info = {
            'models_available': [],
            'training_status': self.get_training_status(),
            'models_dir': self.models_dir
        }
        
        for model_name in ['intent_classifier', 'table_classifier', 'response_templates']:
            model_path = os.path.join(self.models_dir, f"{model_name}.pkl" if model_name != 'response_templates' else f"{model_name}.json")
            if os.path.exists(model_path):
                info['models_available'].append(model_name)
        
        return info
