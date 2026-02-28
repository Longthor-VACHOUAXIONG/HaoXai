"""
ML Routes for HaoXai
Machine Learning prediction endpoints and UI
"""
from flask import Blueprint, render_template, request, jsonify, session
import flask
import os
import sys
import pickle
import numpy as np
import pandas as pd
import sqlite3
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
from sklearn.metrics import accuracy_score, mean_squared_error
from sklearn.preprocessing import LabelEncoder

ml_bp = Blueprint('ml', __name__, url_prefix='/ml')

@ml_bp.route('/')
def ml_dashboard():
    """ML Dashboard page"""
    return render_template('ml_dashboard.html')

@ml_bp.route('/chat/tables', methods=['GET'])
def get_database_tables():
    """Get all tables from the connected database"""
    try:
        # Connect to database
        db_path = session.get('db_path')
        if not db_path:
            return jsonify({
                'success': False,
                'message': 'No database connected'
            }), 400
        
        conn = sqlite3.connect(db_path)
        
        # Get all table names
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        tables_df = pd.read_sql_query(query, conn)
        
        conn.close()
        
        # Filter out system tables and get user tables
        tables = tables_df['name'].tolist()
        
        # Sort tables alphabetically
        tables.sort()
        
        return jsonify({
            'success': True,
            'tables': tables,
            'count': len(tables)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Failed to load tables: {str(e)}'
        }), 500

# Chat-specific ML endpoints
@ml_bp.route('/chat/train-auto', methods=['POST'])
def train_chat_model_auto():
    """Train ML model automatically - no user input required"""
    try:
        data = request.get_json()
        model_type = data.get('model_type', 'classification')
        
        # Connect to database
        db_path = session.get('db_path')
        if not db_path:
            return jsonify({
                'success': False,
                'message': 'No database connected'
            }), 400
        
        conn = sqlite3.connect(db_path)
        
        # Get all tables
        tables_query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        tables_df = pd.read_sql_query(tables_query, conn)
        tables = tables_df['name'].tolist()
        
        if not tables:
            return jsonify({
                'success': False,
                'message': 'No tables found in database'
            }), 400
        
        # Train models on ALL tables in the database
        models_trained = []
        failed_tables = []
        
        for table in tables:
            try:
                # Get table schema and sample data
                schema_query = f"PRAGMA table_info({table})"
                schema_df = pd.read_sql_query(schema_query, conn)
                columns = schema_df['name'].tolist()
                
                # Load sample data to determine data types
                sample_query = f"SELECT * FROM {table} LIMIT 100"
                df = pd.read_sql_query(sample_query, conn)
                
                # Filter suitable features (exclude id, timestamps, and columns with too many unique values)
                suitable_features = []
                for col in columns:
                    if col in ['id', 'created_at', 'updated_at', 'timestamp', 'date']:
                        continue
                    
                    # Check if column has reasonable data for ML
                    unique_count = df[col].nunique()
                    null_count = df[col].isnull().sum()
                    
                    # Skip columns with too many unique values (likely IDs) or too many nulls
                    if unique_count > len(df) * 0.8 or null_count > len(df) * 0.5:
                        continue
                    
                    suitable_features.append(col)
                
                if len(suitable_features) < 2:
                    failed_tables.append(f"{table}: Insufficient features")
                    continue
                
                # Basic data preprocessing
                df_clean = df[suitable_features].dropna()
                
                if len(df_clean) < 10:
                    failed_tables.append(f"{table}: Insufficient clean data")
                    continue
                
                # Auto-select target variable for supervised learning
                target_variable = None
                if model_type in ['classification', 'regression']:
                    # Find a good target variable (categorical for classification, numeric for regression)
                    for col in suitable_features:
                        if model_type == 'classification' and df_clean[col].dtype == 'object':
                            if 2 <= df_clean[col].nunique() <= 10:  # Good for classification
                                target_variable = col
                                break
                        elif model_type == 'regression' and df_clean[col].dtype in ['int64', 'float64']:
                            target_variable = col
                            break
                
                # Prepare features
                features = [col for col in suitable_features if col != target_variable]
                X = df_clean[features]
                
                # Handle categorical variables
                label_encoders = {}
                for col in X.select_dtypes(include=['object']).columns:
                    le = LabelEncoder()
                    X[col] = le.fit_transform(X[col].astype(str))
                    label_encoders[col] = le
                
                # Train model based on type
                if model_type == 'classification':
                    if not target_variable:
                        # Create a synthetic target if none found
                        target_variable = features[0]
                        y = (X[target_variable] > X[target_variable].median()).astype(int)
                    else:
                        y = df_clean[target_variable]
                        if y.dtype == 'object':
                            y_le = LabelEncoder()
                            # Handle None/NaN values by converting to string first
                            y_clean = y.astype(str).fillna('Unknown')
                            y_le.fit(y_clean)
                            y = y_le.transform(y_clean)
                            label_encoders['target'] = y_le
                    
                    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
                    
                    model = RandomForestClassifier(n_estimators=100, random_state=42)
                    model.fit(X_train, y_train)
                    
                    y_pred = model.predict(X_test)
                    accuracy = accuracy_score(y_test, y_pred)
                    
                    metrics = {
                        'accuracy': accuracy,
                        'samples': len(df_clean),
                        'features': len(features),
                        'test_samples': len(X_test),
                        'target_variable': target_variable
                    }
                    
                elif model_type == 'regression':
                    if not target_variable:
                        # Use first numeric feature as target
                        numeric_features = [col for col in features if X[col].dtype in ['int64', 'float64']]
                        if numeric_features:
                            target_variable = numeric_features[0]
                            features.remove(target_variable)
                            X = X[features]
                            y = X[target_variable]
                        else:
                            failed_tables.append(f"{table}: No suitable numeric target")
                            continue
                    else:
                        y = df_clean[target_variable]
                    
                    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
                    
                    model = RandomForestRegressor(n_estimators=100, random_state=42)
                    model.fit(X_train, y_train)
                    
                    y_pred = model.predict(X_test)
                    mse = mean_squared_error(y_test, y_pred)
                    rmse = np.sqrt(mse)
                    
                    metrics = {
                        'mse': mse,
                        'rmse': rmse,
                        'samples': len(df_clean),
                        'features': len(features),
                        'test_samples': len(X_test),
                        'target_variable': target_variable
                    }
                    
                elif model_type == 'clustering':
                    model = KMeans(n_clusters=min(3, len(X)), random_state=42)
                    model.fit(X)
                    
                    labels = model.labels_
                    metrics = {
                        'clusters': len(np.unique(labels)),
                        'samples': len(df_clean),
                        'features': len(features),
                        'inertia': model.inertia_
                    }
                    
                elif model_type == 'anomaly_detection':
                    model = IsolationForest(contamination=0.1, random_state=42)
                    model.fit(X)
                    
                    predictions = model.predict(X)
                    anomalies = sum(1 for p in predictions if p == -1)
                    
                    metrics = {
                        'anomalies': anomalies,
                        'anomaly_rate': anomalies / len(df_clean),
                        'samples': len(df_clean),
                        'features': len(features)
                    }
                
                # Store model info (without sklearn objects for JSON serialization)
                model_info = {
                    'table': table,
                    'model_type': model_type,
                    'features': features,
                    'target_variable': target_variable,
                    'label_encoders_count': len(label_encoders),
                    'metrics': metrics
                }
                models_trained.append(model_info)
                
                # Store full model data with sklearn objects for pickle (separate from JSON response)
                full_model_data = {
                    'table': table,
                    'model': model,
                    'model_type': model_type,
                    'features': features,
                    'target_variable': target_variable,
                    'label_encoders': label_encoders,
                    'metrics': metrics
                }
                # Add to session storage list
                if 'full_ml_models' not in session:
                    session['full_ml_models'] = []
                full_models = session.get('full_ml_models', [])
                full_models.append(pickle.dumps(full_model_data))
                session['full_ml_models'] = full_models
                
            except Exception as e:
                failed_tables.append(f"{table}: {str(e)}")
                continue
        
        conn.close()
        
        if not models_trained:
            return jsonify({
                'success': False,
                'message': f'No models could be trained. Failed tables: {"; ".join(failed_tables)}'
            }), 400
        
        return jsonify({
            'success': True,
            'message': f'Trained {len(models_trained)} {model_type} models successfully',
            'models_trained': len(models_trained),
            'failed_tables': len(failed_tables),
            'model_summaries': [
                {
                    'table': model['table'],
                    'accuracy': model['metrics'].get('accuracy', 'N/A'),
                    'samples': model['metrics']['samples'],
                    'features': model['metrics']['features']
                } for model in models_trained
            ],
            'details': models_trained
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Auto-training failed: {str(e)}'
        }), 500

@ml_bp.route('/chat/predict', methods=['POST'])
def ml_predict():
    """Make predictions using trained ML models"""
    try:
        # Check if this is being called from chat endpoint with direct data
        if hasattr(flask.request, 'data') and 'query' in flask.request.data:
            query = flask.request.data['query'].lower()
            print(f"DEBUG: ML Prediction request received (direct): {query}")
        else:
            data = request.get_json()
            query = data.get('query', '').lower()
            print(f"DEBUG: ML Prediction request received (json): {query}")
        
        # Check if we have trained models
        if 'full_ml_models' not in session:
            return jsonify({
                'success': False,
                'answer': 'No trained models available. Please train models first.'
            })
        
        # Load trained models from session
        full_models = session.get('full_ml_models', [])
        if not full_models:
            return jsonify({
                'success': False,
                'answer': 'No trained models available. Please train models first.'
            })
        
        print(f"DEBUG: Found {len(full_models)} trained models")
        
        # Find the requested table in the query
        table_name = None
        model = None
        features = None
        target_variable = None
        label_encoders = None
        metrics = None
        
        for model_pickle in full_models:
            model_data = pickle.loads(model_pickle)
            current_table = model_data['table'].lower()
            print(f"DEBUG: Checking table: {current_table}")
            
            # Check if this table is mentioned in the query
            if current_table in query:
                print(f"DEBUG: Found matching table: {current_table}")
                table_name = model_data['table']
                model = model_data['model']
                features = model_data['features']
                target_variable = model_data['target_variable']
                label_encoders = model_data['label_encoders']
                metrics = model_data['metrics']
                break
        
        if not table_name:
            available_tables = [pickle.loads(m)['table'] for m in full_models]
            print(f"DEBUG: No match found. Available tables: {available_tables}")
            return jsonify({
                'success': False,
                'answer': f'Could not find a trained model for the requested table. Available models: ' + 
                          ', '.join(available_tables)
            })
        
        print(f"DEBUG: Using table {table_name} for predictions")
        
        # Get database connection
        db_path = session.get('db_path')
        if not db_path:
            return jsonify({
                'success': False,
                'answer': 'No database connected.'
            })
        
        conn = sqlite3.connect(db_path)
        
        # Load some sample data for prediction
        sample_query = f"SELECT {', '.join(features)} FROM {table_name} LIMIT 5"
        print(f"DEBUG: Running query: {sample_query}")
        sample_df = pd.read_sql_query(sample_query, conn)
        conn.close()
        
        if len(sample_df) == 0:
            return jsonify({
                'success': False,
                'answer': f'No data available in {table_name} table for predictions.'
            })
        
        print(f"DEBUG: Loaded {len(sample_df)} samples for prediction")
        
        # Prepare data for prediction (same preprocessing as training)
        X_pred = sample_df.copy()
        
        # Apply label encoders
        for col, le in label_encoders.items():
            if col in X_pred.columns and col != 'target':  # Skip target variable encoder
                # Handle unseen labels by converting to string and mapping unknown values
                X_pred[col] = X_pred[col].astype(str).fillna('Unknown')
                
                # Check for unseen values
                unique_values = set(X_pred[col].unique())
                le_classes = set(le.classes_)
                unseen_values = unique_values - le_classes
                
                if unseen_values:
                    print(f"DEBUG: Unseen values in {col}: {unseen_values}")
                    # Map unseen values to the most common class or first class
                    fallback_class = list(le_classes)[0] if le_classes else 'Unknown'
                    X_pred[col] = X_pred[col].apply(lambda x: fallback_class if x not in le_classes else x)
                
                X_pred[col] = le.transform(X_pred[col])
        
        # Make predictions
        try:
            predictions = model.predict(X_pred)
            print(f"DEBUG: Made {len(predictions)} predictions")
            
            # Format results
            result_text = f"🤖 **ML Predictions from {table_name.title()} Table**\n\n"
            result_text += f"Model Type: {model_data['model_type'].title()}\n"
            result_text += f"Features Used: {', '.join(features)}\n"
            result_text += f"Target Variable: {target_variable or 'None'}\n"
            result_text += f"Training Accuracy: {metrics.get('accuracy', 'N/A')}\n\n"
            
            result_text += "**Sample Predictions:**\n"
            for i, (idx, row) in enumerate(sample_df.iterrows()):
                if i >= 3:  # Show only first 3 predictions
                    break
                result_text += f"Sample {i+1}: {predictions[i]}\n"
                result_text += f"  Features: {dict(row)}\n\n"
            
            result_text += f"*Model trained on {metrics['samples']} samples with {metrics['features']} features.*"
            
            return jsonify({
                'success': True,
                'answer': result_text
            })
            
        except Exception as e:
            print(f"DEBUG: Prediction error: {str(e)}")
            return jsonify({
                'success': False,
                'answer': f'Prediction failed: {str(e)}'
            })
            
    except Exception as e:
        print(f"DEBUG: General error: {str(e)}")
        return jsonify({
            'success': False,
            'answer': f'Prediction error: {str(e)}'
        })

def get_ml_features():
    """Get available features for ML training from database table"""
    try:
        data = request.get_json()
        table_name = data.get('table', 'samples')
        
        # Connect to database
        db_path = session.get('db_path')
        if not db_path:
            return jsonify({
                'success': False,
                'message': 'No database connected'
            }), 400
        
        conn = sqlite3.connect(db_path)
        
        # Get column names and sample data
        query = f"PRAGMA table_info({table_name})"
        columns_df = pd.read_sql_query(query, conn)
        
        # Get sample data to determine column types
        sample_query = f"SELECT * FROM {table_name} LIMIT 5"
        sample_df = pd.read_sql_query(sample_query, conn)
        
        conn.close()
        
        # Filter out non-numeric columns for ML features
        numeric_columns = []
        for col in columns_df['name']:
            if col not in ['id', 'created_at', 'updated_at'] and sample_df[col].dtype in ['int64', 'float64', 'object']:
                numeric_columns.append(col)
        
        return jsonify({
            'success': True,
            'features': numeric_columns,
            'sample_data': sample_df.to_dict('records')[:3]
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Failed to load features: {str(e)}'
        }), 500

@ml_bp.route('/chat/train', methods=['POST'])
def train_chat_model():
    """Train ML model for chat interface"""
    try:
        data = request.get_json()
        model_type = data.get('model_type', 'classification')
        data_source = data.get('data_source', 'samples')
        target_variable = data.get('target_variable')
        features = data.get('features', [])
        test_size = data.get('test_size', 0.2)
        
        if not features:
            return jsonify({
                'success': False,
                'message': 'No features selected'
            }), 400
        
        # Connect to database
        db_path = session.get('db_path')
        if not db_path:
            return jsonify({
                'success': False,
                'message': 'No database connected'
            }), 400
        
        conn = sqlite3.connect(db_path)
        
        # Load data
        query = f"SELECT {', '.join(features + ([target_variable] if target_variable else []))} FROM {data_source}"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Basic data preprocessing
        df = df.dropna()
        
        if len(df) < 10:
            return jsonify({
                'success': False,
                'message': 'Insufficient data for training (need at least 10 samples)'
            }), 400
        
        # Simple model training based on type
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score, mean_squared_error
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import LabelEncoder
        
        X = df[features]
        
        # Handle categorical variables
        label_encoders = {}
        for col in X.select_dtypes(include=['object']).columns:
            le = LabelEncoder()
            X[col] = le.fit_transform(X[col].astype(str))
            label_encoders[col] = le
        
        if model_type == 'classification':
            if not target_variable:
                return jsonify({
                    'success': False,
                    'message': 'Target variable required for classification'
                }), 400
            
            y = df[target_variable]
            if y.dtype == 'object':
                y_le = LabelEncoder()
                y = y_le.fit_transform(y.astype(str))
                label_encoders['target'] = y_le
            
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
            
            model = RandomForestClassifier(n_estimators=100, random_state=42)
            model.fit(X_train, y_train)
            
            y_pred = model.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            
            metrics = {
                'accuracy': accuracy,
                'samples': len(df),
                'features': len(features),
                'test_samples': len(X_test)
            }
            
        elif model_type == 'regression':
            if not target_variable:
                return jsonify({
                    'success': False,
                    'message': 'Target variable required for regression'
                }), 400
            
            y = df[target_variable]
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
            
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(X_train, y_train)
            
            y_pred = model.predict(X_test)
            mse = mean_squared_error(y_test, y_pred)
            rmse = np.sqrt(mse)
            
            metrics = {
                'mse': mse,
                'rmse': rmse,
                'samples': len(df),
                'features': len(features),
                'test_samples': len(X_test)
            }
            
        elif model_type == 'clustering':
            model = KMeans(n_clusters=3, random_state=42)
            model.fit(X)
            
            labels = model.labels_
            metrics = {
                'clusters': len(np.unique(labels)),
                'samples': len(df),
                'features': len(features),
                'inertia': model.inertia_
            }
            
        elif model_type == 'anomaly_detection':
            # Simple anomaly detection using isolation forest
            from sklearn.ensemble import IsolationForest
            
            model = IsolationForest(contamination=0.1, random_state=42)
            model.fit(X)
            
            predictions = model.predict(X)
            anomalies = sum(1 for p in predictions if p == -1)
            
            metrics = {
                'anomalies': anomalies,
                'anomaly_rate': anomalies / len(df),
                'samples': len(df),
                'features': len(features)
            }
        
        # Save model (in production, use proper model storage)
        import pickle
        model_data = {
            'model': model,
            'model_type': model_type,
            'features': features,
            'target_variable': target_variable,
            'label_encoders': label_encoders,
            'metrics': metrics
        }
        
        # For demo, store in session (in production, use database)
        session['current_ml_model'] = pickle.dumps(model_data)
        
        return jsonify({
            'success': True,
            'message': f'{model_type.title()} model trained successfully',
            'model': {
                'type': model_type,
                'features': features,
                'target': target_variable
            },
            'metrics': metrics
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Training failed: {str(e)}'
        }), 500

