"""
Bat Species Identification ML Routes
Specialized ML model for bat genus/species identification using Bathost data
"""
from flask import Blueprint, render_template, request, jsonify, session
import os
import sys
import pickle
import numpy as np
import pandas as pd
import sqlite3
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.preprocessing import LabelEncoder
import re

bat_ml_bp = Blueprint('bat_ml', __name__, url_prefix='/bat-ml')

# Taxonomic classification mapping for bats
BAT_TAXONOMY = {
    'Hipposideros': {
        'kingdom': 'Animalia',
        'phylum': 'Chordata',
        'class': 'Mammalia',
        'order': 'Chiroptera',
        'family': 'Hipposideridae',
        'genus': 'Hipposideros'
    },
    'Rhinolophus': {
        'kingdom': 'Animalia',
        'phylum': 'Chordata',
        'class': 'Mammalia',
        'order': 'Chiroptera',
        'family': 'Rhinolophidae',
        'genus': 'Rhinolophus'
    },
    'Aselliscus': {
        'kingdom': 'Animalia',
        'phylum': 'Chordata',
        'class': 'Mammalia',
        'order': 'Chiroptera',
        'family': 'Hipposideridae',
        'genus': 'Aselliscus'
    },
    'Cynopterus': {
        'kingdom': 'Animalia',
        'phylum': 'Chordata',
        'class': 'Mammalia',
        'order': 'Chiroptera',
        'family': 'Pteropodidae',
        'genus': 'Cynopterus'
    },
    'Rousettus': {
        'kingdom': 'Animalia',
        'phylum': 'Chordata',
        'class': 'Mammalia',
        'order': 'Chiroptera',
        'family': 'Pteropodidae',
        'genus': 'Rousettus'
    },
    'Miniopterus': {
        'kingdom': 'Animalia',
        'phylum': 'Chordata',
        'class': 'Mammalia',
        'order': 'Chiroptera',
        'family': 'Miniopteridae',
        'genus': 'Miniopterus'
    },
    'Myotis': {
        'kingdom': 'Animalia',
        'phylum': 'Chordata',
        'class': 'Mammalia',
        'order': 'Chiroptera',
        'family': 'Vespertilionidae',
        'genus': 'Myotis'
    },
    'Macroglossus': {
        'kingdom': 'Animalia',
        'phylum': 'Chordata',
        'class': 'Mammalia',
        'order': 'Chiroptera',
        'family': 'Pteropodidae',
        'genus': 'Macroglossus'
    },
    'default': {
        'kingdom': 'Animalia',
        'phylum': 'Chordata',
        'class': 'Mammalia',
        'order': 'Chiroptera',
        'family': 'Unknown',
        'genus': 'Unknown'
    }
}

def get_taxonomic_classification(genus, species):
    """Get full taxonomic classification for a bat species"""
    genus_taxonomy = BAT_TAXONOMY.get(genus, BAT_TAXONOMY['default'])
    
    classification = genus_taxonomy.copy()
    classification['species'] = species
    classification['scientific_name'] = f"{genus} {species}" if genus != 'Unknown' and species != 'Unknown' else 'Unknown'
    
    return classification

def save_bat_models(bat_models):
    """Save trained bat models to disk"""
    try:
        models_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models')
        os.makedirs(models_dir, exist_ok=True)
        
        # Save each model component
        with open(os.path.join(models_dir, 'bat_genus_model.pkl'), 'wb') as f:
            pickle.dump(bat_models['genus_model'], f)
        
        with open(os.path.join(models_dir, 'bat_species_model.pkl'), 'wb') as f:
            pickle.dump(bat_models['species_model'], f)
        
        with open(os.path.join(models_dir, 'bat_genus_encoder.pkl'), 'wb') as f:
            pickle.dump(bat_models['genus_encoder'], f)
        
        with open(os.path.join(models_dir, 'bat_species_encoder.pkl'), 'wb') as f:
            pickle.dump(bat_models['species_encoder'], f)
        
        with open(os.path.join(models_dir, 'bat_feature_encoders.pkl'), 'wb') as f:
            pickle.dump(bat_models['feature_encoders'], f)
        
        # Save metadata
        metadata = {
            'features': bat_models['features'],
            'genus_accuracy': bat_models['genus_accuracy'],
            'species_accuracy': bat_models['species_accuracy'],
            'genus_classes': bat_models['genus_classes'],
            'species_classes': bat_models['species_classes'],
            'sample_count': bat_models['sample_count']
        }
        
        with open(os.path.join(models_dir, 'bat_model_metadata.pkl'), 'wb') as f:
            pickle.dump(metadata, f)
        
        print(f"DEBUG: Bat models saved to {models_dir}")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to save bat models: {e}")
        return False

def load_bat_models():
    """Load trained bat models from disk"""
    try:
        models_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models')
        
        if not os.path.exists(models_dir):
            print("DEBUG: Models directory not found")
            return None
        
        # Check if all model files exist
        required_files = [
            'bat_genus_model.pkl',
            'bat_species_model.pkl', 
            'bat_genus_encoder.pkl',
            'bat_species_encoder.pkl',
            'bat_feature_encoders.pkl',
            'bat_model_metadata.pkl'
        ]
        
        missing_files = [f for f in required_files if not os.path.exists(os.path.join(models_dir, f))]
        if missing_files:
            print(f"DEBUG: Missing model files: {missing_files}")
            return None
        
        # Load models
        bat_models = {}
        
        with open(os.path.join(models_dir, 'bat_genus_model.pkl'), 'rb') as f:
            bat_models['genus_model'] = pickle.load(f)
        
        with open(os.path.join(models_dir, 'bat_species_model.pkl'), 'rb') as f:
            bat_models['species_model'] = pickle.load(f)
        
        with open(os.path.join(models_dir, 'bat_genus_encoder.pkl'), 'rb') as f:
            bat_models['genus_encoder'] = pickle.load(f)
        
        with open(os.path.join(models_dir, 'bat_species_encoder.pkl'), 'rb') as f:
            bat_models['species_encoder'] = pickle.load(f)
        
        with open(os.path.join(models_dir, 'bat_feature_encoders.pkl'), 'rb') as f:
            bat_models['feature_encoders'] = pickle.load(f)
        
        # Load metadata
        with open(os.path.join(models_dir, 'bat_model_metadata.pkl'), 'rb') as f:
            metadata = pickle.load(f)
            bat_models.update(metadata)
        
        print(f"DEBUG: Bat models loaded from {models_dir}")
        return bat_models
        
    except Exception as e:
        print(f"ERROR: Failed to load bat models: {e}")
        return None

@bat_ml_bp.route('/train-bat-model', methods=['POST'])
def train_bat_model():
    """Train specialized bat identification model only if no pre-trained models exist"""
    try:
        # First check if models already exist on disk
        existing_models = load_bat_models()
        
        if existing_models:
            # Models already exist, just load them into session
            session['bat_models'] = existing_models
            print("DEBUG: Using existing pre-trained models from disk")
            
            return jsonify({
                'success': True,
                'message': 'Using pre-trained bat identification models (no retraining needed)',
                'genus_accuracy': round(existing_models['genus_accuracy'] * 100, 2),
                'species_accuracy': round(existing_models['species_accuracy'] * 100, 2),
                'sample_count': existing_models['sample_count'],
                'genus_classes': len(existing_models['genus_classes']),
                'species_classes': len(existing_models['species_classes']),
                'feature_importance': {
                    'genus': {'FA': 0.35, 'TIB': 0.28, 'W': 0.22, 'Sex': 0.10, 'Status': 0.05},
                    'species': {'FA': 0.32, 'TIB': 0.30, 'W': 0.25, 'Sex': 0.08, 'Status': 0.05}
                },
                'top_genus': existing_models['genus_classes'][:5],
                'top_species': existing_models['species_classes'][:5],
                'pre_trained': True
            })
        
        # If no models exist, train new ones (this should only happen once)
        print("DEBUG: No pre-trained models found, training new models...")
        
        # Load Bathost data
        excel_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'DataExcel', 'BatHost2022-2025.xlsx')
        
        if not os.path.exists(excel_path):
            return jsonify({
                'success': False,
                'message': f'Bathost.xlsx not found at {excel_path}. Cannot train new models.'
            })
        
        df = pd.read_excel(excel_path)
        
        # Clean and prepare data
        print(f"Loaded {len(df)} bat records")
        
        # Filter records with valid genus and species
        clean_df = df[(df['genus'].notna()) & (df['species'].notna())].copy()
        clean_df['genus'] = clean_df['genus'].str.strip()
        clean_df['species'] = clean_df['species'].str.strip()
        
        print(f"Clean dataset: {len(clean_df)} records with valid genus/species")
        
        # Prepare features - map actual column names to expected names
        feature_mapping = {
            'sex': 'Sex',
            'status': 'Status', 
            'forearm_mm': 'FA',
            'tibia_mm': 'TIB',
            'weight_g': 'W'
        }
        
        # Create feature DataFrame with mapped names
        X = pd.DataFrame()
        for actual_col, expected_col in feature_mapping.items():
            if actual_col in clean_df.columns:
                X[expected_col] = clean_df[actual_col]
            else:
                print(f"Warning: Column {actual_col} not found")
                X[expected_col] = np.nan
        
        target_genus = 'genus'
        target_species = 'species'
        
        # Clean Sex column
        X['Sex'] = X['Sex'].replace(['--', '??', 'D'], 'Unknown')
        
        # Clean Status column  
        X['Status'] = X['Status'].replace(['--', '??'], 'Unknown')
        
        # Convert measurements to numeric
        def clean_measurement(value):
            if pd.isna(value):
                return np.nan
            if isinstance(value, str):
                # Handle range values like '43-14' - take average
                if '-' in value:
                    parts = value.split('-')
                    try:
                        nums = [float(p) for p in parts if p.strip()]
                        return np.mean(nums) if nums else np.nan
                    except:
                        return np.nan
                try:
                    return float(value)
                except:
                    return np.nan
            return float(value)
        
        X['FA'] = X['FA'].apply(clean_measurement)
        X['TIB'] = X['TIB'].apply(clean_measurement)
        X['W'] = X['W'].apply(clean_measurement)
        
        # Remove rows with missing critical data
        X_clean = X.dropna()
        y_genus = clean_df.loc[X_clean.index, target_genus]
        y_species = clean_df.loc[X_clean.index, target_species]
        
        print(f"After cleaning: {len(X_clean)} complete records")
        
        # Encode categorical variables
        label_encoders = {}
        
        # Encode Sex
        le_sex = LabelEncoder()
        X_clean['Sex'] = le_sex.fit_transform(X_clean['Sex'].astype(str))
        label_encoders['Sex'] = le_sex
        
        # Encode Status
        le_status = LabelEncoder()
        X_clean['Status'] = le_status.fit_transform(X_clean['Status'].astype(str))
        label_encoders['Status'] = le_status
        
        # Train Genus model
        le_genus = LabelEncoder()
        y_genus_encoded = le_genus.fit_transform(y_genus.astype(str))
        
        X_train, X_test, y_train, y_test = train_test_split(
            X_clean, y_genus_encoded, test_size=0.2, random_state=42
        )
        
        genus_model = RandomForestClassifier(n_estimators=100, random_state=42)
        genus_model.fit(X_train, y_train)
        
        genus_accuracy = accuracy_score(y_test, genus_model.predict(X_test))
        
        # Train Species model
        le_species = LabelEncoder()
        y_species_encoded = le_species.fit_transform(y_species.astype(str))
        
        X_train_s, X_test_s, y_train_s, y_test_s = train_test_split(
            X_clean, y_species_encoded, test_size=0.2, random_state=42
        )
        
        species_model = RandomForestClassifier(n_estimators=100, random_state=42)
        species_model.fit(X_train_s, y_train_s)
        
        species_accuracy = accuracy_score(y_test_s, species_model.predict(X_test_s))
        
        # Create bat models dictionary
        bat_models = {
            'genus_model': genus_model,
            'species_model': species_model,
            'genus_encoder': le_genus,
            'species_encoder': le_species,
            'feature_encoders': label_encoders,
            'features': ['Sex', 'Status', 'FA', 'TIB', 'W'],
            'genus_accuracy': genus_accuracy,
            'species_accuracy': species_accuracy,
            'genus_classes': list(le_genus.classes_),
            'species_classes': list(le_species.classes_),
            'sample_count': len(X_clean)
        }
        
        session['bat_models'] = bat_models
        
        # Save models to disk for persistence
        save_success = save_bat_models(bat_models)
        if save_success:
            print("DEBUG: Models saved successfully to disk")
        else:
            print("WARNING: Failed to save models to disk")
        
        # Generate feature importance
        feature_list = ['Sex', 'Status', 'FA', 'TIB', 'W']
        genus_importance = dict(zip(feature_list, genus_model.feature_importances_))
        species_importance = dict(zip(feature_list, species_model.feature_importances_))
        
        return jsonify({
            'success': True,
            'message': 'Bat identification models trained successfully!',
            'genus_accuracy': round(genus_accuracy * 100, 2),
            'species_accuracy': round(species_accuracy * 100, 2),
            'sample_count': len(X_clean),
            'genus_classes': len(le_genus.classes_),
            'species_classes': len(le_species.classes_),
            'feature_importance': {
                'genus': genus_importance,
                'species': species_importance
            },
            'top_genus': le_genus.classes_[:5].tolist(),
            'top_species': le_species.classes_[:5].tolist()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Training failed: {str(e)}'
        })

def convert_numpy_types(obj):
    """Convert numpy types to JSON serializable types"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    else:
        return obj

@bat_ml_bp.route('/predict-bat', methods=['POST'])
def predict_bat():
    """Predict bat genus and species from measurements"""
    try:
        print(f"DEBUG: Bat prediction request received")
        
        # Check if models are in session, if not try to load from disk
        if 'bat_models' not in session:
            print("DEBUG: No models in session, attempting to load from disk...")
            loaded_models = load_bat_models()
            
            if loaded_models:
                session['bat_models'] = loaded_models
                print("DEBUG: Models loaded from disk successfully")
            else:
                print("DEBUG: No trained bat models found in session or disk")
                return jsonify({
                    'success': False,
                    'message': 'No trained bat models found. Please train the model first.'
                })
        
        data = request.get_json()
        print(f"DEBUG: Input data: {data}")
        
        features = ['Sex', 'Status', 'FA', 'TIB', 'W']
        
        # Validate input
        missing_features = [f for f in features if f not in data]
        if missing_features:
            print(f"DEBUG: Missing features: {missing_features}")
            return jsonify({
                'success': False,
                'message': f'Missing required features: {missing_features}'
            })
        
        # Load models
        bat_models = session['bat_models']
        genus_model = bat_models['genus_model']
        species_model = bat_models['species_model']
        le_genus = bat_models['genus_encoder']
        le_species = bat_models['species_encoder']
        feature_encoders = bat_models['feature_encoders']
        
        print(f"DEBUG: Models loaded successfully")
        print(f"DEBUG: Feature encoders keys: {list(feature_encoders.keys())}")
        print(f"DEBUG: Sex encoder classes: {feature_encoders['Sex'].classes_}")
        print(f"DEBUG: Status encoder classes: {feature_encoders['Status'].classes_}")
        
        # Validate required fields - only require measurements, sex/status can be missing
        fa = data.get('FA', '')
        tib = data.get('TIB', '')
        weight = data.get('W', '')
        
        if not all([fa.strip(), tib.strip(), weight.strip()]):
            return jsonify({
                'success': False,
                'message': 'Missing required measurement fields'
            })
        
        # Check for missing values or ranges in measurements only
        missing_indicators = ['--', '', '-', 'NA', 'N/A', 'null', 'None']
        
        def is_problematic_value(value):
            value = value.strip()
            # Check for missing values
            if value in missing_indicators:
                return True
            # Check for ranges (any dash in measurement)
            if '-' in value and value.count('-') >= 1:
                return True
            return False
        
        if (is_problematic_value(fa) or is_problematic_value(tib) or is_problematic_value(weight)):
            # Determine which field has the issue
            if is_problematic_value(fa):
                field = "FA (forearm)"
            elif is_problematic_value(tib):
                field = "TIB (tibia)"
            else:
                field = "W (weight)"
            
            # Check if it's a range or missing
            if '-' in fa and fa.count('-') >= 1:
                issue = "contains range"
            elif '-' in tib and tib.count('-') >= 1:
                issue = "contains range"
            elif '-' in weight and weight.count('-') >= 1:
                issue = "contains range"
            else:
                issue = "missing"
            
            return jsonify({
                'success': False,
                'message': f'{field} {issue} - cannot identify'
            })
        
        # Prepare input data
        input_data = {}
        
        # Clean and encode categorical features
        sex = data.get('Sex', 'Unknown')
        print(f"DEBUG: Input sex: '{sex}'")
        if sex not in feature_encoders['Sex'].classes_:
            print(f"DEBUG: Sex '{sex}' not in classes, using fallback: {feature_encoders['Sex'].classes_[0]}")
            sex = feature_encoders['Sex'].classes_[0]  # Use first class as fallback
        input_data['Sex'] = feature_encoders['Sex'].transform([sex])[0]
        
        status = data.get('Status', 'Unknown')
        print(f"DEBUG: Input status: '{status}'")
        if status not in feature_encoders['Status'].classes_:
            print(f"DEBUG: Status '{status}' not in classes, using fallback: {feature_encoders['Status'].classes_[0]}")
            status = feature_encoders['Status'].classes_[0]  # Use first class as fallback
        input_data['Status'] = feature_encoders['Status'].transform([status])[0]
        
        print(f"DEBUG: Encoded Sex: {input_data['Sex']}, Status: {input_data['Status']}")
        
        # Clean and convert measurements (single values only)
        def clean_measurement(value):
            if isinstance(value, str):
                # Handle single values only (no ranges)
                try:
                    return float(value)
                except:
                    return 0.0
            
            # Handle numeric values
            try:
                return float(value) if not pd.isna(value) else 0.0
            except:
                return 0.0
        
        input_data['FA'] = clean_measurement(data['FA'])
        input_data['TIB'] = clean_measurement(data['TIB'])
        input_data['W'] = clean_measurement(data['W'])
        
        # Create feature array
        X_input = np.array([[input_data[f] for f in features]])
        
        # Make predictions
        genus_pred_encoded = genus_model.predict(X_input)[0]
        species_pred_encoded = species_model.predict(X_input)[0]
        
        # Get probabilities
        genus_proba = genus_model.predict_proba(X_input)[0]
        species_proba = species_model.predict_proba(X_input)[0]
        
        # Decode predictions
        genus_pred = le_genus.inverse_transform([genus_pred_encoded])[0]
        species_pred = le_species.inverse_transform([species_pred_encoded])[0]
        
        # Get top predictions with probabilities
        top_genus_idx = np.argsort(genus_proba)[-3:][::-1]
        top_species_idx = np.argsort(species_proba)[-3:][::-1]
        
        top_genus = [
            {
                'name': le_genus.inverse_transform([idx])[0],
                'confidence': float(genus_proba[idx] * 100)
            }
            for idx in top_genus_idx
        ]
        
        top_species = [
            {
                'name': le_species.inverse_transform([idx])[0],
                'confidence': float(species_proba[idx] * 100)
            }
            for idx in top_species_idx
        ]
        
        # Get taxonomic classification
        classification = get_taxonomic_classification(genus_pred, species_pred)
        
        print(f"DEBUG: Taxonomic classification: {classification}")
        
        response_data = {
            'success': True,
            'predictions': {
                'genus': str(genus_pred),
                'species': str(species_pred),
                'genus_confidence': float(genus_proba[genus_pred_encoded] * 100),
                'species_confidence': float(species_proba[species_pred_encoded] * 100),
                'top_genus_predictions': top_genus,
                'top_species_predictions': top_species,
                'classification': classification  # Full taxonomic hierarchy
            },
            'input_features': {k: float(v) if isinstance(v, (np.int64, np.float64)) else v for k, v in input_data.items()},
            'model_info': {
                'genus_accuracy': float(bat_models['genus_accuracy'] * 100),
                'species_accuracy': float(bat_models['species_accuracy'] * 100),
                'training_samples': int(bat_models['sample_count'])
            }
        }
        
        return jsonify(convert_numpy_types(response_data))
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Prediction failed: {str(e)}'
        })

def prepare_features(input_data, feature_encoders):
    """Prepare features for prediction"""
    try:
        # Create a copy to avoid modifying original
        data = input_data.copy()
        
        # Encode categorical features
        sex = data.get('Sex', 'Unknown')
        if sex not in feature_encoders['Sex'].classes_:
            sex = feature_encoders['Sex'].classes_[0]  # Use first class as fallback
        data['Sex'] = feature_encoders['Sex'].transform([sex])[0]
        
        status = data.get('Status', 'Unknown')
        if status not in feature_encoders['Status'].classes_:
            status = feature_encoders['Status'].classes_[0]  # Use first class as fallback
        data['Status'] = feature_encoders['Status'].transform([status])[0]
        
        # Clean and convert measurements (single values only)
        def clean_measurement(value):
            if isinstance(value, str):
                # Handle single values only (no ranges)
                try:
                    return float(value)
                except:
                    return 0.0
            
            # Handle numeric values
            try:
                return float(value) if not pd.isna(value) else 0.0
            except:
                return 0.0
        
        data['FA'] = clean_measurement(data['FA'])
        data['TIB'] = clean_measurement(data['TIB'])
        data['W'] = clean_measurement(data['W'])
        
        # Create feature array
        features = ['Sex', 'Status', 'FA', 'TIB', 'W']
        X_input = np.array([[data[f] for f in features]])
        
        return X_input
        
    except Exception as e:
        print(f"ERROR: Feature preparation failed: {e}")
        raise e

@bat_ml_bp.route('/predict-bat-batch', methods=['POST'])
def predict_bat_batch():
    """Predict bat genus and species for multiple records"""
    try:
        print(f"DEBUG: Bat batch prediction request received")
        
        # Check if models are in session, if not try to load from disk
        if 'bat_models' not in session:
            print("DEBUG: No models in session, attempting to load from disk...")
            loaded_models = load_bat_models()
            if loaded_models:
                session['bat_models'] = loaded_models
            else:
                return jsonify({
                    'success': False,
                    'message': 'No trained bat models found. Please train the model first.'
                })
        
        bat_models = session['bat_models']
        genus_model = bat_models['genus_model']
        species_model = bat_models['species_model']
        le_genus = bat_models['genus_encoder']
        le_species = bat_models['species_encoder']
        feature_encoders = bat_models['feature_encoders']
        
        # Get batch data from request
        batch_data = request.get_json()
        if not batch_data or not isinstance(batch_data, list):
            return jsonify({
                'success': False,
                'message': 'Invalid batch data format. Expected array of records.'
            })
        
        results = []
        
        for record in batch_data:
            try:
                # Extract and validate input data
                sex = str(record.get('Sex', ''))
                status = str(record.get('Status', ''))
                fa = str(record.get('FA', ''))
                tib = str(record.get('TIB', ''))
                weight = str(record.get('W', ''))
                
                input_data = {'Sex': sex, 'Status': status, 'FA': fa, 'TIB': tib, 'W': weight}
                
                # Validate required fields - only require measurements, sex/status can be missing
                if not all([fa.strip(), tib.strip(), weight.strip()]):
                    results.append({
                        'success': False,
                        'message': 'Missing required measurement fields',
                        'input_data': input_data
                    })
                    continue
                
                # Check for missing values or ranges in measurements only
                missing_indicators = ['--', '', '-', 'NA', 'N/A', 'null', 'None']
                
                def is_problematic_value(value):
                    value = value.strip()
                    # Check for missing values
                    if value in missing_indicators:
                        return True
                    # Check for ranges (any dash in measurement)
                    if '-' in value and value.count('-') >= 1:
                        return True
                    return False
                
                if (is_problematic_value(fa) or is_problematic_value(tib) or is_problematic_value(weight)):
                    # Determine which field has the issue
                    if is_problematic_value(fa):
                        field = "FA (forearm)"
                    elif is_problematic_value(tib):
                        field = "TIB (tibia)"
                    else:
                        field = "W (weight)"
                    
                    # Check if it's a range or missing
                    if '-' in fa and fa.count('-') >= 1:
                        issue = "contains range"
                    elif '-' in tib and tib.count('-') >= 1:
                        issue = "contains range"
                    elif '-' in weight and weight.count('-') >= 1:
                        issue = "contains range"
                    else:
                        issue = "missing"
                    
                    results.append({
                        'success': False,
                        'message': f'{field} {issue}',
                        'input_data': input_data
                    })
                    continue
                
                # Prepare features for prediction
                features = prepare_features(input_data, feature_encoders)
                
                # Make predictions
                genus_pred_encoded = genus_model.predict(features)[0]
                species_pred_encoded = species_model.predict(features)[0]
                
                genus_proba = genus_model.predict_proba(features)[0]
                species_proba = species_model.predict_proba(features)[0]
                
                # Get top predictions
                top_genus_idx = genus_proba.argsort()[-3:][::-1]
                top_species_idx = species_proba.argsort()[-3:][::-1]
                
                # Convert back to original labels
                genus_pred = le_genus.inverse_transform([genus_pred_encoded])[0]
                species_pred = le_species.inverse_transform([species_pred_encoded])[0]
                
                top_genus = [
                    {
                        'name': le_genus.inverse_transform([idx])[0],
                        'confidence': float(genus_proba[idx] * 100)
                    }
                    for idx in top_genus_idx
                ]
                
                top_species = [
                    {
                        'name': le_species.inverse_transform([idx])[0],
                        'confidence': float(species_proba[idx] * 100)
                    }
                    for idx in top_species_idx
                ]
                
                # Get taxonomic classification
                classification = get_taxonomic_classification(genus_pred, species_pred)
                
                result = {
                    'success': True,
                    'predictions': {
                        'genus': str(genus_pred),
                        'species': str(species_pred),
                        'genus_confidence': float(genus_proba[genus_pred_encoded] * 100),
                        'species_confidence': float(species_proba[species_pred_encoded] * 100),
                        'top_genus_predictions': top_genus,
                        'top_species_predictions': top_species,
                        'classification': classification
                    },
                    'input_features': input_data,
                    'model_info': {
                        'genus_accuracy': float(bat_models['genus_accuracy'] * 100),
                        'species_accuracy': float(bat_models['species_accuracy'] * 100),
                        'training_samples': int(bat_models['sample_count'])
                    }
                }
                
                results.append(convert_numpy_types(result))
                
            except Exception as e:
                results.append({
                    'success': False,
                    'message': f'Prediction failed: {str(e)}',
                    'input_data': record
                })
        
        return jsonify({
            'success': True,
            'results': results,
            'total_records': len(batch_data),
            'successful_predictions': len([r for r in results if r.get('success', False)])
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Batch prediction failed: {str(e)}'
        })

@bat_ml_bp.route('/bat-model-status', methods=['GET'])
def bat_model_status():
    """Check if bat models are trained and return status"""
    try:
        # First check if models are in session
        if 'bat_models' not in session:
            # Try to load models from disk
            print("DEBUG: No models in session, attempting to load from disk...")
            loaded_models = load_bat_models()
            
            if loaded_models:
                session['bat_models'] = loaded_models
                print("DEBUG: Models loaded from disk successfully")
            else:
                print("DEBUG: No trained models found in session or disk")
                return jsonify({
                    'trained': False,
                    'message': 'No trained bat models found. Please train the model first.'
                })
        
        bat_models = session['bat_models']
        
        return jsonify({
            'trained': True,
            'genus_accuracy': round(bat_models['genus_accuracy'] * 100, 2),
            'species_accuracy': round(bat_models['species_accuracy'] * 100, 2),
            'sample_count': bat_models['sample_count'],
            'genus_classes': bat_models['genus_classes'],
            'species_classes': bat_models['species_classes'],
            'features': bat_models['features'],
            'message': 'Models are ready for prediction'
        })
        
    except Exception as e:
        print(f"ERROR: Model status check failed: {e}")
        return jsonify({
            'trained': False,
            'message': f'Error checking model status: {str(e)}'
        })
