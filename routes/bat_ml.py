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
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import LabelEncoder
import re

bat_ml_bp = Blueprint('bat_ml', __name__, url_prefix='/bat-ml')

# Global cache for bat identification models
# This prevents redundant disk I/O and session size issues
BAT_MODEL_CACHE = None

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

def clean_numeric(value):
    """
    Strict numeric parsing for bat measurements.
    """
    if pd.isna(value):
        return np.nan
    
    if isinstance(value, (int, float)):
        return float(value)
    
    # Convert to string and clean
    s = str(value).strip().lower()
    if not s or s in ['-', 'none', 'nan', 'unknown', '--', 'null']:
        return np.nan
        
    try:
        # Check for simple numeric strings first
        if re.match(r'^-?\d+(\.\d+)?$', s):
            return float(s)
            
        # Handle ranges (43.1-43.5) by taking average
        if '-' in s:
            parts = s.split('-')
            if len(parts) == 2:
                # If it looks like box info (e.g. 43-14) or IDs, skip the "smart" decimal conversion
                # which was causing fake weights like 43.14. 
                # REAL ranges are usually close (e.g. 43.1-43.5). 
                # If they differ by > 20%, it's probably an ID, not a measurement.
                p1 = re.sub(r'[^-0-9.]', '', parts[0])
                p2 = re.sub(r'[^-0-9.]', '', parts[1])
                if p1 and p2:
                    v1, v2 = float(p1), float(p2)
                    if abs(v1 - v2) < (max(v1, v2) * 0.2): # Within 20%
                        return (v1 + v2) / 2
            return np.nan # Assume junk if not a close range
            
        # Clean common garbage characters
        s_clean = re.sub(r'[^-0-9.]', '', s)
        if s_clean:
            return float(s_clean)
    except:
        pass
        
    return np.nan

def save_bat_models(bat_models):
    """Save trained bat models to disk"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        models_dir = os.path.join(os.path.dirname(current_dir), 'models')
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
        
        # Update global cache
        global BAT_MODEL_CACHE
        BAT_MODEL_CACHE = bat_models
        
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to save bat models: {e}")
        return False

def load_bat_models():
    """Load trained bat models from disk or cache"""
    global BAT_MODEL_CACHE
    
    try:
        # Check cache first
        if BAT_MODEL_CACHE is not None:
            # print("DEBUG: Returning bat models from memory cache")
            return BAT_MODEL_CACHE
            
        # Use absolute path for reliability
        current_dir = os.path.dirname(os.path.abspath(__file__))
        models_dir = os.path.join(os.path.dirname(current_dir), 'models')
        
        # print(f"DEBUG: Attempting to load models from: {models_dir}")
        
        if not os.path.exists(models_dir):
            print(f"DEBUG: Models directory not found at {models_dir}")
            return None
        
        # Check if personalized models exist, otherwise fallback to default
        required_files = [
            'bat_genus_model.pkl',
            'bat_species_model.pkl', 
            'bat_genus_encoder.pkl',
            'bat_species_encoder.pkl',
            'bat_feature_encoders.pkl',
            'bat_model_metadata.pkl'
        ]
        
        missing_personalized = [f for f in required_files if not os.path.exists(os.path.join(models_dir, f))]
        
        if missing_personalized:
            print(f"DEBUG: Missing personalized models: {missing_personalized}. Trying default models...")
            # Fallback to default models which are always included
            default_files = {
                'genus_model': 'default_genus_model.pkl',
                'species_model': 'default_species_model.pkl',
                'genus_encoder': 'default_encoders.pkl', # Generic encoder usually contains all
                'species_encoder': 'default_encoders.pkl',
                'feature_encoders': 'default_encoders.pkl',
                'metadata': 'default_metadata.pkl'
            }
            # Special check for encoders - might be different structure in default
            # For simplicity, if personalized are missing but default exist, we'll try to load default
            if not os.path.exists(os.path.join(models_dir, 'default_genus_model.pkl')):
                print("DEBUG: Even default models are missing.")
                return None
            
            # Note: Default loading might need different logic depending on how default_encoders.pkl is structured
            # For now, let's focus on whypersonalized are missing or failing
            
        # Try loading personalized models
        bat_models = {}
        try:
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
                
            print(f"DEBUG: Personalized bat models loaded successfully from {models_dir}")
            
        except Exception as e:
            print(f"DEBUG: Failed to load personalized models: {e}. Falling back to defaults...")
            # Try defaults
            try:
                with open(os.path.join(models_dir, 'default_genus_model.pkl'), 'rb') as f:
                    bat_models['genus_model'] = pickle.load(f)
                with open(os.path.join(models_dir, 'default_species_model.pkl'), 'rb') as f:
                    bat_models['species_model'] = pickle.load(f)
                with open(os.path.join(models_dir, 'default_metadata.pkl'), 'rb') as f:
                    metadata = pickle.load(f)
                    bat_models.update(metadata)
                
                # Check if we have encoders in metadata or separate
                if 'genus_encoder' not in bat_models:
                    with open(os.path.join(models_dir, 'default_encoders.pkl'), 'rb') as f:
                        encoders = pickle.load(f)
                        bat_models.update(encoders)
                
                print("DEBUG: Default bat models loaded successfully")
            except Exception as e2:
                print(f"DEBUG: Failed to load default models: {e2}")
                return None
        
        # Update cache
        BAT_MODEL_CACHE = bat_models
        return bat_models
        
    except Exception as e:
        print(f"ERROR: Unexpected error in load_bat_models: {e}")
        return None

@bat_ml_bp.route('/train-bat-model', methods=['POST'])
def train_bat_model():
    """Train specialized bat identification model"""
    try:
        # Handle both JSON and FormData
        if request.is_json:
            force_train = request.json.get('force', False)
            uploaded_file = None
        else:
            force_train = request.form.get('force') == 'true'
            uploaded_file = request.files.get('file')
        
        # Only skip if NOT forcing and models exist
        if not force_train and not uploaded_file:
            existing_models = load_bat_models()
            if existing_models:
                # Models already exist and we aren't forcing, so just return status
                session['bat_models_loaded'] = True
                print("DEBUG: Using existing pre-trained models from disk/cache")
                
                return jsonify({
                    'success': True,
                    'message': 'Using pre-trained bat identification models',
                    'genus_accuracy': round(existing_models['genus_accuracy'] * 100, 2),
                    'species_accuracy': round(existing_models['species_accuracy'] * 100, 2),
                    'sample_count': existing_models['sample_count'],
                    'genus_classes': len(existing_models['genus_classes']),
                    'species_classes': len(existing_models['species_classes']),
                    'pre_trained': True
                })
        
        # Prepare dataframe
        if uploaded_file:
            print(f"DEBUG: Training models using uploaded custom file: {uploaded_file.filename}")
            df = pd.read_excel(uploaded_file)
        else:
            # Enforce mandatory file upload as per user requirement
            return jsonify({
                'success': False,
                'message': 'Please select a custom training file (.xlsx) first.'
            })
        
        # Standardize column names
        df.columns = [str(col).strip() for col in df.columns]
        
        # Map BATHOST specific columns to expected model features
        column_mapping = {
            'forearm_mm': 'FA',
            'tibia_mm': 'TIB',
            'weight_g': 'W',
            'sex': 'Sex',
            'status': 'Status',
            'Genus': 'genus',
            'Species': 'species'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        # 1. CLEANING CATEGORICAL DATA (Fix "Sphaerias " vs "Sphaerias")
        for col in ['genus', 'species', 'Sex', 'Status']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()
                # Remove rows with placeholder text
                df = df[~df[col].isin(['-', 'None', 'Nan', 'Unknown', 'nan', ''])]
        
        # 2. CLEANING NUMERIC DATA (Fix "43-14" -> nan, handle ranges correctly)
        for col in ['FA', 'TIB', 'W']:
            if col in df.columns:
                df[col] = df[col].apply(clean_numeric)
        
        # 3. FILTERING
        initial_count = len(df)
        required_cols = ['genus', 'species', 'FA', 'TIB', 'W', 'Sex', 'Status']
        
        # Check if required columns actually exist
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
             # Last resort case-insensitive search
             for m in missing:
                 for col in df.columns:
                     if col.lower() == m.lower():
                         df = df.rename(columns={col: m})
                         break
        
        # FINAL CHECK for required columns (especially for uploaded files)
        re_missing = [c for c in required_cols if c not in df.columns]
        if re_missing:
            return jsonify({
                'success': False,
                'message': f'Uploaded file is missing required columns: {", ".join(re_missing)}'
            })

        # Verify required columns exist before dropping NA
        existing_required = [c for c in required_cols if c in df.columns]
        clean_df = df.dropna(subset=existing_required).copy()
        
        # CREATE BINOMIAL TARGET early for filtering (ensures uniqueness across genera)
        clean_df['binomial'] = clean_df['genus'] + "_" + clean_df['species']
        
        # REMOVE OUTLIERS (Strict filtering for accuracy)
        if 'W' in clean_df.columns:
            clean_df = clean_df[(clean_df['W'] > 0) & (clean_df['W'] < 300)]
        if 'TIB' in clean_df.columns:
            clean_df = clean_df[(clean_df['TIB'] > 0) & (clean_df['TIB'] < 300)]
        if 'FA' in clean_df.columns:
            clean_df = clean_df[(clean_df['FA'] > 0) & (clean_df['FA'] < 300)]

        # ITERATIVE RARE CLASS FILTERING
        # We MUST filter on 'binomial' to ensure species models have enough samples
        # for EVERY unique genus-species combination.
        for _ in range(2):
            binomial_counts = clean_df['binomial'].value_counts()
            valid_binomials = binomial_counts[binomial_counts >= 3].index
            clean_df = clean_df[clean_df['binomial'].isin(valid_binomials)]
            
            genus_counts = clean_df['genus'].value_counts()
            valid_genus = genus_counts[genus_counts >= 3].index
            clean_df = clean_df[clean_df['genus'].isin(valid_genus)]
        
        print(f"DEBUG: Clean dataset: {len(clean_df)} records (from {initial_count} initial)")
        print(f"DEBUG: Unique Genus: {clean_df['genus'].nunique()}, Unique Binomials: {clean_df['binomial'].nunique()}")
        
        if len(clean_df) < 50:
            return jsonify({
                'success': False,
                'message': f'Insufficient clean data ({len(clean_df)} rows). Please check your Excel format.'
            })
        
        # 4. ENCODING
        feature_encoders = {}
        for col in ['Sex', 'Status']:
            le = LabelEncoder()
            clean_df[col] = le.fit_transform(clean_df[col].astype(str))
            feature_encoders[col] = le
            
        # Target encoders
        genus_le = LabelEncoder()
        binomial_le = LabelEncoder() # Use binomial for species model
        y_genus = genus_le.fit_transform(clean_df['genus'])
        y_binomial = binomial_le.fit_transform(clean_df['binomial'])
        
        # Hierarchical Encoding: Include Genus in Species features
        clean_df['genus_enc'] = y_genus
        
        X_genus = clean_df[['FA', 'TIB', 'W', 'Sex', 'Status']]
        X_species = clean_df[['FA', 'TIB', 'W', 'Sex', 'Status', 'genus_enc']]
        
        # 5. MODELING (Switching back to high-capacity RandomForest for reliability)
        # 5. MODELING (High-capacity RandomForest for reliability)
        print("DEBUG: Training Genus model (RandomForest)...")
        # Training Genus model
        genus_model = RandomForestClassifier(n_estimators=300, random_state=42, class_weight='balanced')
        
        X_train, X_test, y_g_train, y_g_test = train_test_split(
            X_genus, y_genus, test_size=0.2, random_state=42, stratify=y_genus
        )
        genus_model.fit(X_train, y_g_train)
        genus_acc = accuracy_score(y_g_test, genus_model.predict(X_test))
        print(f"DEBUG: Genus Accuracy: {genus_acc:.4f}")
        
        print("DEBUG: Training Species model (RandomForest)...")
        species_model = RandomForestClassifier(n_estimators=500, max_depth=None, random_state=42, class_weight='balanced')
        
        X_train_sp, X_test_sp, y_sp_train, y_sp_test = train_test_split(
            X_species, y_binomial, test_size=0.2, random_state=42, stratify=y_binomial
        )
        species_model.fit(X_train_sp, y_sp_train)
        species_acc = accuracy_score(y_sp_test, species_model.predict(X_test_sp))
        print(f"DEBUG: Species Accuracy: {species_acc:.4f}")
        
        # 6. ASSEMBLE MODEL OBJECT
        bat_models = {
            'genus_model': genus_model,
            'species_model': species_model,
            'genus_encoder': genus_le,
            'species_encoder': binomial_le, # Stores binomials
            'feature_encoders': feature_encoders,
            'features': ['FA', 'TIB', 'W', 'Sex', 'Status'],
            'genus_accuracy': float(genus_acc),
            'species_accuracy': float(species_acc),
            'genus_classes': genus_le.classes_.tolist(),
            'species_classes': binomial_le.classes_.tolist(),
            'sample_count': int(len(clean_df))
        }
        
        # Save models to disk for persistence
        save_success = save_bat_models(bat_models)
        if save_success:
            print("DEBUG: Models saved successfully to disk")
        
        return jsonify({
            'success': True,
            'message': 'Bat identification models trained successfully!',
            'genus_accuracy': round(genus_acc * 100, 2),
            'species_accuracy': round(species_acc * 100, 2),
            'sample_count': len(clean_df),
            'genus_classes': len(genus_le.classes_),
            'species_classes': len(binomial_le.classes_),
            'pre_trained': False
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
        
        # Check if models are available in cache/disk
        bat_models = load_bat_models()
        
        if not bat_models:
            print("DEBUG: No trained bat models found")
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
        
        # Models are now loaded from cache/disk
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
        
        # 1. Prepare base features vector (FA, TIB, W, Sex, Status)
        X_base = prepare_features(data, feature_encoders)
        
        # 2. Predict Genus first
        genus_pred_encoded = genus_model.predict(X_base)[0]
        genus_proba = genus_model.predict_proba(X_base)[0]
        genus_pred = le_genus.inverse_transform([genus_pred_encoded])[0]
        
        # 3. Predict Species using base features + predicted genus as feature
        X_species = np.append(X_base, [[genus_pred_encoded]], axis=1)
        species_pred_encoded = species_model.predict(X_species)[0]
        species_proba = species_model.predict_proba(X_species)[0]
        
        # Decode binomial (Genus_Species)
        binomial_pred = le_species.inverse_transform([species_pred_encoded])[0]
        species_pred = binomial_pred.split('_')[1] if '_' in binomial_pred else binomial_pred
        
        # Get top genus predictions
        top_genus_idx = np.argsort(genus_proba)[-3:][::-1]
        top_genus = [
            {
                'name': le_genus.inverse_transform([idx])[0],
                'confidence': float(genus_proba[idx] * 100)
            }
            for idx in top_genus_idx
        ]
        
        # Get top species predictions (using current genus context for features)
        top_species_idx = np.argsort(species_proba)[-3:][::-1]
        top_species = []
        for idx in top_species_idx:
            b_name = le_species.inverse_transform([idx])[0]
            s_name = b_name.split('_')[1] if '_' in b_name else b_name
            top_species.append({
                'name': s_name,
                'confidence': float(species_proba[idx] * 100)
            })
        
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
            'input_features': {k: float(v) if isinstance(v, (np.int64, np.float64)) else v for k, v in data.items()},
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

def prepare_features(data, feature_encoders):
    """
    Prepare feature vector for prediction.
    Must match the training order: ['FA', 'TIB', 'W', 'Sex', 'Status']
    """
    try:
        # 1. Clean and encode categorical features
        sex = str(data.get('Sex', 'Unknown')).strip().title()
        if sex not in feature_encoders['Sex'].classes_:
            sex = feature_encoders['Sex'].classes_[0]
        sex_enc = feature_encoders['Sex'].transform([sex])[0]
        
        status = str(data.get('Status', 'Unknown')).strip().title()
        if status not in feature_encoders['Status'].classes_:
            status = feature_encoders['Status'].classes_[0]
        status_enc = feature_encoders['Status'].transform([status])[0]
        
        # 2. Clean numeric features using global helper
        fa = clean_numeric(data.get('FA'))
        tib = clean_numeric(data.get('TIB'))
        w = clean_numeric(data.get('W'))
        
        # 3. Handle missing values (HistGradientBoosting handles NaNs, but we'll use 0 as fallback if needed)
        fa = fa if not pd.isna(fa) else 0.0
        tib = tib if not pd.isna(tib) else 0.0
        w = w if not pd.isna(w) else 0.0
        
        # 4. Create feature array in CORRECT order
        # CRITICAL: Must match clean_df[['FA', 'TIB', 'W', 'Sex', 'Status']]
        feature_vec = [fa, tib, w, sex_enc, status_enc]
        return np.array([feature_vec])
        
    except Exception as e:
        print(f"ERROR: prepare_features failed: {e}")
        # Return a safe fallback vector if everything fails
        return np.array([[0.0, 0.0, 0.0, 0, 0]])

@bat_ml_bp.route('/predict-bat-batch', methods=['POST'])
def predict_bat_batch():
    """Predict bat genus and species for multiple records"""
    try:
        print(f"DEBUG: Bat batch prediction request received")
        
        # Check if models are available in cache/disk
        bat_models = load_bat_models()
        if not bat_models:
            return jsonify({
                'success': False,
                'message': 'No trained bat models found. Please train the model first.'
            })
        
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
                
                # 1. Prepare base features
                X_base = prepare_features(input_data, feature_encoders)
                
                # 2. Predict Genus first
                genus_pred_encoded = genus_model.predict(X_base)[0]
                genus_proba = genus_model.predict_proba(X_base)[0]
                genus_pred = le_genus.inverse_transform([genus_pred_encoded])[0]
                
                # 3. Predict Species (Base + Genus_Enc)
                X_species = np.append(X_base, [[genus_pred_encoded]], axis=1)
                species_pred_encoded = species_model.predict(X_species)[0]
                species_proba = species_model.predict_proba(X_species)[0]
                
                # Decode binomial
                binomial_pred = le_species.inverse_transform([species_pred_encoded])[0]
                species_pred = binomial_pred.split('_')[1] if '_' in binomial_pred else binomial_pred
                
                # Get top genus predictions
                top_genus_idx = np.argsort(genus_proba)[-3:][::-1]
                top_genus = [
                    {
                        'name': le_genus.inverse_transform([idx])[0],
                        'confidence': float(genus_proba[idx] * 100)
                    }
                    for idx in top_genus_idx
                ]
                
                # Get top species predictions (binomials -> species)
                top_species_idx = np.argsort(species_proba)[-3:][::-1]
                top_species = []
                for idx in top_species_idx:
                    b_name = le_species.inverse_transform([idx])[0]
                    s_name = b_name.split('_')[1] if '_' in b_name else b_name
                    top_species.append({
                        'name': s_name,
                        'confidence': float(species_proba[idx] * 100)
                    })
                
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
        # Check models in cache/disk
        bat_models = load_bat_models()
        
        if not bat_models:
            return jsonify({
                'trained': False,
                'message': 'No trained bat models found. Please train the model first.'
            })
        
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

# Auto-load models on module import
try:
    print("DEBUG: Auto-loading bat models on startup...")
    load_bat_models()
except Exception as e:
    print(f"DEBUG: Auto-load failed: {e}")