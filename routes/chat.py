"""
AI Chat Interface for HaoXai
Natural language queries about virology data
"""
import sqlite3
import os
import json
import re
import pandas as pd
import io
import pickle
from flask import Blueprint, render_template, request, jsonify, current_app, send_file, session
import flask
from datetime import datetime
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from ml_trainer import DatabaseTrainer
from master_sql_trainer import MasterSQLTrainer
from master_python_trainer import MasterPythonTrainer

chat_bp = Blueprint('chat', __name__, url_prefix='/chat')

def get_country_from_province(province):
    """Get country based on province name"""
    if not province or province == 'N/A':
        return 'N/A'
    
    # Map provinces to countries
    province_to_country = {
        'Khammouan': 'Laos',
        'Vientiane': 'Laos',
        'Luang Namtha': 'Laos',
        'Oudomxay': 'Laos',
        'Phongsaly': 'Laos',
        'Luang Prabang': 'Laos',
        'Savannakhet': 'Laos',
        'Saravan': 'Laos',
        'Champasak': 'Laos',
        'Attapeu': 'Laos',
        'Xekong': 'Laos',
        'Xieng Khouang': 'Laos',
        'Bolikhamxay': 'Laos',
        'Kaysone Phomvihan': 'Laos'
    }
    
    # Extract province name (remove "Province" suffix if present)
    clean_province = province.replace(' Province', '').strip()
    
    return province_to_country.get(clean_province, 'N/A')

def get_rodent_number_from_fieldid(field_id):
    """Extract rodent number from FieldId (e.g., DS241218.1 -> 1)"""
    if not field_id or field_id == 'N/A':
        return 'N/A'
    
    import re
    match = re.search(r'\.(\d+)$', field_id)
    if match:
        return match.group(1)
    
    return 'N/A'

def get_collector_from_fieldid(field_id):
    """Extract collector from FieldId (e.g., DS241218.1 -> DS)"""
    if not field_id or field_id == 'N/A':
        return 'N/A'
    
    import re
    match = re.match(r'^([A-Z]+)', field_id)
    if match:
        return match.group(1)
    
    return 'N/A'

def parse_rodent_location(location_str):
    """Parse location string into village, district, province"""
    if not location_str or location_str == 'N/A':
        return 'N/A', 'N/A', 'N/A'
    
    parts = [part.strip() for part in location_str.split(',')]
    
    village = 'N/A'
    district = 'N/A' 
    province = 'N/A'
    
    for part in parts:
        if 'Village' in part:
            village = part.replace('Village', '').strip()
        elif 'District' in part:
            district = part.replace('District', '').strip()
        elif 'Province' in part:
            province = part.replace('Province', '').strip()
    
    return village, district, province

def parse_field_date(field_id):
    """Parse FieldId like DS241218.4 to extract capture date"""
    if not field_id or field_id == 'N/A':
        return 'N/A'
    
    import re
    match = re.search(r'DS(\d{2})(\d{2})(\d+)\.\d+', field_id)
    if match:
        year = '20' + match.group(1)
        month = match.group(2)
        day = match.group(3)
        
        month_names = {
            '01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
            '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
            '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'
        }
        
        month_name = month_names.get(month, month)
        return f"{year} {month_name} {day}"
    
    return 'N/A'

def get_smart_ai():
    """Get Smart AI instance - now using FREE local AI"""
    db_type = session.get('db_type', 'sqlite')
    
    if db_type == 'sqlite':
        db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
    else:
        db_config = session.get('db_params')
    
    return SmartLocalAI(db_config, db_type)

class SmartLocalAI:
    """Smart Local AI - FREE, dynamically queries any connected database with ML enhancement"""
    
    def __init__(self, db_config, db_type='sqlite'):
        self.db_config = db_config
        self.db_type = db_type
        self.schema_cache = None
        self.trainer = None
        self.models_loaded = False
        self.master_sql_trainer = None
        self.master_python_trainer = None
        self.master_models_loaded = False
        
        # Initialize ML trainer
        try:
            self.trainer = DatabaseTrainer(db_config, db_type)
            self.models_loaded = self.trainer.load_models()
            if self.models_loaded:
                print("✅ ML models loaded successfully")
            else:
                print("ℹ️ No trained models available - using rule-based approach")
        except Exception as e:
            print(f"⚠️ Error loading ML models: {e}")
            self.trainer = None
        
        # Initialize Master SQL trainer
        try:
            self.master_sql_trainer = MasterSQLTrainer(db_config, db_type)
            sql_loaded = self.master_sql_trainer.load_master_sql_models()
            if sql_loaded:
                print("✅ Master SQL models loaded successfully")
                self.master_models_loaded = True
            else:
                print("ℹ️ No Master SQL models available")
        except Exception as e:
            print(f"⚠️ Error loading Master SQL models: {e}")
            self.master_sql_trainer = None
        
        # Initialize Master Python trainer
        try:
            self.master_python_trainer = MasterPythonTrainer(db_config, db_type)
            python_loaded = self.master_python_trainer.load_master_python_models()
            if python_loaded:
                print("✅ Master Python models loaded successfully")
                self.master_models_loaded = True
            else:
                print("ℹ️ No Master Python models available")
        except Exception as e:
            print(f"⚠️ Error loading Master Python models: {e}")
            self.master_python_trainer = None
        
        if self.master_models_loaded:
            print("🚀 Master Intelligence models loaded successfully!")
        elif self.models_loaded:
            print("🧠 Enhanced ML models loaded successfully!")
        else:
            print("ℹ️ Using basic rule-based approach")
    
    def get_connection(self):
        """Get database connection"""
        from database.db_manager_flask import DatabaseManagerFlask
        return DatabaseManagerFlask.get_connection(self.db_config, self.db_type)
    
    def _find_numeric_sample_id(self, cursor, string_sample_id):
        """Find numeric sample_id by searching screening_results for string IDs"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Search screening_results for the string ID
            cursor.execute(f"SELECT sample_id FROM {q}screening_results{q} WHERE {q}tested_sample_id{q} = {p} LIMIT 1", (string_sample_id,))
            result = cursor.fetchone()
            
            if result:
                numeric_id = result[0]
                print(f"DEBUG: Found numeric sample_id {numeric_id} for string ID {string_sample_id} in screening_results")
                return str(numeric_id)
            
            # Also try other common string ID columns
            string_id_columns = ['tube_id', 'sample_tube_id', 'field_id', 'source_id']
            for col in string_id_columns:
                cursor.execute(f"SELECT sample_id FROM {q}screening_results{q} WHERE {q}{col}{q} = {p} LIMIT 1", (string_sample_id,))
                result = cursor.fetchone()
                if result:
                    numeric_id = result[0]
                    print(f"DEBUG: Found numeric sample_id {numeric_id} for string ID {string_sample_id} via {col}")
                    return str(numeric_id)
            
            print(f"DEBUG: No numeric sample_id found for string ID {string_sample_id}")
            return None
            
        except Exception as e:
            print(f"DEBUG: Error finding numeric sample_id for {string_sample_id}: {e}")
            return None
    
    def _build_recursive_fk_profile(self, cursor, table_name, record_id, id_column):
        """Build profile using recursive FK traversal across multiple tables"""
        profile = {
            'main_record': {},
            'discovered_data': {},
            'fk_paths': [],
            'summary': []
        }
        
        try:
            print(f"DEBUG: Building recursive FK profile for {table_name}.{id_column} = {record_id}")
            
            # Initialize recursive traversal state
            self.visited_tables = set()
            self.discovered_paths = []
            self.max_depth = 4  # Prevent infinite loops
            
            # Start recursive traversal
            result = self._recursive_traverse(cursor, table_name, record_id, id_column, depth=0)
            
            if result:
                profile['main_record'] = result['record']
                profile['discovered_data'] = result['related_data']
                profile['fk_paths'] = self.discovered_paths
                profile['summary'] = self._build_recursive_summary(result['related_data'])
            
        except Exception as e:
            print(f"DEBUG: Error in recursive traversal: {e}")
            import traceback
            traceback.print_exc()
        
        return profile
    
    def _recursive_traverse(self, cursor, table_name, record_id, id_column, depth):
        """Recursively traverse FK relationships"""
        if depth >= self.max_depth or table_name in self.visited_tables:
            print(f"DEBUG: Max depth reached or table {table_name} already visited at depth {depth}")
            return None
        
        self.visited_tables.add(table_name)
        print(f"DEBUG: Traversing {table_name} at depth {depth} with ID {record_id}")
        
        result = {
            'record': {},
            'related_data': {},
            'relationships': {}
        }
        
        try:
            # Get the main record
            main_record = self._get_record_by_id(cursor, table_name, id_column, record_id)
            if not main_record:
                print(f"DEBUG: No record found for {table_name}.{id_column} = {record_id}")
                return result
            
            result['record'] = main_record
            print(f"DEBUG: Found main record in {table_name}")
            
            # Get all FK relationships for this table
            fk_relationships = self._get_foreign_key_relationships(table_name, cursor)
            print(f"DEBUG: Found {len(fk_relationships)} FK relationships for {table_name}")
            
            # Follow each FK relationship
            for fk in fk_relationships:
                relationship_data = self._follow_fk_relationship(cursor, main_record, fk, depth)
                if relationship_data:
                    table_key = f"{fk['to_table']}_data" if fk['type'] == 'forward' else f"{fk['from_table']}_data"
                    result['related_data'][table_key] = relationship_data
                    result['relationships'][table_key] = {
                        'type': fk['type'],
                        'path': f"{table_name}.{fk['from_column']} → {fk['to_table']}.{fk['to_column']}",
                        'data': relationship_data
                    }
                    
                    # Record the FK path
                    path = f"{'→' if fk['type'] == 'forward' else '←'} {fk['from_table']}.{fk['from_column']} → {fk['to_table']}.{fk['to_column']}"
                    self.discovered_paths.append(path)
                    
                    # Recursively traverse related tables
                    if isinstance(relationship_data, list) and relationship_data:
                        for related_record in relationship_data[:3]:  # Limit to prevent explosion
                            related_id = self._extract_record_id(related_record, fk['to_table'] if fk['type'] == 'forward' else fk['from_table'])
                            if related_id:
                                # Use the correct ID column for the next table
                                next_id_column = self._get_primary_key_column(cursor, fk['to_table'] if fk['type'] == 'forward' else fk['from_table'])
                                recursive_result = self._recursive_traverse(
                                    cursor, 
                                    fk['to_table'] if fk['type'] == 'forward' else fk['from_table'], 
                                    related_id, 
                                    next_id_column, 
                                    depth + 1
                                )
                                if recursive_result:
                                    # Merge recursive results
                                    self._merge_recursive_results(result, recursive_result, table_key)
            
            print(f"DEBUG: Completed traversal of {table_name} at depth {depth}")
            
        except Exception as e:
            print(f"DEBUG: Error traversing {table_name}: {e}")
            import traceback
            traceback.print_exc()
        
        return result
    
    def _follow_fk_relationship(self, cursor, main_record, fk, depth):
        """Follow a single FK relationship"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            if fk['type'] == 'forward':
                # Forward: current_table → related_table
                fk_value = main_record.get(fk['from_column'])
                if not fk_value:
                    return None
                
                print(f"DEBUG: Following forward FK: {fk['from_table']}.{fk['from_column']} → {fk['to_table']}.{fk['to_column']} (value: {fk_value})")
                
                cursor.execute(f"SELECT * FROM {q}{fk['to_table']}{q} WHERE {q}{fk['to_column']}{q} = {p} LIMIT 10", (fk_value,))
                rows = cursor.fetchall()
                
            elif fk['type'] == 'reverse':
                # Reverse: related_table → current_table
                pk_value = main_record.get(fk['to_column'])
                if not pk_value:
                    return None
                
                print(f"DEBUG: Following reverse FK: {fk['from_table']}.{fk['from_column']} → {fk['to_table']}.{fk['to_column']} (value: {pk_value})")
                
                cursor.execute(f"SELECT * FROM {q}{fk['from_table']}{q} WHERE {q}{fk['from_column']}{q} = {p} LIMIT 10", (pk_value,))
                rows = cursor.fetchall()
            
            if rows:
                # Get column names dynamically
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{fk["to_table"] if fk["type"] == "forward" else fk["from_table"]}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{fk["to_table"] if fk["type"] == "forward" else fk["from_table"]}`')
                    columns = [c[0] for c in cursor.fetchall()]
                
                return [dict(zip(columns, row)) for row in rows]
            
            return []
            
        except Exception as e:
            print(f"DEBUG: Error following FK {fk['from_table']}.{fk['from_column']} → {fk['to_table']}.{fk['to_column']}: {e}")
            return []
    
    def _extract_record_id(self, record, table_name):
        """Extract the primary ID from a record"""
        id_columns = ['id', 'sample_id', 'sample_code', 'Id', 'Sample_Id', 'Sample_Code']
        
        for id_col in id_columns:
            if id_col in record and record[id_col]:
                return str(record[id_col])
        
        # Fallback to first column
        if record:
            first_key = list(record.keys())[0]
            return str(record[first_key])
        
        return None
    
    def _get_primary_key_column(self, cursor, table_name):
        """Get the primary key column name for a table"""
        try:
            if self.db_type == 'sqlite':
                cursor.execute(f'PRAGMA table_info("{table_name}")')
                columns = cursor.fetchall()
                for col in columns:
                    if col[5] == 1:  # PK flag
                        return col[1]
            else:  # MySQL/MariaDB
                cursor.execute(f'DESCRIBE `{table_name}`')
                columns = cursor.fetchall()
                for col in columns:
                    if col[3] == 'PRI':  # Primary Key
                        return col[0]
            
            # Fallback to common primary key names
            common_pks = ['id', 'sample_id', 'location_id', 'host_id', 'taxonomy_id', 'screening_id', 'storage_id']
            
            # Check if any common PK exists in the table
            if self.db_type == 'sqlite':
                cursor.execute(f'PRAGMA table_info("{table_name}")')
                columns = [c[1] for c in cursor.fetchall()]
            else:
                cursor.execute(f'DESCRIBE `{table_name}`')
                columns = [c[0] for c in cursor.fetchall()]
            
            for pk in common_pks:
                if pk in columns:
                    return pk
            
            # Last resort: return first column
            return columns[0] if columns else 'id'
            
        except Exception as e:
            print(f"DEBUG: Error getting PK column for {table_name}: {e}")
            return 'id'  # Fallback
    
    def _merge_recursive_results(self, current_result, recursive_result, table_key):
        """Merge recursive results into current result"""
        if recursive_result and recursive_result.get('related_data'):
            for related_table, related_data in recursive_result['related_data'].items():
                if related_table not in current_result['related_data']:
                    current_result['related_data'][related_table] = related_data
                else:
                    # Merge data if table already exists
                    if isinstance(current_result['related_data'][related_table], list) and isinstance(related_data, list):
                        current_result['related_data'][related_table].extend(related_data)
    
    def _build_recursive_summary(self, related_data):
        """Build summary from recursively discovered relationships"""
        summary_parts = []
        
        summary_parts.append("🔗 **Recursive FK Discovery Results**")
        
        for table_key, data in related_data.items():
            if isinstance(data, list):
                table_name = table_key.replace('_data', '').replace('_', ' ').title()
                summary_parts.append(f"• {table_name}: {len(data)} records")
                
                # Show sample of data from this table
                if data and len(data) > 0:
                    sample_record = data[0]
                    important_fields = ['name', 'title', 'species', 'scientific_name', 'country', 'province', 'sample_id']
                    for field in important_fields:
                        if field in sample_record and sample_record[field]:
                            display_name = field.replace('_', ' ').title()
                            summary_parts.append(f"  - {display_name}: {sample_record[field]}")
                            break
            elif data:
                table_name = table_key.replace('_info', '').replace('_', ' ').title()
                summary_parts.append(f"• {table_name}: 1 record")
                
                important_fields = ['name', 'title', 'species', 'scientific_name', 'country', 'province', 'sample_id']
                for field in important_fields:
                    if field in data and data[field]:
                        display_name = field.replace('_', ' ').title()
                        summary_parts.append(f"  - {display_name}: {data[field]}")
                        break
        
        return summary_parts
    
    def _convert_recursive_to_legacy_format(self, recursive_profile):
        """Convert recursive profile to legacy format for compatibility"""
        discovered_data = recursive_profile.get('discovered_data', {})
        
        return {
            'sample_info': recursive_profile.get('main_record', {}),
            'host_info': discovered_data.get('hosts_data', [{}])[0] if discovered_data.get('hosts_data') else {},
            'taxonomy_info': discovered_data.get('taxonomy_data', [{}])[0] if discovered_data.get('taxonomy_data') else {},
            'location_info': discovered_data.get('locations_data', [{}])[0] if discovered_data.get('locations_data') else {},
            'screening_results': discovered_data.get('screening_results_data', []),
            'sequencing_data': discovered_data.get('sequencing_data', []) or discovered_data.get('sequences_data', []),
            'storage_info': discovered_data.get('storage_locations_data', [{}])[0] if discovered_data.get('storage_locations_data') else {},
            'fk_debug_info': {
                'paths_used': recursive_profile.get('fk_paths', []),
                'summary': recursive_profile.get('summary', []),
                'discovered_data': discovered_data
            }
        }
    
    def _build_dynamic_fk_profile(self, cursor, table_name, record_id, id_column):
        """Build profile using completely dynamic FK relationships"""
        profile = {
            'main_record': {},
            'related_data': {},
            'fk_paths_used': [],
            'summary': []
        }
        
        try:
            print(f"DEBUG: Building dynamic profile for {table_name}.{id_column} = {record_id}")
            
            # Step 1: Get the main record
            main_record = self._get_record_by_id(cursor, table_name, id_column, record_id)
            if not main_record:
                print(f"DEBUG: Record not found: {table_name}.{id_column} = {record_id}")
                return profile
            
            profile['main_record'] = main_record
            print(f"DEBUG: Found main record: {table_name}")
            
            # Step 2: Discover and follow all FK relationships
            fk_relationships = self._get_foreign_key_relationships(table_name, cursor)
            print(f"DEBUG: Found {len(fk_relationships)} FK relationships for {table_name}")
            
            # Step 3: Follow forward relationships (main table -> related tables)
            for fk in fk_relationships:
                if fk['type'] == 'forward':
                    related_data = self._follow_forward_fk_dynamic(cursor, main_record, fk)
                    if related_data:
                        table_key = f"{fk['to_table']}_info"
                        profile['related_data'][table_key] = related_data
                        profile['fk_paths_used'].append(f"FORWARD: {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}")
                        print(f"DEBUG: Found {fk['to_table']} via forward FK")
            
            # Step 4: Follow reverse relationships (related tables -> main table)
            for fk in fk_relationships:
                if fk['type'] == 'reverse':
                    related_data = self._follow_reverse_fk_dynamic(cursor, main_record, fk)
                    if related_data:
                        table_key = f"{fk['from_table']}_data"
                        profile['related_data'][table_key] = related_data
                        profile['fk_paths_used'].append(f"REVERSE: {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}")
                        print(f"DEBUG: Found {len(related_data)} records in {fk['from_table']} via reverse FK")
            
            # Step 5: Build summary
            profile['summary'] = self._build_dynamic_summary(profile)
            
        except Exception as e:
            print(f"DEBUG: Error building dynamic profile: {e}")
            import traceback
            traceback.print_exc()
        
        return profile
    
    def _get_record_by_id(self, cursor, table_name, id_column, record_id):
        """Get record by ID dynamically - tries multiple ID columns for samples table"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # For samples table, try multiple ID columns
            if table_name == 'samples':
                id_columns_to_try = ['sample_id', 'tissue_id', 'source_id', 'host_id', 'intestine_id', 'plasma_id']
            else:
                id_columns_to_try = [id_column]
            
            for col_to_try in id_columns_to_try:
                cursor.execute(f"SELECT * FROM {q}{table_name}{q} WHERE {q}{col_to_try}{q} = {p} LIMIT 1", (record_id,))
                row = cursor.fetchone()
                if row:
                    # Get column names dynamically
                    if self.db_type == 'sqlite':
                        cursor.execute(f'PRAGMA table_info("{table_name}")')
                        columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f'DESCRIBE `{table_name}`')
                        columns = [c[0] for c in cursor.fetchall()]
                    record = dict(zip(columns, row))
                    print(f"DEBUG: Found record in {table_name} via {col_to_try}: {record_id}")
                    return record
            
            print(f"DEBUG: No record found in {table_name} with ID {record_id} (tried: {id_columns_to_try})")
        except Exception as e:
            print(f"Error getting record {table_name}.{id_column} = {record_id}: {e}")
        
        return None
    
    def _follow_forward_fk_dynamic(self, cursor, main_record, fk):
        """Follow forward FK relationship dynamically"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Get the FK value from main record
            fk_value = main_record.get(fk['from_column'])
            if not fk_value:
                return None
            
            # Query the related table
            cursor.execute(f"SELECT * FROM {q}{fk['to_table']}{q} WHERE {q}{fk['to_column']}{q} = {p} LIMIT 1", (fk_value,))
            row = cursor.fetchone()
            if row:
                # Get column names dynamically
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{fk["to_table"]}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{fk["to_table"]}`')
                    columns = [c[0] for c in cursor.fetchall()]
                return dict(zip(columns, row))
        
        except Exception as e:
            print(f"Error following forward FK {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}: {e}")
        
        return None
    
    def _follow_reverse_fk_dynamic(self, cursor, main_record, fk):
        """Follow reverse FK relationship dynamically"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Get the primary key value from main record
            pk_value = main_record.get(fk['to_column'])
            if not pk_value:
                return None
            
            # Query for records that reference this main record
            cursor.execute(f"SELECT * FROM {q}{fk['from_table']}{q} WHERE {q}{fk['from_column']}{q} = {p} LIMIT 10", (pk_value,))
            rows = cursor.fetchall()
            if rows:
                # Get column names dynamically
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{fk["from_table"]}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{fk["from_table"]}`')
                    columns = [c[0] for c in cursor.fetchall()]
                return [dict(zip(columns, row)) for row in rows]
        
        except Exception as e:
            print(f"Error following reverse FK {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}: {e}")
        
        return []
    
    def _build_dynamic_summary(self, profile):
        """Build summary from dynamically discovered relationships"""
        summary_parts = []
        
        main_record = profile.get('main_record', {})
        related_data = profile.get('related_data', {})
        
        # Main record info
        if main_record:
            # Try to find a meaningful identifier
            for field in ['sample_id', 'name', 'title', 'id']:
                if field in main_record and main_record[field]:
                    summary_parts.append(f"• {field.replace('_', ' ').title()}: {main_record[field]}")
                    break
        
        # Related data summary
        for table_key, data in related_data.items():
            if isinstance(data, list):
                summary_parts.append(f"• {table_key.replace('_data', '').replace('_', ' ').title()}: {len(data)} records")
            elif data:
                # Try to find a meaningful field from single record
                for field in ['name', 'title', 'species', 'scientific_name', 'country', 'province']:
                    if field in data and data[field]:
                        summary_parts.append(f"• {table_key.replace('_info', '').replace('_', ' ').title()}: {data[field]}")
                        break
        
        return summary_parts
    
    def _convert_dynamic_to_legacy_format(self, dynamic_profile):
        """Convert dynamic profile to legacy format for compatibility"""
        related_data = dynamic_profile.get('related_data', {})
        
        return {
            'sample_info': dynamic_profile.get('main_record', {}),
            'host_info': related_data.get('hosts_info', {}) or related_data.get('hosts_data', [{}])[0] if related_data.get('hosts_data') else {},
            'taxonomy_info': related_data.get('taxonomy_info', {}) or related_data.get('taxonomy_data', [{}])[0] if related_data.get('taxonomy_data') else {},
            'location_info': related_data.get('locations_info', {}) or related_data.get('locations_data', [{}])[0] if related_data.get('locations_data') else {},
            'screening_results': related_data.get('screening_results_data', []) or related_data.get('screening_data', []),
            'sequencing_data': related_data.get('sequencing_data', []) or related_data.get('sequences_data', []),
            'storage_info': related_data.get('storage_locations_info', {}) or related_data.get('storage_locations_data', [{}])[0] if related_data.get('storage_locations_data') else {},
            'fk_debug_info': {
                'paths_used': dynamic_profile.get('fk_paths_used', []),
                'summary': dynamic_profile.get('summary', [])
            }
        }
    
    def get_schema(self):
        """Dynamically discover database schema"""
        if self.schema_cache:
            return self.schema_cache
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get all tables
            if self.db_type == 'sqlite':
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            else:  # MySQL
                cursor.execute("SHOW TABLES")
            
            tables = [row[0] for row in cursor.fetchall()]
            
            schema = {}
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
            
            conn.close()
            self.schema_cache = schema
            return schema
            
        except Exception as e:
            print(f"Schema discovery error: {e}")
            return {}
    
    def analyze_question(self, question):
        """Analyze question using ML models when available, fallback to rule-based approach"""
        # Try ML approach first if models are available
        if self.models_loaded and self.trainer:
            try:
                # Use trained models for prediction
                intent_prediction = self.trainer.predict_intent(question)
                table_prediction = self.trainer.predict_table(question)
                
                print(f"DEBUG: ML predictions - Intent: {intent_prediction}, Table: {table_prediction}")
                
                # If ML predictions are confident enough, use them
                ml_analysis = self._ml_analysis(question, intent_prediction, table_prediction)
                if ml_analysis:
                    print(f"✅ Using ML analysis for: {question}")
                    return ml_analysis
                else:
                    print(f"⚠️ ML analysis failed, falling back to rule-based")
            except Exception as e:
                print(f"⚠️ ML analysis failed, falling back to rule-based: {e}")
        
        # Fallback to rule-based analysis
        print(f"ℹ️ Using rule-based analysis for: {question}")
        return self._rule_based_analysis(question)
    
    def _ml_analysis(self, question, intent_prediction, table_prediction):
        """Use ML models to analyze the question"""
        try:
            # Check if predictions are confident enough (lowered threshold further)
            min_confidence = 0.2  # 20% minimum confidence (very permissive)
            
            if (intent_prediction and intent_prediction['confidence'] >= min_confidence and
                table_prediction and table_prediction['confidence'] >= min_confidence):
                
                # Extract keywords using enhanced method
                keywords = self._extract_keywords_enhanced(question)
                
                analysis = {
                    'intent': intent_prediction['intent'],
                    'tables': [table_prediction['table']],  # Focus on predicted table
                    'keywords': keywords,
                    'limit': 50,
                    'ml_confidence': {
                        'intent': intent_prediction['confidence'],
                        'table': table_prediction['confidence']
                    }
                }
                
                print(f"✅ Using ML analysis - Intent: {intent_prediction['intent']} (confidence: {intent_prediction['confidence']:.2f}), Table: {table_prediction['table']} (confidence: {table_prediction['confidence']:.2f})")
                return analysis
            
            else:
                print(f"⚠️ ML confidence too low - Intent: {intent_prediction['confidence']:.2f}, Table: {table_prediction['confidence']:.2f}")
                return None
            
        except Exception as e:
            print(f"Error in ML analysis: {e}")
            return None
    
    def _rule_based_analysis(self, question):
        """Original rule-based question analysis"""
        q = question.lower()
        
        analysis = {
            'intent': 'search',
            'tables': [],  # Will be populated with ALL tables
            'keywords': [],
            'limit': 50
        }
        
        # Simple intent detection
        if any(p in q for p in ['how many', 'count', 'total', 'number of']):
            analysis['intent'] = 'count'
        
        # 1. Extract "Complex Identifiers" FIRST (sequences with <, >, :, -, .)
        # This MUST happen before filters to avoid scientific IDs being split by colons
        import re
        
        # Capture any sequence of alphanumerics + special ID characters
        potential_complex = re.findall(r'[a-z0-9_<>:.-]+', q)
        extracted_ids = []
        for cid in potential_complex:
            # Must have at least one special char AND be at least 4 chars long
            if any(c in cid for c in '<>:-') and len(cid) >= 4:
                extracted_ids.append(cid.upper())
                # Consume from string to prevent filter or word logic from seeing it
                q = q.replace(cid, " ")
        
        analysis['keywords'] = extracted_ids
        
        # 2. Extract specifically formatted filters from the REMAINING string
        filters = {}
        # Matches "Key: Value" or "Key=Value"
        filter_matches = re.findall(r'(\w+)\s*[:=]\s*(\w+)', q)
        filter_keys = set()
        for field, value in filter_matches:
            if len(field) >= 3 and len(value) >= 2:
                filters[field] = value.upper()
                filter_keys.add(field)
                # Consume to avoid word logic double-dipping
                q = q.replace(field + ":" + value, " ").replace(field + "=" + value, " ")
        
        analysis['filters'] = filters
        
        # 3. Extract standard words as keywords from what's left
        words = re.findall(r'\b[a-z0-9_]{3,}\b', q)
        analysis['keywords'].extend([w.upper() for w in words if w not in filter_keys and w not in ['the', 'and', 'for', 'with', 'from', 'where', 'what', 'find', 'search', 'show', 'list', 'how', 'many', 'this', 'that', 'are', 'was', 'were', 'been', 'have', 'has', 'had', 'did', 'does', 'doing', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'need', 'needs', 'dare', 'dares', 'ought', 'used', 'use', 'using', 'get', 'got', 'gets', ' gotten', 'become', 'became', 'becomes', 'seem', 'seems', 'seemed', 'look', 'looks', 'looked', 'appear', 'appears', 'appeared', 'happen', 'happens', 'happened', 'try', 'tries', 'tried', 'trying', 'want', 'wants', 'wanted', 'wanting', 'like', 'likes', 'liked', 'liking', 'love', 'loves', 'loved', 'loving', 'hate', 'hates', 'hated', 'hating', 'prefer', 'prefers', 'preferred', 'preferring', 'hope', 'hopes', 'hoped', 'hoping', 'wish', 'wishes', 'wished', 'wishing', 'believe', 'believes', 'believed', 'believing', 'think', 'thinks', 'thought', 'thinking', 'suppose', 'supposes', 'supposed', 'supposing', 'expect', 'expects', 'expected', 'expecting', 'imagine', 'imagines', 'imagined', 'imagining', 'feel', 'feels', 'felt', 'feeling', 'see', 'sees', 'saw', 'seen', 'seeing', 'watch', 'watches', 'watched', 'watching', 'hear', 'hears', 'heard', 'hearing', 'notice', 'notices', 'noticed', 'noticing', 'let', 'lets', 'letting', 'make', 'makes', 'made', 'making', 'help', 'helps', 'helped', 'helping', 'force', 'forces', 'forced', 'forcing', 'drive', 'drives', 'drove', 'driven', 'driving']])
        
        # Also extract quoted strings as-is
        quoted = re.findall(r'["\']([^"\']+)["\']', question)
        analysis['keywords'].extend([q.strip().upper() for q in quoted])
        
        # Remove duplicates
        analysis['keywords'] = list(dict.fromkeys(analysis['keywords']))
        
        return analysis
    
    def _extract_keywords_enhanced(self, question):
        """Enhanced keyword extraction for ML models"""
        import re
        
        keywords = []
        
        # Extract complex identifiers (sample IDs, etc.)
        complex_ids = re.findall(r'\b[a-zA-Z0-9_<>:.-]{4,}\b', question)
        keywords.extend([cid.upper() for cid in complex_ids if any(c in cid for c in '<>:-')])
        
        # Extract quoted phrases
        quoted = re.findall(r'["\']([^"\']+)["\']', question)
        keywords.extend([q.strip().upper() for q in quoted])
        
        # Extract important words (filter out common words)
        stop_words = {'the', 'and', 'for', 'with', 'from', 'where', 'what', 'find', 'search', 'show', 'list', 'how', 'many', 'this', 'that', 'are', 'was', 'were', 'been', 'have', 'has', 'had', 'did', 'does', 'doing', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'need', 'needs', 'dare', 'dares', 'ought', 'used', 'use', 'using', 'get', 'got', 'gets', 'gotten', 'become', 'became', 'becomes', 'seem', 'seems', 'seemed', 'look', 'looks', 'looked', 'appear', 'appears', 'appeared', 'happen', 'happens', 'happened', 'try', 'tries', 'tried', 'trying', 'want', 'wants', 'wanted', 'wanting', 'like', 'likes', 'liked', 'liking', 'love', 'loves', 'loved', 'loving', 'hate', 'hates', 'hated', 'hating', 'prefer', 'prefers', 'preferred', 'preferring', 'hope', 'hopes', 'hoped', 'hoping', 'wish', 'wishes', 'wished', 'wishing', 'believe', 'believes', 'believed', 'believing', 'think', 'thinks', 'thought', 'thinking', 'suppose', 'supposes', 'supposed', 'supposing', 'expect', 'expects', 'expected', 'expecting', 'imagine', 'imagines', 'imagined', 'imagining', 'feel', 'feels', 'felt', 'feeling', 'see', 'sees', 'saw', 'seen', 'seeing', 'watch', 'watches', 'watched', 'watching', 'hear', 'hears', 'heard', 'hearing', 'notice', 'notices', 'noticed', 'noticing', 'let', 'lets', 'letting', 'make', 'makes', 'made', 'making', 'help', 'helps', 'helped', 'helping', 'force', 'forces', 'forced', 'forcing', 'drive', 'drives', 'drove', 'driven', 'driving'}
        
        words = re.findall(r'\b[a-zA-Z0-9_]{3,}\b', question.lower())
        keywords.extend([w.upper() for w in words if w not in stop_words])
        
        # Remove duplicates and return
        return list(dict.fromkeys(keywords))
    
    def generate_sql(self, analysis):
        """Generate SQL query - use ML predictions when available, fallback to all tables"""
        schema = self.get_schema()
        
        if not schema:
            return None, "Could not access database schema"
        
        results = []
        
        # If ML analysis provided specific tables, use them first
        if 'ml_confidence' in analysis and analysis['tables']:
            # Use ML-predicted tables with high confidence
            target_tables = analysis['tables']
            print(f"🎯 ML targeting tables: {target_tables}")
            
            for table in target_tables:
                if table in schema:
                    query_result = self._generate_table_query(table, analysis, schema)
                    if query_result:
                        results.append(query_result)
            
            # If no results from ML-predicted tables, fallback to broader search
            if not results:
                print("🔄 ML tables yielded no results, expanding search...")
                all_tables = list(schema.keys())
                for table in all_tables:
                    if table not in target_tables:
                        query_result = self._generate_table_query(table, analysis, schema)
                        if query_result:
                            results.append(query_result)
        else:
            # Fallback to original approach - search all tables
            all_tables = list(schema.keys())
            analysis['tables'] = all_tables  # Update analysis with all tables
            
            for table in all_tables:
                query_result = self._generate_table_query(table, analysis, schema)
                if query_result:
                    results.append(query_result)
        
        return results, None
    
    def _generate_table_query(self, table, analysis, schema):
        """Generate SQL query for a specific table"""
        try:
            columns = schema[table]['columns']
            if not columns:
                return None
            
            col_names = [c['name'] for c in columns]
            
            # Determine quoting and placeholder
            q = '"' if self.db_type == 'sqlite' else '`'
            placeholder = '?' if self.db_type == 'sqlite' else '%s'
            
            # 1. Build conditions for specific filters (Field: Value)
            filter_conditions = []
            filter_params = []
            
            for field, value in analysis.get('filters', {}).items():
                matched_col = None
                # Try exact match first
                for col in col_names:
                    if col.lower() == field.lower():
                        matched_col = col
                        break
                # Then fuzzy substring match
                if not matched_col:
                    for col in col_names:
                        if field.lower() in col.lower() or col.lower() in field.lower():
                            matched_col = col
                            break
                
                if matched_col:
                    filter_conditions.append(f'LOWER({q}{matched_col}{q}) LIKE LOWER({placeholder})')
                    filter_params.append(f'%{value}%')
            
            # 2. Build conditions for general keywords
            keyword_conditions = []
            keyword_params = []
            
            for keyword in analysis['keywords']:
                keyword_str = str(keyword).strip()
                if len(keyword_str) < 2:
                    continue
                
                for col in col_names:
                    keyword_conditions.append(f'LOWER({q}{col}{q}) LIKE LOWER({placeholder})')
                    keyword_params.append(f'%{keyword_str}%')
            
            # Combine conditions
            params = []
            where_clause = ""
            
            if filter_conditions:
                # If we have filters that match this table's columns, use AND logic
                where_clause = " WHERE (" + " AND ".join(filter_conditions) + ")"
                params.extend(filter_params)
                
                # If we ALSO have general keywords, further filter the results
                if keyword_conditions:
                    where_clause += " AND (" + " OR ".join(keyword_conditions) + ")"
                    params.extend(keyword_params)
            elif keyword_conditions:
                # If no filters match, fall back to general keyword OR search
                where_clause = " WHERE (" + " OR ".join(keyword_conditions) + ")"
                params.extend(keyword_params)
            
            # Construct final SQL
            if analysis['intent'] == 'count':
                sql = f'SELECT COUNT(*) FROM {q}{table}{q}{where_clause}'
            else:
                sql = f'SELECT * FROM {q}{table}{q}{where_clause}'
                sql += f' LIMIT {analysis["limit"]}'
            
            if where_clause:
                return {
                    'table': table,
                    'sql': sql,
                    'params': params,
                    'intent': analysis['intent']
                }
            
        except Exception as e:
            print(f"Error generating query for {table}: {e}")
        
        return None
    
    def execute_queries(self, queries):
        """Execute generated queries and format results"""
        if not queries:
            return None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            all_results = []
            
            for query_info in queries:
                try:
                    cursor.execute(query_info['sql'], query_info['params'])
                    
                    if query_info['intent'] == 'count':
                        count = cursor.fetchone()[0]
                        all_results.append({
                            'table': query_info['table'],
                            'type': 'count',
                            'count': count
                        })
                    else:
                        rows = cursor.fetchall()
                        if rows:
                            columns = [desc[0] for desc in cursor.description]
                            all_results.append({
                                'table': query_info['table'],
                                'type': 'data',
                                'columns': columns,
                                'rows': rows
                            })
                        
                except Exception as e:
                    print(f"Query error for {query_info['table']}: {e}")
                    continue
            
            conn.close()
            return all_results
            
        except Exception as e:
            return None
    
    def format_response(self, question, results, analysis):
        """Format query results with ML-enhanced responses"""
        if not results:
            return self._get_help_message(question)
        
        # Check if we have ML confidence info and use enhanced formatting
        if 'ml_confidence' in analysis:
            return self._format_ml_response(question, results, analysis)
        
        # For count queries, show simple summary
        if analysis['intent'] == 'count':
            lines = ["📊 **Record Counts**"]
            for result in results:
                if result['type'] == 'count':
                    lines.append(f"• **{result['table']}**: {result['count']:,} records")
            return "\n".join(lines)
        
        # For specific record searches, use enhanced format
        if analysis['intent'] == 'search':
            return self._format_search_response(question, results, analysis)
        
        # Fallback for unexpected cases
        return "I found some data but couldn't format it properly. Try a more specific question."
    
    def _format_ml_response(self, question, results, analysis):
        """Format response with ML-enhanced information"""
        response_parts = []
        
        # Add ML confidence info
        if 'ml_confidence' in analysis:
            confidence = analysis['ml_confidence']
            response_parts.append(f"🤖 **AI Analysis** (Confidence: {(confidence.get('intent', 0) * 100):.1f}%)")
        
        # Format results based on ML predictions
        results_found = False
        for result in results:
            if result['type'] == 'data' and result['rows']:
                results_found = True
                table_name = result['table']
                rows = result['rows']
                columns = result['columns']
                
                # Table header with emoji
                emoji = self._get_table_emoji(table_name)
                response_parts.append(f"\n{emoji} **Found in {table_name}** ({len(rows)} records)")
                
                # Smart formatting based on table type and row count
                if len(rows) == 1:
                    # Single record - show key details
                    row_data = dict(zip(columns, rows[0]))
                    response_parts.extend(self._format_single_record(table_name, row_data, question))
                else:
                    # Multiple records - show summary
                    response_parts.extend(self._format_multiple_records(table_name, rows, columns))
        
        if not results_found:
            response_parts.append(f"\n❌ No matching records found for '{question}'")
        
        return "\n".join(response_parts).strip()
    
    def _format_search_response(self, question, results, analysis):
        """Format search results with enhanced display"""
        response_parts = []
        results_found = False
        
        for result in results:
            if result['type'] == 'data' and result['rows']:
                results_found = True
                table_name = result['table']
                rows = result['rows']
                columns = result['columns']
                
                # Table header
                emoji = self._get_table_emoji(table_name)
                response_parts.append(f"\n{emoji} **Found in {table_name}**")
                
                if len(rows) == 1:
                    # Single record - simplified format
                    row_data = dict(zip(columns, rows[0]))
                    response_parts.extend(self._format_single_record(table_name, row_data, question))
                else:
                    # Multiple records - summary list
                    response_parts.extend(self._format_multiple_records(table_name, rows, columns))
        
        if not results_found:
            return self._get_help_message(question)
        
        return "\n".join(response_parts).strip()
    
    def _format_single_record(self, table_name, row_data, question=None):
        """Format a single record with smart field selection and intelligent related data discovery"""
        parts = []
        
        # Smart field mapping based on table type (keep this for basic formatting)
        if 'sample' in table_name.lower():
            parts.append("📋 **Sample Information**")
            key_fields = ['sample_id', 'source_id', 'sample_origin', 'collection_date', 'tissue_type']
        elif 'host' in table_name.lower() or 'bathost' in table_name.lower():
            parts.append("🦇 **Host Information**")
            key_fields = ['field_id', 'species', 'scientific_name', 'sex', 'weight', 'location']
        elif 'screening' in table_name.lower():
            parts.append("🧪 **Screening Results**")
            key_fields = ['sample_id', 'test_type', 'result', 'test_date', 'team']
        elif 'location' in table_name.lower():
            parts.append("📍 **Location Information**")
            key_fields = ['country', 'province', 'district', 'site_name']
        elif 'storage' in table_name.lower():
            parts.append("❄️ **Storage Information**")
            key_fields = ['freezer_no', 'rack', 'box', 'position', 'sample_tube_id']
        else:
            parts.append(f"📋 **{table_name.title()} Information**")
            key_fields = self._get_important_columns(list(row_data.keys()))[:5]
        
        # Display key fields
        for field in key_fields:
            if field in row_data and row_data[field] is not None and str(row_data[field]).strip():
                display_name = field.replace('_', ' ').title()
                value = str(row_data[field])
                # Truncate long values
                if len(value) > 100:
                    value = value[:97] + "..."
                parts.append(f"• **{display_name}**: {value}")
        
        # If this is a sample record, fetch related data using intelligent discovery
        if 'sample' in table_name.lower() and len(parts) > 1:
            sample_id = row_data.get('sample_id') or row_data.get('id') or row_data.get('sample_code')
            if sample_id:
                related_data = self._fetch_intelligent_related_data(sample_id, row_data, question)
                if related_data:
                    parts.extend(related_data)
        
        return parts
    
    def _fetch_intelligent_related_data(self, sample_id, sample_data, question=None):
        """Fetch related data using intelligent table relationship discovery"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            related_parts = []
            
            print(f"DEBUG: Starting intelligent related data search for sample_id: {sample_id}")
            print(f"DEBUG: Original question: {question}")
            print(f"DEBUG: Sample data keys: {list(sample_data.keys())}")
            
            # Get database schema for intelligent discovery
            schema = self.get_schema()
            if not schema:
                conn.close()
                return None
            
            # Try the intelligent discovery first
            try:
                related_data = self._discover_and_fetch_related_data(cursor, schema, sample_id, sample_data)
                print(f"DEBUG: Found related data in {len(related_data)} tables: {list(related_data.keys())}")
            except Exception as e:
                print(f"DEBUG: Intelligent discovery failed: {e}")
                print(f"DEBUG: Falling back to simple approach")
                related_data = {}
            
            # If intelligent discovery failed, try a simple direct approach
            if not related_data:
                print(f"DEBUG: Trying simple direct approach")
                related_data = self._simple_related_data_search(cursor, sample_id, sample_data, question)
            
            # Format the discovered related data
            for table_name, data in related_data.items():
                if data and len(data) > 0:
                    print(f"DEBUG: Formatting {len(data)} records from {table_name}")
                    # Get appropriate emoji and title for the table
                    emoji = self._get_table_emoji(table_name)
                    title = table_name.replace('_', ' ').title()
                    
                    related_parts.append(f"{emoji} **{title}**")
                    
                    if isinstance(data, list):
                        # Multiple records (like screening results)
                        related_parts.append(f"({len(data)} records)")
                        
                        # For screening results, prioritize the one that matches the search term
                        if 'screening' in table_name.lower():
                            # Get the original search term from question
                            search_term = None
                            if question:
                                # Extract tube ID from the question - more flexible patterns
                                import re
                                tube_patterns = [
                                    r'\b[A-Z]{3,}[_-]?[A-Z0-9_]+\d+\b',  # Any 3+ letter prefix with numbers
                                    r'\b[A-Z]+[_-]?\d+[_\d]*\b',      # General pattern with letters and numbers
                                    r'\b\d{3,}[_-]?[A-Z]+\b',          # Numbers followed by letters
                                    r'\b[A-Z]+\d+[_\d]*\b',            # Letters + numbers
                                ]
                                for pattern in tube_patterns:
                                    match = re.search(pattern, question, re.IGNORECASE)
                                    if match:
                                        search_term = match.group().upper()
                                        print(f"DEBUG: Found screening search term: {search_term}")
                                        break
                            
                            # Sort records to prioritize matching tested_sample_id
                            if search_term:
                                def match_priority(record):
                                    if record.get('tested_sample_id') == search_term:
                                        return 0  # Highest priority
                                    elif search_term in str(record.get('tested_sample_id', '')):
                                        return 1  # Medium priority
                                    else:
                                        return 2  # Lowest priority
                                
                                data.sort(key=match_priority)
                                print(f"DEBUG: Prioritized screening results for search term: {search_term}")
                        
                        for i, record in enumerate(data[:3], 1):  # Limit to 3 for display
                            related_parts.append(f"**Record {i}**")
                            for key, value in record.items():
                                if value and key != 'sample_id':
                                    display_name = key.replace('_', ' ').title()
                                    related_parts.append(f"• **{display_name}**: {value}")
                        
                        if len(data) > 3:
                            related_parts.append(f"... and {len(data) - 3} more records")
                    else:
                        # Single record (like host or storage info)
                        for key, value in data.items():
                            if value:
                                display_name = key.replace('_', ' ').title()
                                related_parts.append(f"• **{display_name}**: {value}")
            
            # Add intelligent summary
            if related_parts:
                summary_parts = self._generate_intelligent_summary(related_data, sample_id)
                if summary_parts:
                    related_parts.append("")
                    related_parts.extend(summary_parts)
            
            conn.close()
            return related_parts if related_parts else None
            
        except Exception as e:
            print(f"Error in intelligent related data fetch: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _simple_related_data_search(self, cursor, sample_id, sample_data, question=None):
        """Simple direct search for related data"""
        related_data = {}
        
        try:
            # Extract the searched tube ID from the question
            searched_tube_id = None
            if question:
                import re
                # More flexible tube ID patterns - works with any format
                tube_patterns = [
                    r'\b[A-Z]{3,}[_-]?[A-Z0-9_]+\d+\b',  # Any 3+ letter prefix with numbers
                    r'\b[A-Z]+[_-]?\d+[_\d]*\b',      # General pattern with letters and numbers
                    r'\b\d{3,}[_-]?[A-Z]+\b',          # Numbers followed by letters
                    r'\b[A-Z]+\d+[_\d]*\b',            # Letters + numbers
                ]
                for pattern in tube_patterns:
                    match = re.search(pattern, question, re.IGNORECASE)
                    if match:
                        searched_tube_id = match.group().upper()
                        print(f"DEBUG: Found tube ID with pattern {pattern}: {searched_tube_id}")
                        break
            
            print(f"DEBUG: *** SEARCHED TUBE ID FROM QUESTION: {searched_tube_id} ***")
            
            # Extract key identifiers
            identifiers = {
                'sample_id': str(sample_data.get('sample_id', '')),
                'host_id': str(sample_data.get('host_id', '')),
                'location_id': str(sample_data.get('location_id', '')),
                'tissue_id': str(sample_data.get('tissue_id', '')),
                'source_id': str(sample_data.get('source_id', '')),
                'field_id': str(sample_data.get('field_id', '')),
                'tested_sample_id': str(sample_data.get('tested_sample_id', '')),
                'sample_tube_id': str(sample_data.get('sample_tube_id', '')),
                'intestine_id': str(sample_data.get('intestine_id', '')),
                'plasma_id': str(sample_data.get('plasma_id', '')),
                'blood_id': str(sample_data.get('blood_id', '')),
                'saliva_id': str(sample_data.get('saliva_id', '')),
                'anal_id': str(sample_data.get('anal_id', '')),
                'urine_id': str(sample_data.get('urine_id', '')),
                'ecto_id': str(sample_data.get('ecto_id', '')),
                'env_sample_id': str(sample_data.get('env_sample_id', '')),
                'adipose_id': str(sample_data.get('adipose_id', ''))
            }
            
            print(f"DEBUG: Simple search with identifiers: {identifiers}")
            
            # Get ALL tables in the database
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            all_tables = [str(row[0]) for row in cursor.fetchall() if row and row[0] is not None]
            
            # Skip system tables
            skip_tables = ['RecycleBin', 'security_users', 'security_roles', 'security_audit_log', 'security_backup_log', 
                          'security_schema_protection', 'security_row_policies', 'security_encrypted_fields']
            
            q = '"' if self.db_type == 'sqlite' else '`'
            
            print(f"DEBUG: Searching all {len(all_tables)} tables (skipping {len(skip_tables)})")
            
            for table_name in all_tables:
                if table_name in skip_tables:
                    print(f"DEBUG: Skipping system table {table_name}")
                    continue
                
                try:
                    # Get table structure
                    if self.db_type == 'sqlite':
                        cursor.execute(f'PRAGMA table_info("{table_name}")')
                        table_info = {'columns': [{'name': col[1]} for col in cursor.fetchall()]}
                    else:
                        cursor.execute(f'DESCRIBE `{table_name}`')
                        table_info = {'columns': [{'name': col[0]} for col in cursor.fetchall()]}
                    columns = [col['name'] for col in table_info['columns']]
                    
                    print(f"DEBUG: Checking table {table_name} with columns: {columns}")
                    
                    # Special handling for storage_locations table
                    if 'storage' in table_name.lower():
                        print(f"DEBUG: *** STORAGE TABLE FOUND: {table_name} ***")
                        print(f"DEBUG: Special handling for storage table {table_name}")
                        
                        print(f"DEBUG: *** SEARCHED TUBE ID: {searched_tube_id} ***")
                        
                        # Use the searched tube ID for storage search
                        if searched_tube_id:
                            # More flexible storage column detection
                            storage_identifiers = {}
                            
                            # Try common storage column names
                            possible_storage_columns = [
                                'sample_tube_id', 'tube_id', 'tissue_id', 'tested_sample_id',
                                'sample_code', 'tube_code', 'tissue_code', 'sample_identifier',
                                'storage_tube_id', 'container_id', 'specimen_id'
                            ]
                            
                            for col_name in possible_storage_columns:
                                if col_name in columns:
                                    storage_identifiers[col_name] = searched_tube_id
                            
                            print(f"DEBUG: Storage identifiers: {storage_identifiers}")
                            
                            found_records = None
                            for id_field, id_value in storage_identifiers.items():
                                if id_value and id_field in columns:
                                    try:
                                        query = f"SELECT * FROM {q}{table_name}{q} WHERE {q}{id_field}{q} = ? LIMIT 10"
                                        print(f"DEBUG: Storage query: {query} with {id_value}")
                                        cursor.execute(query, (id_value,))
                                        rows = cursor.fetchall()
                                        
                                        if rows:
                                            records = []
                                            for row in rows:
                                                record = dict(zip(columns, row))
                                                # Less strict filtering - keep more fields
                                                filtered_record = {}
                                                for k, v in record.items():
                                                    if v is not None and str(v).strip():
                                                        filtered_record[k] = v
                                                if filtered_record:
                                                    records.append(filtered_record)
                                            
                                            if records:
                                                found_records = records
                                                print(f"DEBUG: ✅ Found {len(records)} storage records using {id_field}")
                                                print(f"DEBUG: Sample storage record: {records[0]}")
                                                break
                                    except Exception as e:
                                        print(f"DEBUG: ❌ Storage query failed for {id_field}: {e}")
                                        continue
                            
                            if found_records:
                                related_data[table_name] = found_records
                                print(f"DEBUG: Added {table_name} to related data")
                            else:
                                print(f"DEBUG: No storage records found in {table_name}")
                        else:
                            print(f"DEBUG: No searched tube ID found for storage search")
                        continue  # Skip the general search for storage tables
                    
                    # General search for other tables
                    found_records = None
                    for id_field, id_value in identifiers.items():
                        if id_value and id_field in columns:
                            try:
                                query = f"SELECT * FROM {q}{table_name}{q} WHERE {q}{id_field}{q} = ? LIMIT 10"
                                print(f"DEBUG: Trying query: {query} with {id_value}")
                                cursor.execute(query, (id_value,))
                                rows = cursor.fetchall()
                                
                                if rows:
                                    records = []
                                    for row in rows:
                                        record = dict(zip(columns, row))
                                        # Less strict filtering - keep more fields
                                        filtered_record = {}
                                        for k, v in record.items():
                                            if v is not None and str(v).strip():
                                                filtered_record[k] = v
                                        if filtered_record:
                                            records.append(filtered_record)
                                    
                                    if records:
                                        found_records = records
                                        print(f"DEBUG: ✅ Found {len(records)} records in {table_name} using {id_field}")
                                        print(f"DEBUG: Sample record: {records[0]}")
                                        break
                            except Exception as e:
                                print(f"DEBUG: ❌ Query failed for {table_name}.{id_field}: {e}")
                                continue
                        
                        if found_records:
                            break
                    
                    if found_records:
                        related_data[table_name] = found_records
                        print(f"DEBUG: Added {table_name} to related data")
                    else:
                        print(f"DEBUG: No matching records found in {table_name}")
                            
                except Exception as e:
                    print(f"DEBUG: Error in simple search for {table_name}: {e}")
                    continue
            
            # Special case: If we found host data, also fetch taxonomy information
            if 'hosts' in related_data:
                host_records = related_data['hosts']
                for host_record in host_records:
                    taxonomy_id = host_record.get('taxonomy_id')
                    if taxonomy_id:
                        try:
                            # Check if taxonomy table exists
                            if self.db_type == 'sqlite':
                                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", ('taxonomy',))
                            else:
                                cursor.execute("SHOW TABLES LIKE 'taxonomy'")
                            if cursor.fetchone():
                                cursor.execute(f'SELECT * FROM {q}taxonomy{q} WHERE {q}taxonomy_id{q} = {p} LIMIT 1', (str(taxonomy_id),))
                                taxonomy_row = cursor.fetchone()
                                if taxonomy_row:
                                    if self.db_type == 'sqlite':
                                        cursor.execute(f'PRAGMA table_info("taxonomy")')
                                        taxonomy_columns = [col[1] for col in cursor.fetchall()]
                                    else:
                                        cursor.execute(f'DESCRIBE `taxonomy`')
                                        taxonomy_columns = [col[0] for col in cursor.fetchall()]
                                    taxonomy_data = dict(zip(taxonomy_columns, taxonomy_row))
                                    
                                    # Add taxonomy fields to host record
                                    for key, value in taxonomy_data.items():
                                        if value and key not in host_record:
                                            host_record[f'taxonomy_{key}'] = value
                                    
                                    print(f"DEBUG: Added taxonomy data for taxonomy_id {taxonomy_id}")
                        except Exception as e:
                            print(f"DEBUG: Error fetching taxonomy for taxonomy_id {taxonomy_id}: {e}")
            
            print(f"DEBUG: Simple search results: {list(related_data.keys())}")
            print(f"DEBUG: Total tables with data: {len(related_data)}")
            
            # Debug: Show what we found for storage_locations specifically
            if 'storage_locations' in related_data:
                print(f"DEBUG: Storage locations found: {len(related_data['storage_locations'])} records")
                for i, record in enumerate(related_data['storage_locations']):
                    print(f"DEBUG: Storage record {i+1}: {record}")
            else:
                print(f"DEBUG: Storage locations NOT found in results")
            
            return related_data
            
        except Exception as e:
            print(f"Error in simple related data search: {e}")
            return {}
    
    def _discover_and_fetch_related_data(self, cursor, schema, sample_id, sample_data):
        """Discover table relationships and fetch related data intelligently"""
        related_data = {}
        
        # Extract key identifiers from the sample data
        identifiers = self._extract_identifiers(sample_data)
        
        print(f"DEBUG: Schema has {len(schema)} tables: {list(schema.keys())}")
        
        # Search through all tables for related data
        for table_name, table_info in schema.items():
            if table_name.lower() in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin']:
                continue
            
            # Skip the original sample table
            if 'sample' in table_name.lower() and table_name in identifiers.get('source_table', ''):
                continue
            
            print(f"DEBUG: Checking table {table_name}")
            
            # Look for relationships
            related_records = self._find_related_records(cursor, table_name, table_info, identifiers)
            if related_records:
                related_data[table_name] = related_records
        
        return related_data
    
    def _extract_identifiers(self, sample_data):
        """Extract various identifiers from sample data for relationship discovery"""
        identifiers = {
            'sample_id': None,
            'field_id': None,
            'source_id': None,
            'tested_sample_id': None,
            'sample_tube_id': None,
            'source_table': None
        }
        
        print(f"DEBUG: Extracting identifiers from sample_data: {sample_data}")
        
        # Extract all possible ID fields and ensure they are strings
        for key, value in sample_data.items():
            if value and 'id' in key.lower():
                identifiers[key] = str(value)
                print(f"DEBUG: Found identifier {key}: {str(value)}")
        
        # Determine source table if possible
        for key in sample_data.keys():
            if 'sample' in key.lower() and 'id' in key.lower():
                identifiers['source_table'] = 'samples'
                print(f"DEBUG: Set source_table to samples based on key: {key}")
                break
        
        # Also check for sample_id specifically
        if 'sample_id' in sample_data and sample_data['sample_id']:
            identifiers['sample_id'] = str(sample_data['sample_id'])
            print(f"DEBUG: Set sample_id from direct field: {sample_data['sample_id']}")
        
        print(f"DEBUG: Final identifiers: {identifiers}")
        return identifiers
    
    def _find_related_records(self, cursor, table_name, table_info, identifiers):
        """Find records in a table that relate to the sample"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            
            print(f"DEBUG: _find_related_records called for {table_name}")
            print(f"DEBUG: table_info type: {type(table_info)}")
            print(f"DEBUG: identifiers type: {type(identifiers)}")
            
            # Check if table_info has the expected structure
            if not table_info or 'columns' not in table_info:
                print(f"DEBUG: Invalid table_info for {table_name}: {table_info}")
                return None
            
            columns = [col['name'] for col in table_info['columns']]
            
            print(f"DEBUG: Searching {table_name} with columns: {columns}")
            print(f"DEBUG: Available identifiers: {identifiers}")
            
            # Try each identifier individually with a simple approach
            for id_field, id_value in identifiers.items():
                if id_value and id_field in columns:
                    try:
                        # Ensure id_value is a simple string, handle complex types
                        if isinstance(id_value, (int, float)):
                            param_value = str(id_value)
                        elif isinstance(id_value, str):
                            param_value = id_value
                        else:
                            # Handle other types (like None, complex objects)
                            param_value = str(id_value) if id_value is not None else ""
                        
                        # Simple query with proper parameter handling
                        query = f"SELECT * FROM {q}{table_name}{q} WHERE {q}{id_field}{q} = ? LIMIT 10"
                        
                        print(f"DEBUG: Trying query: {query}")
                        print(f"DEBUG: Param type: {type(param_value)}, value: {repr(param_value)}")
                        
                        # Execute with proper parameter handling
                        cursor.execute(query, (param_value,))
                        rows = cursor.fetchall()
                        
                        if rows:
                            # Convert to list of dictionaries
                            records = []
                            for row in rows:
                                record = dict(zip(columns, row))
                                # Filter out empty/null values
                                filtered_record = {k: v for k, v in record.items() if v is not None and str(v).strip()}
                                if filtered_record:
                                    records.append(filtered_record)
                            
                            print(f"DEBUG: ✅ Found {len(records)} related records in {table_name} using {id_field}")
                            return records if records else None
                        else:
                            print(f"DEBUG: No results for {id_field} in {table_name}")
                            
                    except Exception as e:
                        print(f"DEBUG: ❌ Query failed for {id_field} in {table_name}: {e}")
                        print(f"DEBUG:   id_value type: {type(id_value)}, value: {repr(id_value)}")
                        continue
                else:
                    print(f"DEBUG: Skipping {id_field} - not in columns or no value")
            
            print(f"DEBUG: No records found in {table_name} with any identifier")
            return None
            
        except Exception as e:
            print(f"Error finding related records in {table_name}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _generate_intelligent_summary(self, related_data, sample_id):
        """Generate an intelligent summary based on discovered related data"""
        summary_parts = []
        summary_parts.append("📊 **Profile Summary**")
        summary_parts.append(f"• Sample ID: {sample_id}")
        
        # Extract key information from related data
        host_info = None
        location_info = None
        screening_count = 0
        storage_available = False
        
        for table_name, data in related_data.items():
            if isinstance(data, list):
                if 'screening' in table_name.lower():
                    screening_count = len(data)
            else:
                if 'host' in table_name.lower() or 'bathost' in table_name.lower():
                    host_info = data
                elif 'location' in table_name.lower():
                    location_info = data
                elif 'storage' in table_name.lower():
                    storage_available = True
        
        # Add summary items
        if host_info and host_info.get('species'):
            summary_parts.append(f"• Host: {host_info['species']}")
        
        if location_info:
            if location_info.get('province'):
                summary_parts.append(f"• Location: {location_info['province']}")
            elif location_info.get('country'):
                summary_parts.append(f"• Location: {location_info['country']}")
        
        if screening_count > 0:
            summary_parts.append(f"• Screening Tests: {screening_count}")
        
        if storage_available:
            summary_parts.append("• Storage: Available")
        
        return summary_parts
    
    def _build_sample_profile(self, cursor, sample_id):
        """Build comprehensive sample profile using recursive FK discovery (no hardcoding)"""
        try:
            print(f"DEBUG: Building recursive FK profile for sample_id: {sample_id}")
            
            # First try to find the sample by string ID (like CANB_TIS23_L_075)
            # by checking screening_results first
            numeric_sample_id = self._find_numeric_sample_id(cursor, sample_id)
            
            if numeric_sample_id:
                print(f"DEBUG: Found numeric sample_id {numeric_sample_id} for string ID {sample_id}")
                profile = self._build_recursive_fk_profile(cursor, 'samples', numeric_sample_id, 'sample_id')
            else:
                # Try direct lookup with original ID
                profile = self._build_recursive_fk_profile(cursor, 'samples', sample_id, 'sample_id')
            
            # Convert recursive profile to expected format for compatibility
            return self._convert_recursive_to_legacy_format(profile)
            
        except Exception as e:
            print(f"DEBUG: Error building dynamic FK profile for {sample_id}: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback to empty profile
            return {
                'sample_info': {},
                'host_info': {},
            'related_data': {},
            'fk_paths_used': [],
            'summary': []
        }
        
        try:
            print(f"DEBUG: Building dynamic profile for {table_name}.{id_column} = {record_id}")
            
            # Step 1: Get the main record
            main_record = self._get_record_by_id(cursor, table_name, id_column, record_id)
            if not main_record:
                print(f"DEBUG: Record not found: {table_name}.{id_column} = {record_id}")
                return profile
            
            profile['main_record'] = main_record
            print(f"DEBUG: Found main record: {table_name}")
            
            # Step 2: Discover and follow all FK relationships
            fk_relationships = self._get_foreign_key_relationships(table_name, cursor)
            print(f"DEBUG: Found {len(fk_relationships)} FK relationships for {table_name}")
            
            # Step 3: Follow forward relationships (main table -> related tables)
            for fk in fk_relationships:
                if fk['type'] == 'forward':
                    related_data = self._follow_forward_fk_dynamic(cursor, main_record, fk)
                    if related_data:
                        table_key = f"{fk['to_table']}_info"
                        profile['related_data'][table_key] = related_data
                        profile['fk_paths_used'].append(f"FORWARD: {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}")
                        print(f"DEBUG: Found {fk['to_table']} via forward FK")
            
            # Step 4: Follow reverse relationships (related tables -> main table)
            for fk in fk_relationships:
                if fk['type'] == 'reverse':
                    related_data = self._follow_reverse_fk_dynamic(cursor, main_record, fk)
                    if related_data:
                        table_key = f"{fk['from_table']}_data"
                        profile['related_data'][table_key] = related_data
                        profile['fk_paths_used'].append(f"REVERSE: {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}")
                        print(f"DEBUG: Found {len(related_data)} records in {fk['from_table']} via reverse FK")
            
            # Step 5: Build summary
            profile['summary'] = self._build_dynamic_summary(profile)
            
        except Exception as e:
            print(f"DEBUG: Error building dynamic profile: {e}")
            import traceback
            traceback.print_exc()
        
        return profile
    
    def _get_record_by_id(self, cursor, table_name, id_column, record_id):
        """Get record by ID dynamically"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            cursor.execute(f"SELECT * FROM {q}{table_name}{q} WHERE {q}{id_column}{q} = {p} LIMIT 1", (record_id,))
            row = cursor.fetchone()
            if row:
                # Get column names dynamically
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{table_name}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{table_name}`')
                    columns = [c[0] for c in cursor.fetchall()]
                return dict(zip(columns, row))
        except Exception as e:
            print(f"Error getting record {table_name}.{id_column} = {record_id}: {e}")
        
        return None
    
    def _follow_forward_fk_dynamic(self, cursor, main_record, fk):
        """Follow forward FK relationship dynamically"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Get the FK value from main record
            fk_value = main_record.get(fk['from_column'])
            if not fk_value:
                return None
            
            # Query the related table
            cursor.execute(f"SELECT * FROM {q}{fk['to_table']}{q} WHERE {q}{fk['to_column']}{q} = {p} LIMIT 1", (fk_value,))
            row = cursor.fetchone()
            if row:
                # Get column names dynamically
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{fk["to_table"]}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{fk["to_table"]}`')
                    columns = [c[0] for c in cursor.fetchall()]
                return dict(zip(columns, row))
        
        except Exception as e:
            print(f"Error following forward FK {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}: {e}")
        
        return None
    
    def _follow_reverse_fk_dynamic(self, cursor, main_record, fk):
        """Follow reverse FK relationship dynamically"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Get the primary key value from main record
            pk_value = main_record.get(fk['to_column'])
            if not pk_value:
                return None
            
            # Query for records that reference this main record
            cursor.execute(f"SELECT * FROM {q}{fk['from_table']}{q} WHERE {q}{fk['from_column']}{q} = {p} LIMIT 10", (pk_value,))
            rows = cursor.fetchall()
            if rows:
                # Get column names dynamically
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{fk["from_table"]}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{fk["from_table"]}`')
                    columns = [c[0] for c in cursor.fetchall()]
                return [dict(zip(columns, row)) for row in rows]
        
        except Exception as e:
            print(f"Error following reverse FK {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}: {e}")
        
        return []
    
    def _build_dynamic_summary(self, profile):
        """Build summary from dynamically discovered relationships"""
        summary_parts = []
        
        main_record = profile.get('main_record', {})
        related_data = profile.get('related_data', {})
        
        # Main record info
        if main_record:
            # Try to find a meaningful identifier
            for field in ['sample_id', 'name', 'title', 'id']:
                if field in main_record and main_record[field]:
                    summary_parts.append(f"• {field.replace('_', ' ').title()}: {main_record[field]}")
                    break
        
        # Related data summary
        for table_key, data in related_data.items():
            if isinstance(data, list):
                summary_parts.append(f"• {table_key.replace('_data', '').replace('_', ' ').title()}: {len(data)} records")
            elif data:
                # Try to find a meaningful field from single record
                for field in ['name', 'title', 'species', 'scientific_name', 'country', 'province']:
                    if field in data and data[field]:
                        summary_parts.append(f"• {table_key.replace('_info', '').replace('_', ' ').title()}: {data[field]}")
                        break
        
        return summary_parts
    
    def _convert_dynamic_to_legacy_format(self, dynamic_profile):
        """Convert dynamic profile to legacy format for compatibility"""
        related_data = dynamic_profile.get('related_data', {})
        
        return {
            'sample_info': dynamic_profile.get('main_record', {}),
            'host_info': related_data.get('hosts_info', {}) or related_data.get('hosts_data', [{}])[0] if related_data.get('hosts_data') else {},
            'taxonomy_info': related_data.get('taxonomy_info', {}) or related_data.get('taxonomy_data', [{}])[0] if related_data.get('taxonomy_data') else {},
            'location_info': related_data.get('locations_info', {}) or related_data.get('locations_data', [{}])[0] if related_data.get('locations_data') else {},
            'screening_results': related_data.get('screening_results_data', []) or related_data.get('screening_data', []),
            'sequencing_data': related_data.get('sequencing_data', []) or related_data.get('sequences_data', []),
            'storage_info': related_data.get('storage_locations_info', {}) or related_data.get('storage_locations_data', [{}])[0] if related_data.get('storage_locations_data') else {},
            'fk_debug_info': {
                'paths_used': dynamic_profile.get('fk_paths_used', []),
                'summary': dynamic_profile.get('summary', [])
            }
        }
    
    def _get_sample_details(self, cursor, sample_id):
        """Get detailed sample information"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Try to find sample by various ID fields
            sample_queries = [
                f"SELECT * FROM {q}samples{q} WHERE {q}sample_id{q} = {p} LIMIT 1",
                f"SELECT * FROM {q}samples{q} WHERE {q}tissue_id{q} = {p} LIMIT 1",
                f"SELECT * FROM {q}samples{q} WHERE {q}intestine_id{q} = {p} LIMIT 1",
                f"SELECT * FROM {q}samples{q} WHERE {q}plasma_id{q} = {p} LIMIT 1",
                f"SELECT * FROM {q}samples{q} WHERE {q}source_id{q} = {p} LIMIT 1"
            ]
            
            for query in sample_queries:
                cursor.execute(query, (sample_id,))
                row = cursor.fetchone()
                if row:
                    if self.db_type == 'sqlite':
                        cursor.execute(f'PRAGMA table_info("samples")')
                        columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f'DESCRIBE `samples`')
                        columns = [c[0] for c in cursor.fetchall()]
                    return dict(zip(columns, row))
            
            return None
            
        except Exception as e:
            print(f"Error getting sample details: {e}")
            return None
    
    def _get_host_via_host_samples(self, cursor, sample_id):
        """Get host information via host_samples FK relationship"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Step 1: Find host_samples record for this sample
            cursor.execute(f"SELECT * FROM {q}host_samples{q} WHERE {q}sample_id{q} = {p} LIMIT 1", (sample_id,))
            host_sample_row = cursor.fetchone()
            
            if not host_sample_row:
                print(f"DEBUG: No host_samples record found for sample {sample_id}")
                return None
            
            # Get column names for host_samples
            if self.db_type == 'sqlite':
                cursor.execute(f'PRAGMA table_info("host_samples")')
                host_sample_columns = [c[1] for c in cursor.fetchall()]
            else:
                cursor.execute(f'DESCRIBE `host_samples`')
                host_sample_columns = [c[0] for c in cursor.fetchall()]
            
            host_sample_data = dict(zip(host_sample_columns, host_sample_row))
            host_id = host_sample_data.get('host_id')
            
            if not host_id:
                print(f"DEBUG: host_samples record found but no host_id for sample {sample_id}")
                return None
            
            print(f"DEBUG: Found host_id {host_id} via host_samples for sample {sample_id}")
            
            # Step 2: Get host details using the host_id
            cursor.execute(f"SELECT * FROM {q}hosts{q} WHERE {q}host_id{q} = {p} LIMIT 1", (host_id,))
            host_row = cursor.fetchone()
            
            if not host_row:
                print(f"DEBUG: Host {host_id} not found in hosts table")
                return None
            
            # Get column names for hosts
            if self.db_type == 'sqlite':
                cursor.execute(f'PRAGMA table_info("hosts")')
                host_columns = [c[1] for c in cursor.fetchall()]
            else:
                cursor.execute(f'DESCRIBE `hosts`')
                host_columns = [c[0] for c in cursor.fetchall()]
            
            host_data = dict(zip(host_columns, host_row))
            print(f"DEBUG: Found host data: {host_data.get('species', 'Unknown species')}")
            
            return host_data
            
        except Exception as e:
            print(f"DEBUG: Error getting host via host_samples for {sample_id}: {e}")
            return None
    
    def _get_host_details(self, cursor, host_id):
        """Get detailed host information"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            cursor.execute(f"SELECT * FROM {q}hosts{q} WHERE {q}host_id{q} = {p} LIMIT 1", (host_id,))
            row = cursor.fetchone()
            if row:
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("hosts")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `hosts`')
                    columns = [c[0] for c in cursor.fetchall()]
                return dict(zip(columns, row))
            
            return None
            
        except Exception as e:
            print(f"Error getting host details: {e}")
            return None
    
    def _get_taxonomy_details(self, cursor, taxonomy_id):
        """Get detailed taxonomy information"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            if not taxonomy_id:
                print(f"DEBUG: No taxonomy_id provided for taxonomy details")
                return None
            
            cursor.execute(f"SELECT * FROM {q}taxonomy{q} WHERE {q}taxonomy_id{q} = {p} LIMIT 1", (taxonomy_id,))
            row = cursor.fetchone()
            if row:
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("taxonomy")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `taxonomy`')
                    columns = [c[0] for c in cursor.fetchall()]
                return dict(zip(columns, row))
            
            return None
            
        except Exception as e:
            print(f"Error getting taxonomy details: {e}")
            return None
    
    def _get_location_details(self, cursor, location_id):
        """Get detailed location information"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            cursor.execute(f"SELECT * FROM {q}locations{q} WHERE {q}location_id{q} = {p} LIMIT 1", (location_id,))
            row = cursor.fetchone()
            if row:
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("locations")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `locations`')
                    columns = [c[0] for c in cursor.fetchall()]
                return dict(zip(columns, row))
            
            return None
            
        except Exception as e:
            print(f"Error getting location details: {e}")
            return None
    
    def _get_sample_screening_results(self, cursor, sample_id):
        """Get all screening results for a sample - enhanced for tube IDs and host field IDs"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            screening_results = []
            
            print(f"DEBUG: Searching for screening results for {sample_id}")
            
            # First try to find by sample_id
            cursor.execute(f"SELECT * FROM {q}screening_results{q} WHERE {q}sample_id{q} = {p} LIMIT 10", (sample_id,))
            rows = cursor.fetchall()
            if rows:
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("screening_results")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `screening_results`')
                    columns = [c[0] for c in cursor.fetchall()]
                results = [dict(zip(columns, row)) for row in rows]
                print(f"DEBUG: Found {len(results)} screening results by sample_id")
                return results
            
            # If no results and sample_id looks like a tube ID, try tested_sample_id
            if isinstance(sample_id, str) and (sample_id.startswith('CANB_') or sample_id.startswith('CANR_') or sample_id.startswith('IPLNAHL_')):
                cursor.execute(f"SELECT * FROM {q}screening_results{q} WHERE {q}tested_sample_id{q} = {p} LIMIT 10", (sample_id,))
                rows = cursor.fetchall()
                if rows:
                    if self.db_type == 'sqlite':
                        cursor.execute(f'PRAGMA table_info("screening_results")')
                        columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f'DESCRIBE `screening_results`')
                        columns = [c[0] for c in cursor.fetchall()]
                    results = [dict(zip(columns, row)) for row in rows]
                    print(f"DEBUG: Found {len(results)} screening results by tested_sample_id")
                    return results
                
                # Also try any column that might contain tube IDs
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("screening_results")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `screening_results`')
                    columns = [c[0] for c in cursor.fetchall()]
                tube_columns = [col for col in columns if 'tube' in col.lower() or 'sample' in col.lower() or 'tested' in col.lower()]
                
                for col in tube_columns:
                    if col != 'sample_id' and col != 'tested_sample_id':  # Already tried these
                        cursor.execute(f"SELECT * FROM {q}screening_results{q} WHERE {q}{col}{q} = {p} LIMIT 10", (sample_id,))
                        rows = cursor.fetchall()
                        if rows:
                            if self.db_type == 'sqlite':
                                cursor.execute(f'PRAGMA table_info("screening_results")')
                                columns = [c[1] for c in cursor.fetchall()]
                            else:
                                cursor.execute(f'DESCRIBE `screening_results`')
                                columns = [c[0] for c in cursor.fetchall()]
                            results = [dict(zip(columns, row)) for row in rows]
                            print(f"DEBUG: Found {len(results)} screening results in column {col}")
                            return results
            
            print(f"DEBUG: No screening results found for {sample_id}")
            return []
            
        except Exception as e:
            print(f"Error getting screening results: {e}")
            return []
    
    def _get_sample_sequencing_data(self, cursor, sample_id):
        """Get sequencing data for a sample"""
        try:
            sequencing_data = []
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Try different sequencing tables
            seq_tables = ['sequencing', 'sequences', 'rna_sequences', 'dna_sequences']
            
            for table in seq_tables:
                try:
                    if self.db_type == 'sqlite':
                        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                    else:
                        cursor.execute("SHOW TABLES LIKE %s", (table,))
                    if cursor.fetchone():
                        cursor.execute(f"SELECT * FROM {q}{table}{q} WHERE {q}sample_id{q} = {p} LIMIT 5", (sample_id,))
                        rows = cursor.fetchall()
                        if rows:
                            if self.db_type == 'sqlite':
                                cursor.execute(f'PRAGMA table_info("{table}")')
                                columns = [c[1] for c in cursor.fetchall()]
                            else:
                                cursor.execute(f'DESCRIBE `{table}`')
                                columns = [c[0] for c in cursor.fetchall()]
                            for row in rows:
                                row_data = dict(zip(columns, row))
                                row_data['table_source'] = table
                                sequencing_data.append(row_data)
                except:
                    continue
            
            return sequencing_data
            
        except Exception as e:
            print(f"Error getting sequencing data: {e}")
            return []
    
    def _get_sample_storage_info(self, cursor, sample_id):
        """Get storage information for a sample - enhanced for precise tube ID matching"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Prioritize storage_locations table first
            storage_tables = ['storage_locations', 'storage', 'sample_storage']
            
            for table in storage_tables:
                try:
                    if self.db_type == 'sqlite':
                        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                    else:
                        cursor.execute("SHOW TABLES LIKE %s", (table,))
                    if cursor.fetchone():
                        print(f"DEBUG: Checking table {table} for sample_id {sample_id}")
                        
                        # Get table structure first
                        if self.db_type == 'sqlite':
                            cursor.execute(f'PRAGMA table_info("{table}")')
                            columns = [c[1] for c in cursor.fetchall()]
                        else:
                            cursor.execute(f'DESCRIBE `{table}`')
                            columns = [c[0] for c in cursor.fetchall()]
                        print(f"DEBUG: {table} columns: {columns}")
                        
                        # For tube IDs like CANB_INT23_L_057, search for exact matches first
                        if isinstance(sample_id, str) and sample_id.startswith('CANB_'):
                            # Search for exact sample_tube_id match - get ALL matches
                            cursor.execute(f"SELECT * FROM {q}{table}{q} WHERE {q}sample_tube_id{q} = {p}", (sample_id,))
                            rows = cursor.fetchall()
                            if rows:
                                if len(rows) > 1:
                                    print(f"DEBUG: ⚠️  Found {len(rows)} storage records for {sample_id}:")
                                    for i, row in enumerate(rows):
                                        storage_data = dict(zip(columns, row))
                                        print(f"    Record {i+1}: {storage_data}")
                                    # Look for the most recent one (by created_at if available)
                                    if 'created_at' in columns:
                                        # Sort by created_at descending to get most recent
                                        rows_with_dates = []
                                        for row in rows:
                                            row_dict = dict(zip(columns, row))
                                            rows_with_dates.append((row_dict.get('created_at', ''), row_dict))
                                        rows_with_dates.sort(reverse=True)
                                        storage_data = rows_with_dates[0][1]
                                        print(f"DEBUG: ✅ Selected most recent record: {storage_data}")
                                    else:
                                        # Just use the first one
                                        storage_data = dict(zip(columns, rows[0]))
                                        print(f"DEBUG: ✅ Using first record (no date sorting): {storage_data}")
                                else:
                                    storage_data = dict(zip(columns, rows[0]))
                                    print(f"DEBUG: ✅ Found exact storage match for {sample_id}")
                                    print(f"DEBUG: Storage record: {storage_data}")
                                storage_data['table_source'] = table
                                return storage_data
                            
                            # Also try tube_id column
                            if 'tube_id' in columns:
                                cursor.execute(f"SELECT * FROM {q}{table}{q} WHERE {q}tube_id{q} = {p}", (sample_id,))
                                rows = cursor.fetchall()
                                if rows:
                                    storage_data = dict(zip(columns, rows[0]))
                                    storage_data['table_source'] = table
                                    print(f"DEBUG: ✅ Found storage match using tube_id for {sample_id}")
                                    print(f"DEBUG: Storage record: {storage_data}")
                                    return storage_data
                        
                        # Try different sample ID column names and values
                        sample_info = self._get_sample_details(cursor, sample_id)
                        if sample_info:
                            print(f"DEBUG: Sample info found: {sample_info}")
                            
                            # Try tube IDs from sample info
                            for field in ['tissue_id', 'saliva_id', 'anal_id', 'urine_id', 'sample_tube_id']:
                                if field in sample_info and sample_info[field]:
                                    tube_id = sample_info[field]
                                    print(f"DEBUG: Searching for storage using {field}={tube_id}")
                                    
                                    cursor.execute(f"SELECT * FROM {q}{table}{q} WHERE {q}sample_tube_id{q} = {p}", (tube_id,))
                                    rows = cursor.fetchall()
                                    if rows:
                                        storage_data = dict(zip(columns, rows[0]))
                                        storage_data['table_source'] = table
                                        print(f"DEBUG: ✅ Found storage using {field}")
                                        print(f"DEBUG: Storage record: {storage_data}")
                                        return storage_data
                        
                        # Fallback: try any column that might contain the sample ID
                        for col in columns:
                            if 'id' in col.lower() or 'tube' in col.lower() or 'sample' in col.lower():
                                cursor.execute(f"SELECT * FROM {q}{table}{q} WHERE {q}{col}{q} = {p} LIMIT 1", (sample_id,))
                                row = cursor.fetchone()
                                if row:
                                    storage_data = dict(zip(columns, row))
                                    storage_data['table_source'] = table
                                    print(f"DEBUG: ✅ Found storage using fallback column {col}")
                                    print(f"DEBUG: Storage record: {storage_data}")
                                    return storage_data
                                
                except Exception as e:
                    print(f"Error checking table {table}: {e}")
                    continue
            
            print(f"DEBUG: ❌ No storage data found for sample_id {sample_id}")
            return None
            
        except Exception as e:
            print(f"Error getting storage info: {e}")
            return None
    
    def _get_other_related_data(self, cursor, sample_id):
        """Get any other related data for the sample"""
        try:
            other_data = []
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Get all tables and search for sample_id references
            if self.db_type == 'sqlite':
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                all_tables = [t[0] for t in cursor.fetchall()]
            else:
                cursor.execute("SHOW TABLES")
                all_tables = [t[0] for t in cursor.fetchall()]
            
            # Tables we've already handled
            handled_tables = {'samples', 'hosts', 'taxonomy', 'locations', 'screening_results', 'storage', 'storage_locations', 'sample_storage'}
            
            for table in all_tables:
                if table.lower() in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin'] or table in handled_tables:
                    continue
                
                try:
                    # Check if table has sample_id column
                    if self.db_type == 'sqlite':
                        cursor.execute(f'PRAGMA table_info("{table}")')
                        columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f'DESCRIBE `{table}`')
                        columns = [c[0] for c in cursor.fetchall()]
                    
                    if 'sample_id' in columns:
                        cursor.execute(f"SELECT * FROM {q}{table}{q} WHERE {q}sample_id{q} = {p} LIMIT 3", (sample_id,))
                        rows = cursor.fetchall()
                        if rows:
                            for row in rows:
                                row_data = dict(zip(columns, row))
                                row_data['table_source'] = table
                                other_data.append(row_data)
                except:
                    continue
            
            return other_data
            
        except Exception as e:
            print(f"Error getting other related data: {e}")
            return []
    
    def _format_sample_profile(self, profile):
        """Format comprehensive sample profile for display"""
        if not profile:
            return None
        
        parts = []
        parts.append(f"🧬 **Complete Sample Profile: {profile['sample_id']}**")
        parts.append("")
        
        # Sample Information
        if profile['sample_info']:
            parts.append("📋 **Sample Information**")
            sample = profile['sample_info']
            important_fields = ['sample_id', 'source_id', 'sample_origin', 'collection_date', 'tissue_type', 'sample_type']
            for field in important_fields:
                if field in sample and sample[field]:
                    display_name = field.replace('_', ' ').title()
                    parts.append(f"• {display_name}: {sample[field]}")
            parts.append("")
        
        # Host Information
        if profile['host_info']:
            parts.append("🐾 **Host Information**")
            host = profile['host_info']
            host_fields = ['host_id', 'species', 'common_name', 'sex', 'age', 'weight', 'capture_date']
            for field in host_fields:
                if field in host and host[field]:
                    display_name = field.replace('_', ' ').title()
                    parts.append(f"• {display_name}: {host[field]}")
            
            # Taxonomy Information - prioritize scientific name
            if profile.get('taxonomy_info'):
                parts.append("  🧬 **Taxonomy**")
                taxonomy = profile['taxonomy_info']
                # Show scientific name first, then other taxonomic info
                if 'scientific_name' in taxonomy and taxonomy['scientific_name']:
                    parts.append(f"  • Scientific Name: {taxonomy['scientific_name']}")
                elif 'species' in taxonomy and taxonomy['species']:
                    parts.append(f"  • Species: {taxonomy['species']}")
                
                # Show other important taxonomic fields
                tax_fields = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus']
                for field in tax_fields:
                    if field in taxonomy and taxonomy[field]:
                        display_name = field.replace('_', ' ').title()
                        parts.append(f"  • {display_name}: {taxonomy[field]}")
            parts.append("")
        
        # Location Information
        if profile['location_info']:
            parts.append("📍 **Location Information**")
            location = profile['location_info']
            location_fields = ['location_id', 'site_name', 'country', 'province', 'district', 'coordinates', 'elevation']
            for field in location_fields:
                if field in location and location[field]:
                    display_name = field.replace('_', ' ').title()
                    parts.append(f"• {display_name}: {location[field]}")
            parts.append("")
        
        # Screening Results
        if profile['screening_results']:
            parts.append(f"🧪 **Screening Results** ({len(profile['screening_results'])} tests)")
            for i, screening in enumerate(profile['screening_results'], 1):
                parts.append(f"  **Test {i}**")
                # Dynamically show all available screening fields
                for field, value in screening.items():
                    if field not in ['screening_id', 'sample_id', 'table_source'] and value:
                        display_name = field.replace('_', ' ').title()
                        # Format result nicely
                        if field.lower() in ['result']:
                            if isinstance(value, bool):
                                value = 'Positive' if value else 'Negative'
                            elif isinstance(value, (int, float)) and value in [0, 1]:
                                value = 'Positive' if value == 1 else 'Negative'
                            elif isinstance(value, str):
                                if value.lower() in ['positive', 'pos', '+']:
                                    value = 'Positive'
                                elif value.lower() in ['negative', 'neg', '-']:
                                    value = 'Negative'
                        parts.append(f"  • {display_name}: {value}")
            parts.append("")
        
        # Sequencing Data
        if profile['sequencing_data']:
            parts.append(f"🧬 **Sequencing Data** ({len(profile['sequencing_data'])} records)")
            for i, seq in enumerate(profile['sequencing_data'], 1):
                parts.append(f"  **Sequence {i}** (from {seq.get('table_source', 'unknown')})")
                # Show all available sequencing fields
                for field, value in seq.items():
                    if field not in ['table_source', 'sample_id'] and value:
                        display_name = field.replace('_', ' ').title()
                        parts.append(f"  • {display_name}: {value}")
            parts.append("")
        
        # Storage Information
        if profile['storage_info']:
            parts.append("❄️ **Storage Information**")
            storage = profile['storage_info']
            # Prioritize important storage fields
            storage_priority_fields = ['freezer_name', 'freezer', 'location', 'box_number', 'box', 'position', 'rack_position', 'temperature', 'storage_date', 'notes']
            
            # Show priority fields first
            for field in storage_priority_fields:
                if field in storage and storage[field]:
                    display_name = field.replace('_', ' ').title()
                    parts.append(f"• {display_name}: {storage[field]}")
            
            # Show any other storage-related fields
            for field, value in storage.items():
                if field not in ['table_source', 'sample_id'] + storage_priority_fields and value:
                    display_name = field.replace('_', ' ').title()
                    parts.append(f"• {display_name}: {value}")
            parts.append("")
        
        # Other Related Data
        if profile['other_related']:
            parts.append(f"📊 **Other Related Data** ({len(profile['other_related'])} records)")
            for i, data in enumerate(profile['other_related'], 1):
                table_name = data.get('table_source', 'unknown')
                parts.append(f"  **{i}. {table_name.title()}**")
                # Show first few important fields
                for key, value in list(data.items())[:6]:
                    if key not in ['table_source', 'sample_id'] and value:
                        display_name = key.replace('_', ' ').title()
                        parts.append(f"  • {display_name}: {value}")
            parts.append("")
        
        # Summary with actual data
        parts.append("📊 **Profile Summary**")
        parts.append(f"• Sample ID: {profile['sample_id']}")
        
        # Get proper host name (scientific name first, then species)
        host_name = 'Unknown'
        if profile.get('taxonomy_info'):
            taxonomy = profile['taxonomy_info']
            host_name = taxonomy.get('scientific_name', '') or taxonomy.get('species', 'Unknown')
        elif profile['host_info']:
            host_name = profile['host_info'].get('species', 'Unknown')
        parts.append(f"• Host: {host_name}")
        
        # Get actual location
        location_name = 'Unknown'
        if profile['location_info']:
            location_name = profile['location_info'].get('site_name', 'Unknown')
            if not location_name:
                location_name = profile['location_info'].get('country', 'Unknown')
        parts.append(f"• Location: {location_name}")
        
        parts.append(f"• Screening Tests: {len(profile['screening_results'])}")
        parts.append(f"• Sequencing Records: {len(profile['sequencing_data'])}")
        
        # Better storage summary
        if profile['storage_info']:
            storage = profile['storage_info']
            storage_summary = 'Available'
            
            # Prioritize rack position first
            if 'rack_position' in storage and storage['rack_position']:
                storage_summary = f"{storage['rack_position']}"
            elif 'position' in storage and storage['position']:
                storage_summary = f"{storage['position']}"
            elif 'freezer_name' in storage and storage['freezer_name']:
                storage_summary = f"{storage['freezer_name']}"
            elif 'freezer' in storage and storage['freezer_name']:
                storage_summary = f"{storage['freezer']}"
            elif 'location' in storage and storage['location']:
                storage_summary = f"{storage['location']}"
            
            parts.append(f"• Storage: {storage_summary}")
        else:
            parts.append("• Storage: Not recorded")
        
        return "\n".join(parts)
    
    def _find_value_in_all_tables(self, cursor, search_value):
        """Find a specific value in all tables across the database"""
        occurrences = []
        
        try:
            # Get all tables
            if self.db_type == 'sqlite':
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                all_tables = [t[0] for t in cursor.fetchall()]
            else:
                cursor.execute("SHOW TABLES")
                all_tables = [t[0] for t in cursor.fetchall()]
            
            for table_name in all_tables:
                if table_name.lower() in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin']:
                    continue
                
                try:
                    # Get table structure
                    if self.db_type == 'sqlite':
                        cursor.execute(f'PRAGMA table_info("{table_name}")')
                        columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f'DESCRIBE `{table_name}`')
                        columns = [c[0] for c in cursor.fetchall()]
                    
                    # Search for the value in all text columns
                    text_columns = []
                    for col in columns:
                        col_lower = col.lower()
                        if any(keyword in col_lower for keyword in ['id', 'code', 'name', 'sample', 'host', 'location', 'type']):
                            text_columns.append(col)
                    
                    if not text_columns:
                        text_columns = columns[:3]  # Use first 3 columns as fallback
                    
                    # Build search query
                    conditions = []
                    params = []
                    q = '"' if self.db_type == 'sqlite' else '`'
                    p = '?' if self.db_type == 'sqlite' else '%s'
                    
                    for col in text_columns:
                        conditions.append(f"{q}{col}{q} = {p}")
                        params.append(search_value)
                    
                    if conditions:
                        sql = f"SELECT * FROM {q}{table_name}{q} WHERE " + " OR ".join(conditions) + " LIMIT 10"
                        cursor.execute(sql, params)
                        rows = cursor.fetchall()
                        
                        if rows:
                            for row in rows:
                                row_data = dict(zip(columns, row))
                                occurrences.append({
                                    'table': table_name,
                                    'data': row_data,
                                    'columns': columns
                                })
                                print(f"DEBUG: Found {search_value} in {table_name}")
                
                except Exception as e:
                    print(f"Error searching table {table_name}: {e}")
                    continue
        
        except Exception as e:
            print(f"Error in _find_value_in_all_tables: {e}")
        
        return occurrences
    
    def _build_complete_fk_chain(self, cursor, starting_points, original_value):
        """Build complete FK chain starting from all occurrences"""
        visited = set()
        complete_chain = []
        
        for start_point in starting_points:
            table_name = start_point['table']
            record_data = start_point['data']
            
            # Extract the record ID from this occurrence
            record_id = self._extract_record_id(record_data, table_name)
            if record_id:
                chain_segment = self._follow_fk_chain_recursive(
                    cursor, table_name, record_id, visited.copy(), 
                    original_value, depth=0, max_depth=6
                )
                if chain_segment:
                    complete_chain.extend(chain_segment)
        
        return complete_chain
    
    def _follow_fk_chain_recursive(self, cursor, current_table, current_id, visited, 
                                  original_value, depth=0, max_depth=6):
        """Recursively follow FK chains in BOTH forward and backward directions"""
        if depth >= max_depth or current_table in visited:
            return []
        
        visited.add(current_table)
        print(f"DEBUG: Following FK chain in {current_table} at depth {depth}")
        
        chain_results = []
        
        # Get current record data
        record_data = self._get_table_data(cursor, current_table, current_id)
        if not record_data:
            return []
        
        # Add current table to chain
        chain_results.append({
            'table': current_table,
            'data': record_data,
            'depth': depth,
            'relationships': []
        })
        
        # Get ALL FK relationships (both forward and backward)
        fk_relationships = self._get_all_fk_relationships(cursor, current_table)
        
        for fk_info in fk_relationships:
            related_table = fk_info['table']
            direction = fk_info['direction']
            fk_column = fk_info['from']
            parent_column = fk_info['to']
            
            if related_table in visited:
                continue
            
            try:
                # Find connected records based on direction
                if direction == 'forward':
                    # Forward: current_table.fk_column -> related_table.parent_column
                    connected_records = self._find_forward_connected_records(
                        cursor, current_table, related_table, fk_column, parent_column, current_id
                    )
                else:
                    # Backward: related_table.fk_column -> current_table.parent_column
                    connected_records = self._find_backward_connected_records(
                        cursor, current_table, related_table, fk_column, parent_column, current_id
                    )
                
                if connected_records:
                    for record in connected_records[:3]:  # Limit to prevent explosion
                        connected_id = self._extract_record_id(record, related_table)
                        if connected_id:
                            # Add relationship info
                            chain_results[-1]['relationships'].append({
                                'to_table': related_table,
                                'direction': direction,
                                'connected_id': connected_id,
                                'connected_data': record
                            })
                            
                            # Recursively continue the chain in BOTH directions
                            sub_chain = self._follow_fk_chain_recursive(
                                cursor, related_table, connected_id, visited.copy(),
                                original_value, depth + 1, max_depth
                            )
                            chain_results.extend(sub_chain)
                            
            except Exception as e:
                print(f"Error following FK from {current_table} to {related_table}: {e}")
                continue
        
        return chain_results
    
    def _find_forward_connected_records(self, cursor, from_table, to_table, 
                                       fk_column, parent_column, current_id):
        """Find records connected via forward FK relationship"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Forward: from_table.fk_column -> to_table.parent_column
            # First get the FK value from current record
            cursor.execute(f"SELECT {q}{fk_column}{q} FROM {q}{from_table}{q} WHERE {q}id{q} = {p}", (current_id,))
            fk_result = cursor.fetchone()
            if fk_result:
                fk_value = fk_result[0]
                # Find records in to_table where parent_column matches fk_value
                cursor.execute(f"SELECT * FROM {q}{to_table}{q} WHERE {q}{parent_column}{q} = {p} LIMIT 3", (fk_value,))
                rows = cursor.fetchall()
                
                if rows:
                    # Get column names
                    if self.db_type == 'sqlite':
                        cursor.execute(f'PRAGMA table_info("{to_table}")')
                        columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f'DESCRIBE `{to_table}`')
                        columns = [c[0] for c in cursor.fetchall()]
                    
                    return [dict(zip(columns, row)) for row in rows]
            
            return []
            
        except Exception as e:
            print(f"Error finding forward connected records: {e}")
            return []
    
    def _find_backward_connected_records(self, cursor, from_table, to_table, 
                                        fk_column, parent_column, current_id):
        """Find records connected via backward FK relationship"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Backward: to_table.fk_column -> from_table.parent_column
            # Find records in to_table where fk_column matches current_id
            cursor.execute(f"SELECT * FROM {q}{to_table}{q} WHERE {q}{fk_column}{q} = {p} LIMIT 3", (current_id,))
            rows = cursor.fetchall()
            
            if rows:
                # Get column names
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{to_table}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{to_table}`')
                    columns = [c[0] for c in cursor.fetchall()]
                
                return [dict(zip(columns, row)) for row in rows]
            
            return []
            
        except Exception as e:
            print(f"Error finding backward connected records: {e}")
            return []
    
    def _format_complete_chain(self, chain_data):
        """Format the complete FK chain for display with bidirectional clarity"""
        if not chain_data:
            return None
        
        parts = []
        parts.append("🔗 **Complete Bidirectional FK Relationship Chain**")
        parts.append("")
        
        # Group by table to show all occurrences
        table_groups = {}
        for item in chain_data:
            table_name = item['table']
            if table_name not in table_groups:
                table_groups[table_name] = []
            table_groups[table_name].append(item)
        
        # Display each table and its relationships with directional clarity
        for table_name, items in table_groups.items():
            emoji = self._get_table_emoji(table_name)
            parts.append(f"{emoji} **{table_name.title()}**")
            
            for item in items:
                data = item['data']
                depth = item['depth']
                indent = "  " * depth
                
                # Show important fields
                important_cols = self._get_important_columns([{'name': col} for col in data.keys()])
                for col in important_cols[:3]:
                    if col in data and data[col]:
                        display_name = col.replace('_', ' ').title()
                        parts.append(f"{indent}• {display_name}: {data[col]}")
                
                # Show relationships with clear directional indicators
                if item['relationships']:
                    for rel in item['relationships']:
                        direction = rel['direction']
                        rel_emoji = self._get_table_emoji(rel['to_table'])
                        
                        if direction == 'forward':
                            # Forward relationship: This table → Related table
                            parts.append(f"{indent}  🔗 **Forward**: {emoji} {table_name} → {rel_emoji} {rel['to_table'].title()}")
                        else:
                            # Backward relationship: Related table → This table
                            parts.append(f"{indent}  🔗 **Backward**: {rel_emoji} {rel['to_table'].title()} → {emoji} {table_name}")
                        
                        # Show connected record key info
                        rel_data = rel['connected_data']
                        rel_important = self._get_important_columns([{'name': col} for col in rel_data.keys()])
                        for col in rel_important[:2]:
                            if col in rel_data and rel_data[col]:
                                display_name = col.replace('_', ' ').title()
                                parts.append(f"{indent}    - {display_name}: {rel_data[col]}")
                
                parts.append("")
        
        # Add directional summary
        forward_count = 0
        backward_count = 0
        for item in chain_data:
            for rel in item['relationships']:
                if rel['direction'] == 'forward':
                    forward_count += 1
                else:
                    backward_count += 1
        
        unique_tables = len(table_groups)
        total_relationships = forward_count + backward_count
        parts.append(f"📊 **Chain Summary**: {unique_tables} tables, {total_relationships} relationships")
        parts.append(f"   → Forward relationships: {forward_count}")
        parts.append(f"   ← Backward relationships: {backward_count}")
        
        return "\n".join(parts)
    
    def _find_primary_table(self, sample_id, cursor, schema):
        """Find which table contains the sample_id as primary record"""
        for table_name, table_info in schema.items():
            if table_name.lower() in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin']:
                continue
                
            try:
                # Get table columns
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{table_name}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{table_name}`')
                    columns = [c[0] for c in cursor.fetchall()]
                
                # Check if this table has sample_id or similar columns
                sample_columns = [col for col in columns if col.lower() in ['sample_id', 'id', 'sample_code']]
                
                if sample_columns:
                    q = '"' if self.db_type == 'sqlite' else '`'
                    p = '?' if self.db_type == 'sqlite' else '%s'
                    
                    for col in sample_columns:
                        cursor.execute(f"SELECT COUNT(*) FROM {q}{table_name}{q} WHERE {q}{col}{q} = {p}", (sample_id,))
                        if cursor.fetchone()[0] > 0:
                            return table_name
                            
            except Exception as e:
                print(f"Error checking table {table_name}: {e}")
                continue
        
        return None
    
    def _recursive_fk_traversal(self, cursor, current_table, current_id, visited, depth=0, max_depth=4):
        """Recursively traverse FK relationships in all directions"""
        if depth >= max_depth or current_table in visited:
            return {}
        
        visited.add(current_table)
        print(f"DEBUG: Traversing {current_table} at depth {depth} with ID {current_id}")
        
        results = {
            'table': current_table,
            'data': self._get_table_data(cursor, current_table, current_id),
            'relationships': {}
        }
        
        # Get all FK relationships (forward and backward)
        fk_relationships = self._get_all_fk_relationships(cursor, current_table)
        
        for fk_info in fk_relationships:
            related_table = fk_info['table']
            direction = fk_info['direction']  # 'forward' or 'backward'
            fk_column = fk_info['from']
            parent_column = fk_info['to']
            
            if related_table in visited:
                continue
            
            try:
                # Determine the lookup value based on direction
                if direction == 'forward':
                    # Forward: current_table -> related_table
                    lookup_value = self._get_lookup_value(cursor, current_table, current_id, fk_column)
                    if lookup_value:
                        search_column = parent_column
                    else:
                        continue
                else:
                    # Backward: related_table -> current_table
                    lookup_value = current_id
                    search_column = fk_column
                
                if lookup_value:
                    # Get related records
                    related_records = self._find_related_records(
                        cursor, related_table, search_column, lookup_value
                    )
                    
                    if related_records:
                        results['relationships'][related_table] = {
                            'direction': direction,
                            'fk_column': fk_column,
                            'parent_column': parent_column,
                            'records': related_records
                        }
                        
                        # Recursively traverse each related record
                        for record in related_records[:2]:  # Limit to avoid explosion
                            record_id = self._extract_record_id(record, related_table)
                            if record_id:
                                recursive_data = self._recursive_fk_traversal(
                                    cursor, related_table, record_id, visited.copy(), depth + 1, max_depth
                                )
                                if recursive_data:
                                    results['relationships'][related_table]['recursive'] = recursive_data
                                    
            except Exception as e:
                print(f"Error traversing {current_table} -> {related_table}: {e}")
                continue
        
        return results
    
    def _get_all_fk_relationships(self, cursor, table_name):
        """Get all FK relationships for a table (both forward and backward)"""
        relationships = []
        
        try:
            if self.db_type == 'sqlite':
                # Forward relationships (table references other tables)
                cursor.execute(f'PRAGMA foreign_key_list("{table_name}")')
                forward_fks = cursor.fetchall()
                
                for fk in forward_fks:
                    relationships.append({
                        'table': fk[2],      # Referenced table
                        'from': fk[3],       # Column in current table
                        'to': fk[4],         # Column in referenced table
                        'direction': 'forward'
                    })
                
                # Backward relationships (other tables reference this table)
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                all_tables = [t[0] for t in cursor.fetchall()]
                
                for other_table in all_tables:
                    if other_table != table_name and other_table.lower() not in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin']:
                        cursor.execute(f'PRAGMA foreign_key_list("{other_table}")')
                        other_fks = cursor.fetchall()
                        for fk in other_fks:
                            if fk[2] == table_name:  # References our table
                                relationships.append({
                                    'table': other_table,  # Table that references us
                                    'from': fk[3],         # Column in other table
                                    'to': fk[4],           # Column in our table
                                    'direction': 'backward'
                                })
            else:
                # MySQL/MariaDB implementation
                # Forward relationships
                cursor.execute("""
                    SELECT 
                        REFERENCED_TABLE_NAME, 
                        COLUMN_NAME, 
                        REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = %s 
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                """, (table_name,))
                
                for row in cursor.fetchall():
                    relationships.append({
                        'table': row[0],
                        'from': row[1],
                        'to': row[2],
                        'direction': 'forward'
                    })
                
                # Backward relationships
                cursor.execute("""
                    SELECT 
                        TABLE_NAME, 
                        COLUMN_NAME, 
                        REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND REFERENCED_TABLE_NAME = %s
                """, (table_name,))
                
                for row in cursor.fetchall():
                    relationships.append({
                        'table': row[0],
                        'from': row[1],
                        'to': row[2],
                        'direction': 'backward'
                    })
                    
        except Exception as e:
            print(f"Error getting FK relationships for {table_name}: {e}")
        
        return relationships
    
    def _get_table_data(self, cursor, table_name, record_id):
        """Get data for a specific record in a table"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Find the ID column
            if self.db_type == 'sqlite':
                cursor.execute(f'PRAGMA table_info("{table_name}")')
                columns = [c[1] for c in cursor.fetchall()]
            else:
                cursor.execute(f'DESCRIBE `{table_name}`')
                columns = [c[0] for c in cursor.fetchall()]
            
            # Try different ID columns
            id_columns = ['id', 'sample_id', 'sample_code', 'Id', 'Sample_Id', 'Sample_Code']
            
            for id_col in id_columns:
                if id_col in columns:
                    cursor.execute(f"SELECT * FROM {q}{table_name}{q} WHERE {q}{id_col}{q} = {p} LIMIT 1", (record_id,))
                    row = cursor.fetchone()
                    if row:
                        return dict(zip(columns, row))
            
            return None
            
        except Exception as e:
            print(f"Error getting data for {table_name}: {e}")
            return None
    
    def _get_lookup_value(self, cursor, table_name, record_id, fk_column):
        """Get the value of a foreign key column for a specific record"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            cursor.execute(f"SELECT {q}{fk_column}{q} FROM {q}{table_name}{q} WHERE {q}id{q} = {p} LIMIT 1", (record_id,))
            result = cursor.fetchone()
            return result[0] if result else None
            
        except Exception as e:
            print(f"Error getting lookup value: {e}")
            return None
    
    def _find_related_records(self, cursor, table_name, search_column, search_value):
        """Find records in a table that match a search value"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            cursor.execute(f"SELECT * FROM {q}{table_name}{q} WHERE {q}{search_column}{q} = {p} LIMIT 5", (search_value,))
            rows = cursor.fetchall()
            
            if rows:
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{table_name}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{table_name}`')
                    columns = [c[0] for c in cursor.fetchall()]
                
                return [dict(zip(columns, row)) for row in rows]
            
            return []
            
        except Exception as e:
            print(f"Error finding related records: {e}")
            return []
    
    def _extract_record_id(self, record, table_name):
        """Extract the primary ID from a record"""
        id_columns = ['id', 'sample_id', 'sample_code', 'Id', 'Sample_Id', 'Sample_Code']
        
        for id_col in id_columns:
            if id_col in record and record[id_col]:
                return str(record[id_col])
        
        # Fallback to first column
        if record:
            first_key = list(record.keys())[0]
            return str(record[first_key])
        
        return None
    
    def _format_recursive_results(self, related_data, indent=0):
        """Format recursive results into readable output"""
        if not related_data:
            return None
        
        prefix = "  " * indent
        parts = []
        
        # Format main table data
        table_name = related_data['table']
        table_data = related_data['data']
        
        if table_data:
            emoji = self._get_table_emoji(table_name)
            parts.append(f"{prefix}{emoji} {table_name.title()}")
            
            # Show important columns
            important_cols = self._get_important_columns([{'name': col} for col in table_data.keys()])
            for col in important_cols[:4]:
                if col in table_data and table_data[col]:
                    display_name = col.replace('_', ' ').title()
                    parts.append(f"{prefix}• {display_name}: {table_data[col]}")
        
        # Format relationships recursively
        if related_data['relationships']:
            for related_table, rel_info in related_data['relationships'].items():
                direction_symbol = "→" if rel_info['direction'] == 'forward' else "←"
                emoji = self._get_table_emoji(related_table)
                
                parts.append(f"{prefix}{direction_symbol} {emoji} {related_table.title()} ({rel_info['direction']})")
                
                # Show related records
                for i, record in enumerate(rel_info['records'][:2], 1):
                    record_parts = []
                    important_cols = self._get_important_columns([{'name': col} for col in record.keys()])
                    
                    for col in important_cols[:3]:
                        if col in record and record[col]:
                            display_name = col.replace('_', ' ').title()
                            record_parts.append(f"{display_name}: {record[col]}")
                    
                    if record_parts:
                        parts.append(f"{prefix}  {i}. {' | '.join(record_parts)}")
                
                # Recursive traversal
                if 'recursive' in rel_info:
                    recursive_output = self._format_recursive_results(rel_info['recursive'], indent + 2)
                    if recursive_output:
                        parts.append(recursive_output)
        
        return "\n".join(parts)
    
    def _get_foreign_key_relationships(self, table_name, cursor):
        """Get foreign key relationships for a table"""
        relationships = []
        try:
            if self.db_type == 'sqlite':
                cursor.execute(f'PRAGMA foreign_key_list("{table_name}")')
                fks = cursor.fetchall()
                # PRAGMA returns: id, seq, table, from, to, on_update, on_delete, match
                for fk in fks:
                    relationships.append({
                        'id': fk[0],
                        'seq': fk[1],
                        'type': 'forward',   # Forward relationship
                        'from_table': table_name,     # Current table
                        'to_table': fk[2],            # The referenced table
                        'from_column': fk[3],         # Column in current table
                        'to_column': fk[4]            # Column in referenced table
                    })
                
                # Also check for tables that reference this table (reverse FKs)
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                all_tables = [t[0] for t in cursor.fetchall()]
                
                for other_table in all_tables:
                    if other_table != table_name:
                        cursor.execute(f'PRAGMA foreign_key_list("{other_table}")')
                        other_fks = cursor.fetchall()
                        for fk in other_fks:
                            if fk[2] == table_name:  # References our main table
                                relationships.append({
                                    'type': 'reverse',  # Reverse relationship
                                    'from_table': other_table,    # The table that references us
                                    'to_table': table_name,        # Our main table
                                    'from_column': fk[3],         # Column in other table
                                    'to_column': fk[4]            # Column in our table
                                })
            else:  # MariaDB/MySQL
                # Forward FKs
                cursor.execute("""
                    SELECT 
                        REFERENCED_TABLE_NAME, 
                        COLUMN_NAME, 
                        REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = %s 
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                """, (table_name,))
                for row in cursor.fetchall():
                    relationships.append({
                        'type': 'forward',   # Forward relationship
                        'from_table': table_name,     # Current table
                        'to_table': row[0],            # Referenced table
                        'from_column': row[1],         # Column in current table
                        'to_column': row[2]            # Column in referenced table
                    })
                
                # Reverse FKs
                cursor.execute("""
                    SELECT 
                        TABLE_NAME, 
                        COLUMN_NAME, 
                        REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND REFERENCED_TABLE_NAME = %s
                """, (table_name,))
                for row in cursor.fetchall():
                    relationships.append({
                        'type': 'reverse',  # Reverse relationship
                        'from_table': row[0],            # Table that references us
                        'to_table': table_name,        # Our main table
                        'from_column': row[1],         # Column in other table
                        'to_column': row[2]            # Column in our table
                    })
        except Exception as e:
            print(f"Error getting FKs for {table_name}: {e}")
        
        return relationships
    
    def _get_table_emoji(self, table_name):
        """Get appropriate emoji based on table name"""
        name_lower = table_name.lower()
        if 'host' in name_lower:
            return '🐾'
        elif 'screen' in name_lower or 'test' in name_lower:
            return '🧪'
        elif 'storage' in name_lower or 'freezer' in name_lower:
            return '❄️'
        elif 'location' in name_lower:
            return '📍'
        elif 'virus' in name_lower:
            return '🦠'
        elif 'sequence' in name_lower or 'rna' in name_lower or 'dna' in name_lower:
            return '🧬'
        elif 'team' in name_lower or 'user' in name_lower:
            return '👥'
        else:
            return '📋'
    
    def _fetch_hardcoded_related_data(self, sample_id, cursor):
        """Fallback method using hardcoded table names when no FKs available"""
        related_parts = []
        
        # 1. Check for host information
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            cursor.execute(f'''
                SELECT h.* FROM {q}hosts{q} h 
                JOIN {q}host_samples{q} hs ON h.{q}host_id{q} = hs.{q}host_id{q} 
                WHERE hs.{q}sample_id{q} = {p}
            ''', (sample_id,))
            host_row = cursor.fetchone()
            if host_row:
                if self.db_type == 'sqlite':
                    cursor.execute("PRAGMA table_info(hosts)")
                    host_cols = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute("DESCRIBE `hosts`")
                    host_cols = [c[0] for c in cursor.fetchall()]
                host_data = dict(zip(host_cols, host_row))
                
                host_info = []
                # Dynamically find important host fields
                for col in host_cols:
                    if col in host_data and host_data[col]:
                        # Prioritize fields that are likely to be important
                        col_lower = col.lower()
                        if (any(pattern in col_lower for pattern in ['name', 'species', 'scientific', 'common', 'genus']) or
                            len(host_info) < 3):  # Show up to 3 fields
                            display = col.replace('_', ' ').title()
                            host_info.append(f"• {display}: {host_data[col]}")
                
                if host_info:
                    related_parts.append("\n🐾 Host Information")
                    related_parts.extend(host_info[:2])
        except Exception as e:
            pass
        
        # 2. Check for screening results
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            cursor.execute(f'''
                SELECT * FROM {q}screening{q} 
                WHERE {q}sample_id{q} = {p} 
                LIMIT 3
            ''', (sample_id,))
            screening_rows = cursor.fetchall()
            if screening_rows:
                if self.db_type == 'sqlite':
                    cursor.execute("PRAGMA table_info(screening)")
                    screening_cols = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute("DESCRIBE `screening`")
                    screening_cols = [c[0] for c in cursor.fetchall()]
                
                related_parts.append("\n🧪 Screening Results")
                for i, scr_row in enumerate(screening_rows, 1):
                    scr_data = dict(zip(screening_cols, scr_row))
                    
                    # Dynamically find test type field
                    test_type = None
                    for key, value in scr_data.items():
                        if value and any(test_word in key.lower() for test_word in ['test', 'virus', 'pan']):
                            test_type = str(value)
                            break
                    
                    if not test_type:
                        test_type = 'Unknown Test'
                    
                    # Dynamically find result field
                    result_val = None
                    for key, value in scr_data.items():
                        if value and any(result_word in key.lower() for result_word in ['result', 'outcome', 'screening']):
                            result_val = value
                            break
                    
                    if result_val is None:
                        result = 'No Result'
                    elif isinstance(result_val, bool):
                        result = 'Positive' if result_val else 'Negative'
                    elif isinstance(result_val, (int, float)):
                        if isinstance(result_val, int) and result_val in [0, 1]:
                            result = 'Positive' if result_val == 1 else 'Negative'
                        else:
                            result = str(result_val)
                    elif isinstance(result_val, str):
                        result = result_val if result_val.strip() else 'No Result'
                    else:
                        result = str(result_val)
                    
                    related_parts.append(f"• {test_type}: {result}")
        except Exception as e:
            pass
        
        # 3. Check for storage information
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            cursor.execute(f'''
                SELECT * FROM {q}storage{q} 
                WHERE {q}sample_id{q} = {p} 
                LIMIT 1
            ''', (sample_id,))
            storage_row = cursor.fetchone()
            if storage_row:
                if self.db_type == 'sqlite':
                    cursor.execute("PRAGMA table_info(storage)")
                    storage_cols = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute("DESCRIBE `storage`")
                    storage_cols = [c[0] for c in cursor.fetchall()]
                storage_data = dict(zip(storage_cols, storage_row))
                
                storage_info = []
                # Dynamically find important storage fields
                for col in storage_cols:
                    if col in storage_data and storage_data[col]:
                        # Prioritize fields that are likely to be important
                        col_lower = col.lower()
                        if (any(pattern in col_lower for pattern in ['freezer', 'location', 'position', 'box', 'storage']) or
                            len(storage_info) < 2):  # Show up to 2 fields
                            display = col.replace('_', ' ').title()
                            storage_info.append(f"• {display}: {storage_data[col]}")
                
                if storage_info:
                    related_parts.append("\n❄️ Storage Location")
                    related_parts.extend(storage_info[:2])
        except Exception as e:
            pass
        
        return related_parts
    
    def _get_important_columns(self, columns):
        """Dynamically determine important columns based on actual column names in the table"""
        col_names = [c['name'] if isinstance(c, dict) else c for c in columns]
        
        # Dynamic importance detection - prioritize columns that are likely to be important
        important = []
        
        # First, look for ID/identification columns (most important)
        id_patterns = ['id', 'code', 'sample', 'name', 'title', 'key']
        for col in col_names:
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in id_patterns):
                important.append(col)
        
        # Then look for data/content columns
        data_patterns = ['value', 'data', 'result', 'status', 'type', 'category', 'bag', 'tissue', 'organ']
        for col in col_names:
            if col not in important:
                col_lower = col.lower()
                if any(pattern in col_lower for pattern in data_patterns):
                    important.append(col)
        
        # Add remaining columns if we still need more
        if len(important) < 5:
            for col in col_names:
                if col not in important:
                    important.append(col)
                    if len(important) >= 5:
                        break
        
        # If still no columns found, return first few
        if not important:
            important = col_names[:5]
        
        return important
    
    def _get_sample_data(self, conn, sample_id):
        """Get comprehensive sample data including related tables - completely dynamic with FK relationships"""
        try:
            cursor = conn.cursor()
            print(f"DEBUG: Getting sample data for {sample_id}")
            
            # Try different sample ID formats
            sample_id_variants = [sample_id]
            
            # Add common variations (e.g., CANB_ANAL23_033 -> CANB-ANAL23-033)
            if '_' in sample_id:
                sample_id_variants.append(sample_id.replace('_', '-'))
            if '-' in sample_id:
                sample_id_variants.append(sample_id.replace('-', '_'))
            
            print(f"DEBUG: Sample ID variants to try: {sample_id_variants}")
            
            # Discover all tables in the database dynamically
            if self.db_type == 'sqlite':
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            else:
                cursor.execute("SHOW TABLES")
            
            all_tables = [row[0] for row in cursor.fetchall()]
            print(f"DEBUG: Available tables: {all_tables}")
            
            sample_data = {}
            
            # First, find the primary sample record in any table
            primary_sample_found = False
            
            for table in all_tables:
                if table.lower() in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin']:  # Skip system tables
                    continue
                    
                try:
                    # Get table structure
                    if self.db_type == 'sqlite':
                        cursor.execute(f"PRAGMA table_info({table})")
                        columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f"DESCRIBE `{table}`")
                        columns = [c[0] for c in cursor.fetchall()]
                    print(f"DEBUG: Table {table} columns: {columns}")
                    
                    # Look for columns that might contain sample IDs
                    sample_id_columns = []
                    for col in columns:
                        col_lower = col.lower()
                        if any(id_word in col_lower for id_word in ['sample', 'id', 'code']):
                            sample_id_columns.append(col)
                    
                    print(f"DEBUG: Potential sample ID columns in {table}: {sample_id_columns}")
                    
                    # Try to find the sample in this table
                    found_data = None
                    found_column = None
                    q = '"' if self.db_type == 'sqlite' else '`'
                    p = '?' if self.db_type == 'sqlite' else '%s'
                    
                    for sample_col in sample_id_columns:
                        for variant in sample_id_variants:
                            cursor.execute(f"SELECT * FROM {q}{table}{q} WHERE {q}{sample_col}{q} = {p} LIMIT 1", (variant,))
                            row = cursor.fetchone()
                            if row:
                                print(f"DEBUG: Found sample in {table} using column {sample_col} with variant {variant}")
                                found_data = dict(zip(columns, row))
                                found_column = sample_col
                                break
                        if found_data:
                            break
                    
                    if found_data:
                        # Add data with table prefix
                        for key, value in found_data.items():
                            if value:  # Only add non-empty values
                                sample_data[f'{table}_{key}'] = value
                                # Also add without prefix for important fields
                                if any(important in key.lower() for important in ['name', 'type', 'date', 'status', 'result']):
                                    sample_data[key] = value
                        
                        print(f"DEBUG: Added {len(found_data)} fields from {table}")
                        
                        # If this is the primary sample table, use it to find related data
                        if not primary_sample_found and any(col in columns for col in ['sample_id', 'sample_code']):
                            primary_sample_found = True
                            primary_sample_id = found_data.get('sample_id') or found_data.get('sample_code') or found_data.get(found_column)
                            print(f"DEBUG: Primary sample found in {table} with ID: {primary_sample_id}")
                            
                            # Now use foreign keys to find related data
                            self._find_related_data_via_fk(cursor, primary_sample_id, table, sample_data)
                    
                except Exception as e:
                    print(f"DEBUG: Error processing table {table}: {str(e)}")
                    continue
            
            print(f"DEBUG: Total sample data keys for {sample_id}: {len(sample_data)}")
            print(f"DEBUG: Sample data keys sample: {list(sample_data.keys())[:10]}")
                
            conn.close()
            return sample_data
            
        except Exception as e:
            print(f"DEBUG: Major error getting sample data for {sample_id}: {str(e)}")
            if conn:
                conn.close()
            return {}
    
    def _find_related_data_via_fk(self, cursor, sample_id, primary_table, sample_data):
        """Find related data using foreign key relationships"""
        try:
            print(f"DEBUG: Finding related data for sample_id: {sample_id} from primary table: {primary_table}")
            
            # Common foreign key relationships to check
            fk_relationships = [
                # (table_name, foreign_key_column, local_sample_column)
                ('host_samples', 'sample_id', 'sample_id'),
                ('screening', 'sample_id', 'sample_id'),
                ('storage', 'sample_id', 'sample_id'),
            ]
            
            for related_table, fk_column, local_column in fk_relationships:
                try:
                    # Check if the related table exists
                    if self.db_type == 'sqlite':
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (related_table,))
                    else:
                        cursor.execute(f"SHOW TABLES LIKE '{related_table}'")
                        
                    if not cursor.fetchone():
                        continue
                    
                    # Get table structure
                    if self.db_type == 'sqlite':
                        cursor.execute(f"PRAGMA table_info({related_table})")
                        columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f"DESCRIBE `{related_table}`")
                        columns = [c[0] for c in cursor.fetchall()]
                    
                    # Query related data using foreign key
                    q = '"' if self.db_type == 'sqlite' else '`'
                    p = '?' if self.db_type == 'sqlite' else '%s'
                    cursor.execute(f"SELECT * FROM {q}{related_table}{q} WHERE {q}{fk_column}{q} = {p}", (sample_id,))
                    rows = cursor.fetchall()
                    
                    if rows:
                        print(f"DEBUG: Found {len(rows)} related records in {related_table}")
                        for i, row in enumerate(rows):
                            row_data = dict(zip(columns, row))
                            
                            # Add related data with table prefix
                            for key, value in row_data.items():
                                if value:  # Only add non-empty values
                                    if len(rows) == 1:
                                        sample_data[f'{related_table}_{key}'] = value
                                    else:
                                        sample_data[f'{related_table}_{i+1}_{key}'] = value
                                    
                                    # Also add important fields without prefix
                                    if any(important in key.lower() for important in ['name', 'type', 'date', 'status', 'result']):
                                        if len(rows) == 1:
                                            sample_data[key] = value
                                        else:
                                            sample_data[f'{key}_{i+1}'] = value
                        
                        print(f"DEBUG: Added related data from {related_table}")
                    
                except Exception as e:
                    print(f"DEBUG: Error finding related data in {related_table}: {str(e)}")
                    continue
            
            # Special case: if we found host_samples, get host information
            if 'host_samples_host_id' in sample_data:
                host_id = sample_data['host_samples_host_id']
                print(f"DEBUG: Found host_id: {host_id}, getting host information")
                
                try:
                    if self.db_type == 'sqlite':
                        cursor.execute(f"PRAGMA table_info(hosts)")
                        host_columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f"DESCRIBE `hosts`")
                        host_columns = [c[0] for c in cursor.fetchall()]
                    
                    q = '"' if self.db_type == 'sqlite' else '`'
                    p = '?' if self.db_type == 'sqlite' else '%s'
                    cursor.execute(f"SELECT * FROM {q}hosts{q} WHERE {q}host_id{q} = {p}", (host_id,))
                    host_row = cursor.fetchone()
                    
                    if host_row:
                        host_data = dict(zip(host_columns, host_row))
                        print(f"DEBUG: Found host data: {len(host_data)} fields")
                        
                        # Add host data
                        for key, value in host_data.items():
                            if value:
                                sample_data[f'hosts_{key}'] = value
                                # Also add important fields without prefix
                                if any(important in key.lower() for important in ['name', 'type', 'date', 'status', 'result']):
                                    sample_data[key] = value
                except Exception as e:
                    print(f"DEBUG: Error getting host data: {str(e)}")
                    
        except Exception as e:
            print(f"DEBUG: Error in _find_related_data_via_fk: {str(e)}")
    
    def _master_intelligence_analysis(self, question):
        """Master Intelligence analysis using Master SQL and Python models"""
        try:
            # Use Master SQL to understand the query intent
            if self.master_sql_trainer:
                sql_analysis = self.master_sql_trainer.generate_sql_query(question)
                if sql_analysis and sql_analysis.get('confidence') == 'high':
                    print(f"🔧 Master SQL Analysis: {sql_analysis['predicted_category']} ({sql_analysis['predicted_complexity']})")
                    
                    # Generate and execute the actual SQL query
                    result = self._execute_master_sql_query(question, sql_analysis)
                    if result:
                        return result
            
            # Use Master Python for analysis and visualization requests
            if self.master_python_trainer:
                python_analysis = self.master_python_trainer.generate_python_code(question)
                if python_analysis and python_analysis.get('confidence') == 'high':
                    print(f"🐍 Master Python Analysis: {python_analysis['predicted_category']} ({python_analysis['predicted_complexity']})")
                    
                    # Generate Python code suggestions
                    result = self._generate_python_suggestion(question, python_analysis)
                    if result:
                        return result
            
            return None
            
        except Exception as e:
            print(f"DEBUG: Error in Master Intelligence analysis: {str(e)}")
            return None
    
    def _execute_master_sql_query(self, question, sql_analysis):
        """Execute Master SQL query and format results"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Generate appropriate SQL based on analysis
            category = sql_analysis['predicted_category']
            
            if 'compare' in question.lower() and 'province' in question.lower():
                # Coronavirus positivity comparison query
                query = '''
                SELECT 
                    l.province,
                    COUNT(*) as total_samples,
                    SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) as positive_samples,
                    ROUND(SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positivity_rate
                FROM samples s
                JOIN hosts h ON s.host_id = h.host_id
                JOIN locations l ON h.location_id = l.location_id
                LEFT JOIN screening_results sr ON s.sample_id = sr.sample_id
                WHERE sr.pan_corona IS NOT NULL AND l.province IS NOT NULL
                GROUP BY l.province
                ORDER BY positivity_rate DESC
                '''
                
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    # Format results
                    response_parts = ["📊 **Coronavirus Positivity Rates by Province**\n"]
                    response_parts.append("| Province | Total Samples | Positive Samples | Positivity Rate |")
                    response_parts.append("|----------|---------------|-----------------|----------------|")
                    
                    for row in results:
                        province, total, positive, rate = row
                        response_parts.append(f"| {province} | {total} | {positive} | {rate}% |")
                    
                    response_parts.append(f"\n📈 **Analysis Summary:**")
                    response_parts.append(f"• Total provinces analyzed: {len(results)}")
                    if results:
                        highest = max(results, key=lambda x: x[3])
                        response_parts.append(f"• Highest positivity rate: {highest[0]} ({highest[3]}%)")
                    
                    conn.close()
                    return "\n".join(response_parts)
            
            elif 'species' in question.lower() and 'positive' in question.lower():
                # Species with positive results query
                query = '''
                SELECT 
                    t.scientific_name,
                    COUNT(*) as total_samples,
                    SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) as positive_samples,
                    ROUND(SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positivity_rate
                FROM samples s
                JOIN hosts h ON s.host_id = h.host_id
                JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
                LEFT JOIN screening_results sr ON s.sample_id = sr.sample_id
                WHERE sr.pan_corona IS NOT NULL AND t.scientific_name IS NOT NULL
                GROUP BY t.scientific_name
                HAVING COUNT(*) >= 3
                ORDER BY positivity_rate DESC
                LIMIT 10
                '''
                
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    response_parts = ["🦇 **Bat Species with Positive Coronavirus Results**\n"]
                    response_parts.append("| Scientific Name | Total Samples | Positive Samples | Positivity Rate |")
                    response_parts.append("|------------------|---------------|-----------------|----------------|")
                    
                    for row in results:
                        species, total, positive, rate = row
                        response_parts.append(f"| {species} | {total} | {positive} | {rate}% |")
                    
                    conn.close()
                    return "\n".join(response_parts)
            
            conn.close()
            return None
            
        except Exception as e:
            print(f"DEBUG: Error executing Master SQL query: {str(e)}")
            return None
    
    def _generate_python_suggestion(self, question, python_analysis):
        """Generate Python code suggestions for data analysis"""
        try:
            category = python_analysis['predicted_category']
            
            if 'dashboard' in question.lower() or 'visualization' in question.lower():
                return '''🐍 **Python Dashboard Generation Suggestion**

Based on your request for "{}", here's a Python code template:

```python
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3

# Connect to database
conn = sqlite3.connect('your_database.db')

# Load screening data
query = """
SELECT sr.*, h.scientific_name, l.province, l.country
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
JOIN hosts h ON s.host_id = h.host_id
JOIN locations l ON h.location_id = l.location_id
"""
df = pd.read_sql_query(query, conn)

# Create dashboard
fig, axes = plt.subplots(2, 2, figsize=(15, 12))

# 1. Positivity rates by province
province_positivity = df.groupby('province').apply(
    lambda x: (x['pan_corona'] == 'Positive').sum() / len(x) * 100
).sort_values(ascending=False)
axes[0, 0].bar(range(len(province_positivity)), province_positivity.values)
axes[0, 0].set_title('Coronavirus Positivity by Province')
axes[0, 0].set_xticks(range(len(province_positivity)))
axes[0, 0].set_xticklabels(province_positivity.index, rotation=45)

# 2. Species distribution
species_counts = df['scientific_name'].value_counts().head(10)
axes[0, 1].barh(range(len(species_counts)), species_counts.values)
axes[0, 1].set_title('Top 10 Bat Species Sampled')
axes[0, 1].set_yticks(range(len(species_counts)))
axes[0, 1].set_yticklabels(species_counts.index)

# 3. Overall positivity rates
pathogen_positivity = {}
for pathogen in ['pan_corona', 'pan_hanta', 'pan_paramyxo', 'pan_flavi']:
    if pathogen in df.columns:
        positive_rate = (df[pathogen] == 'Positive').sum() / df[pathogen].notna().sum() * 100
        pathogen_positivity[pathogen.replace('pan_', '').title()] = positive_rate

axes[1, 0].bar(range(len(pathogen_positivity)), list(pathogen_positivity.values()))
axes[1, 0].set_title('Overall Pathogen Positivity Rates')
axes[1, 0].set_xticks(range(len(pathogen_positivity)))
axes[1, 0].set_xticklabels(list(pathogen_positivity.keys()), rotation=45)

# 4. Time series (if dates available)
if 'collection_date' in df.columns:
    df['collection_date'] = pd.to_datetime(df['collection_date'], errors='coerce')
    monthly_samples = df.groupby(df['collection_date'].dt.to_period('M')).size()
    axes[1, 1].plot(monthly_samples.index.astype(str), monthly_samples.values, marker='o')
    axes[1, 1].set_title('Sample Collection Over Time')
    axes[1, 1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

conn.close()
```

📊 **This dashboard will show:**
• Coronavirus positivity rates by province
• Top 10 bat species sampled
• Overall pathogen positivity rates  
• Sample collection trends over time

🚀 **Run this code to generate your visualization dashboard!**'''.format(question)
            
            elif 'machine learning' in question.lower() or 'predict' in question.lower():
                return '''🤖 **Python Machine Learning Suggestion**

Based on your request for "{}", here's a Python ML template:

```python
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder
import sqlite3

# Connect and load data
conn = sqlite3.connect('your_database.db')

# Load comprehensive data
query = """
SELECT 
    s.sample_id, s.collection_date, s.sample_origin,
    h.host_type, h.sex, h.age, h.weight_g, h.forearm_mm,
    t.scientific_name, t.family, t.genus,
    l.province, l.country,
    sr.pan_corona, sr.pan_hanta, sr.pan_paramyxo, sr.pan_flavi
FROM samples s
JOIN hosts h ON s.host_id = h.host_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
JOIN locations l ON h.location_id = l.location_id
LEFT JOIN screening_results sr ON s.sample_id = sr.sample_id
WHERE sr.pan_corona IS NOT NULL
"""
df = pd.read_sql_query(query, conn)

# Prepare features
feature_cols = ['sex', 'age', 'weight_g', 'forearm_mm', 'family', 'genus', 'province']
X = df[feature_cols].copy()
y = (df['pan_corona'] == 'Positive').astype(int)  # Binary classification

# Handle categorical variables
for col in X.select_dtypes(include=['object']).columns:
    le = LabelEncoder()
    X[col] = le.fit_transform(X[col].astype(str))

# Handle missing values
X = X.fillna(X.median())
y = y.fillna(0)

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Evaluate
print("Classification Report:")
print(classification_report(y_test, y_pred))

print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred))

# Feature importance
feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print("\nFeature Importance:")
print(feature_importance)

conn.close()
```

🎯 **This ML model will:**
• Predict coronavirus positivity based on host characteristics
• Identify the most important predictive features
• Provide classification metrics and confusion matrix
• Help understand factors influencing pathogen detection

🚀 **Run this code to build your prediction model!**'''.format(question)
            
            return None
            
        except Exception as e:
            print(f"DEBUG: Error generating Python suggestion: {str(e)}")
            return None
    
    def ask(self, question):
        """Main entry point - Master Intelligence approach: Master SQL/Python first, then fallback"""
        try:
            # Check for Excel upload related questions first
            question_lower = question.lower()
            excel_keywords = ['excel', 'upload', 'fill', 'auto-fill', 'auto fill', 'spreadsheet', 'csv']
            has_excel = any(keyword in question_lower for keyword in excel_keywords)
            
            if has_excel:
                return """📊 **Excel Upload Feature**

I can help you automatically fill Excel files with sample data from the database!

**How it works:**
1. Click the 📎 **paperclip button** next to the chat input to upload your Excel file
2. Your file should have a column named "SampleId", "Sample ID", or similar
3. I'll automatically fill in missing data using your actual column names from the database

**Supported formats:** .xlsx, .xls, .csv

**Try it now:** Upload an Excel file with sample IDs and I'll enrich it with database data!"""

            # FUNCTION 0: Try Master SQL/Python Intelligence first
            if self.master_models_loaded:
                print("=== FUNCTION 0: Master Intelligence Analysis ===")
                master_result = self._master_intelligence_analysis(question)
                
                if master_result and not self._is_empty_result(master_result):
                    print("✓ Master Intelligence found results")
                    return master_result
                else:
                    print("✗ Master Intelligence found no results")

            # FUNCTION 1: Try database structure approach
            print("=== FUNCTION 1: Database Structure Search ===")
            db_result = self._database_structure_search(question)
            
            # Check if we got meaningful results from database structure search
            if db_result and not self._is_empty_result(db_result):
                print("✓ Database structure search found results")
                return db_result
            else:
                print("✗ Database structure search found no results")
            
            # FUNCTION 2: Fall back to dynamic search
            print("\n=== FUNCTION 2: Dynamic Search Fallback ===")
            dynamic_result = self._dynamic_search(question)
            
            if dynamic_result and not self._is_empty_result(dynamic_result):
                print("✓ Dynamic search found results")
                return dynamic_result
            else:
                print("✗ Dynamic search found no results")
            
            # If all functions failed, return helpful message
            return self._get_help_message(question)
            
        except Exception as e:
            print(f"Error in Master Intelligence search: {str(e)}")
            return f"I'm a Master Intelligence AI for your database! I can dynamically query any connected database with advanced SQL and Python capabilities.\n\nError: {str(e)}\n\nTry asking:\n- 'Compare coronavirus positivity rates across provinces'\n- 'Create a dashboard for screening results visualization'\n- 'Explain the complete research workflow for bat virology studies'\n- 'Show me host information for sample CANB_TIS23_L_075'"
    
    def _database_structure_search(self, question):
        """Function 1: Search using known database structure"""
        try:
            # Analyze the question for SQL queries
            analysis = self.analyze_question(question)
            
            # Generate SQL queries based on schema
            queries, error = self.generate_sql(analysis)
            if error:
                print(f"Database structure search error: {error}")
                return None
            
            # Execute queries
            results = self.execute_queries(queries)
            
            # Format and return response
            response = self.format_response(question, results, analysis)
            return response
            
        except Exception as e:
            print(f"Database structure search exception: {str(e)}")
            return None
    
    def _dynamic_search(self, question):
        """Function 2: Dynamic search when database structure fails"""
        try:
            # This is a more flexible search that tries different approaches
            print(f"Attempting dynamic search for: {question}")
            
            # Try to extract any potential identifiers or keywords
            keywords = self._extract_search_terms(question)
            print(f"Extracted keywords: {keywords}")
            
            if not keywords:
                return None
            
            # Try broad searches across all tables
            results = self._broad_table_search(keywords)
            
            if results:
                return self._format_dynamic_results(results, keywords)
            else:
                return None
                
        except Exception as e:
            print(f"Dynamic search exception: {str(e)}")
            return None
    
    def _extract_search_terms(self, question):
        """Extract search terms from question for dynamic search"""
        import re
        
        # Remove common words and extract meaningful terms
        stop_words = {'the', 'and', 'for', 'with', 'from', 'where', 'what', 'find', 'search', 'show', 'list', 'how', 'many', 'this', 'that', 'are', 'was', 'were', 'been', 'have', 'has', 'had', 'did', 'does', 'doing', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'need', 'needs', 'dare', 'dares', 'ought', 'used', 'use', 'using', 'get', 'got', 'gets', 'gotten', 'become', 'became', 'becomes', 'seem', 'seems', 'seemed', 'look', 'looks', 'looked', 'appear', 'appears', 'appeared', 'happen', 'happens', 'happened', 'try', 'tries', 'tried', 'trying', 'want', 'wants', 'wanted', 'wanting', 'like', 'likes', 'liked', 'liking', 'love', 'loves', 'loved', 'loving', 'hate', 'hates', 'hated', 'hating', 'prefer', 'prefers', 'preferred', 'preferring', 'hope', 'hopes', 'hoped', 'hoping', 'wish', 'wishes', 'wished', 'wishing', 'believe', 'believes', 'believed', 'believing', 'think', 'thinks', 'thought', 'thinking', 'suppose', 'supposes', 'supposed', 'supposing', 'expect', 'expects', 'expected', 'expecting', 'imagine', 'imagines', 'imagined', 'imagining', 'feel', 'feels', 'felt', 'feeling', 'see', 'sees', 'saw', 'seen', 'seeing', 'watch', 'watches', 'watched', 'watching', 'hear', 'hears', 'heard', 'hearing', 'notice', 'notices', 'noticed', 'noticing', 'let', 'lets', 'letting', 'make', 'makes', 'made', 'making', 'help', 'helps', 'helped', 'helping', 'force', 'forces', 'forced', 'forcing', 'drive', 'drives', 'drove', 'driven', 'driving'}
        
        # Extract words with 3+ characters
        words = re.findall(r'\b[a-zA-Z0-9_]{3,}\b', question.lower())
        
        # Filter out stop words and return meaningful terms
        keywords = [w.upper() for w in words if w not in stop_words]
        
        # Also extract quoted phrases
        quoted = re.findall(r'["\']([^"\']+)["\']', question)
        keywords.extend([q.strip().upper() for q in quoted])
        
        # Extract complex identifiers (like sample IDs)
        complex_ids = re.findall(r'\b[a-zA-Z0-9_<>:.-]{4,}\b', question)
        keywords.extend([cid.upper() for cid in complex_ids if any(c in cid for c in '<>:-')])
        
        return list(dict.fromkeys(keywords))  # Remove duplicates
    
    def _broad_table_search(self, keywords):
        """Search broadly across all tables with keywords - FIXED FOR EXACT SOURCEID MATCHING"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            schema = self.get_schema()
            if not schema:
                return None
            
            all_results = []
            
            # Check if this looks like a SourceId search (contains special characters)
            has_sourceid_pattern = any(any(c in keyword for c in '<>:-') for keyword in keywords)
            
            for table_name, table_info in schema.items():
                if table_name.lower() in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin']:
                    continue
                
                try:
                    # Get table columns
                    if self.db_type == 'sqlite':
                        cursor.execute(f'PRAGMA table_info("{table_name}")')
                        columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f'DESCRIBE `{table_name}`')
                        columns = [c[0] for c in cursor.fetchall()]
                    
                    # Check if this table has source_id column
                    has_source_id = 'source_id' in columns
                    
                    if has_source_id and has_sourceid_pattern:
                        # EXACT SOURCEID MATCHING for patterns like 45100<21:00A109
                        print(f"Found source_id column in {table_name}, doing exact matching")
                        
                        # Build exact match conditions for SourceId
                        conditions = []
                        params = []
                        
                        for keyword in keywords:
                            # Only do exact matching for SourceId-like patterns
                            if any(c in keyword for c in '<>:-'):
                                conditions.append("source_id = ?")
                                params.append(keyword)
                        
                        if conditions:
                            q = '"' if self.db_type == 'sqlite' else '`'
                            p = '?' if self.db_type == 'sqlite' else '%s'
                            
                            # Exact match query
                            sql = f"SELECT * FROM {q}{table_name}{q} WHERE " + " OR ".join(conditions) + " LIMIT 10"
                            
                            try:
                                cursor.execute(sql, params)
                                rows = cursor.fetchall()
                                
                                if rows:
                                    all_results.append({
                                        'table': table_name,
                                        'columns': columns,
                                        'rows': rows,
                                        'match_type': 'exact_sourceid'
                                    })
                                    print(f"Exact SourceId search found {len(rows)} matches in {table_name}")
                        
                            except Exception as e:
                                print(f"Exact SourceId search error in {table_name}: {e}")
                                continue
                    
                    # Fallback to fuzzy matching for other searches
                    text_columns = []
                    for col in columns:
                        # Heuristically determine if this might be a text column
                        if any(text_word in col.lower() for text_word in ['name', 'code', 'id', 'type', 'status', 'result', 'location', 'date', 'sample', 'host', 'virus', 'test', 'screen']):
                            text_columns.append(col)
                    
                    if not text_columns:
                        # If no obvious text columns, use the first few columns
                        text_columns = columns[:3]
                    
                    # Build search conditions for non-SourceId searches
                    conditions = []
                    params = []
                    
                    for keyword in keywords:
                        # Skip exact SourceId patterns here (already handled above)
                        if not any(c in keyword for c in '<>:-'):
                            for col in text_columns:
                                conditions.append(f"{col} LIKE ?")
                                params.append(f"%{keyword}%")
                    
                    if conditions:
                        q = '"' if self.db_type == 'sqlite' else '`'
                        p = '?' if self.db_type == 'sqlite' else '%s'
                        
                        # Fuzzy search query
                        sql = f"SELECT * FROM {q}{table_name}{q} WHERE " + " OR ".join(conditions) + " LIMIT 10"
                        
                        try:
                            cursor.execute(sql, params)
                            rows = cursor.fetchall()
                            
                            if rows:
                                all_results.append({
                                    'table': table_name,
                                    'columns': columns,
                                    'rows': rows,
                                    'match_type': 'fuzzy'
                                })
                                print(f"Fuzzy search found {len(rows)} matches in {table_name}")
                        
                        except Exception as e:
                            print(f"Fuzzy search error in {table_name}: {e}")
                            continue
                
                except Exception as e:
                    print(f"Error processing {table_name} for dynamic search: {e}")
                    continue
            
            conn.close()
            
            # Sort results: exact matches first, then fuzzy
            all_results.sort(key=lambda x: (0 if x['match_type'] == 'exact_sourceid' else 1, x['table']))
            
            return all_results if all_results else None
            
        except Exception as e:
            print(f"Broad table search error: {e}")
            return None
    
    def _format_dynamic_results(self, results, keywords):
        """Format results from dynamic search with enhanced related data discovery - FIXED FOR EXACT MATCHES"""
        if not results:
            return None
        
        # Initialize response_parts early
        response_parts = []
        
        # Separate exact matches from fuzzy matches
        exact_matches = []
        fuzzy_matches = []
        
        for result in results:
            if result['match_type'] == 'exact_sourceid':
                exact_matches.append(result)
            else:
                fuzzy_matches.append(result)
        
        # Initialize sample_records and other_results for both cases
        sample_records = []
        other_results = []
        
        # Process all results to collect sample and other records
        for result in results:
            table_name = result['table']
            rows = result['rows']
            columns = result['columns']
            
            # Check if this is a sample-related table
            if any(keyword in table_name.lower() for keyword in ['sample', 'specimen', 'test', 'screening']):
                for row in rows:
                    row_data = dict(zip(columns, row))
                    sample_records.append((table_name, row_data))
            else:
                other_results.append((table_name, rows, columns))
        
        # Start with exact matches if any
        if exact_matches:
            response_parts.append(f"🎯 **Exact matches found for: {', '.join(keywords[:3])}**")
            
            for result in exact_matches:
                table_name = result['table']
                rows = result['rows']
                columns = result['columns']
                emoji = self._get_table_emoji(table_name)
                response_parts.append(f"\n{emoji} {table_name.title()} ({len(rows)} exact matches)")
                
                for i, row in enumerate(rows[:5], 1):
                    row_data = dict(zip(columns, row))
                    
                    # Show SourceId prominently for exact matches
                    if 'source_id' in row_data and row_data['source_id']:
                        response_parts.append(f"  {i}. **SourceId**: {row_data['source_id']}")
                    
                    # Show other important fields
                    for col in columns:
                        if col in row_data and row_data[col] and col != 'source_id':
                            display_name = col.replace('_', ' ').title()
                            response_parts.append(f"     **{display_name}**: {row_data[col]}")
                            if len([col for col in columns if col in row_data and row_data[col]]) >= 3:
                                break
                
                if len(rows) > 5:
                    response_parts.append(f"  ... and {len(rows) - 5} more exact matches")
            
            # Add separator if there are also fuzzy matches
            if fuzzy_matches:
                response_parts.append(f"\n🔍 **Similar matches (fuzzy search):**")
        
        # Process fuzzy matches
        for result in fuzzy_matches:
            table_name = result['table']
            rows = result['rows']
            columns = result['columns']
            emoji = self._get_table_emoji(table_name)
            response_parts.append(f"\n{emoji} {table_name.title()} ({len(rows)} similar matches)")
            
            for i, row in enumerate(rows[:3], 1):  # Show fewer for fuzzy matches
                row_data = dict(zip(columns, row))
                
                # Show most relevant fields
                important_fields = []
                for col in columns:
                    if col in row_data and row_data[col]:
                        # Check if this field contains any of our keywords
                        field_str = str(row_data[col]).upper()
                        if any(keyword in field_str for keyword in keywords):
                            important_fields.append(f"{col.replace('_', ' ').title()}: {row_data[col]}")
                
                # If no keyword matches, show first few important fields
                if not important_fields:
                    for col in columns[:2]:
                        if col in row_data and row_data[col]:
                            important_fields.append(f"{col.replace('_', ' ').title()}: {row_data[col]}")
                            if len(important_fields) >= 2:
                                break
                
                if important_fields:
                    response_parts.append(f"  {i}. {' | '.join(important_fields)}")
            
            if len(rows) > 3:
                response_parts.append(f"  ... and {len(rows) - 3} more similar matches")
        
        # If no exact matches, show the old format
        if not exact_matches:
            response_parts = [f"🔍 Found matches using dynamic search for: {', '.join(keywords[:3])}"]
            
            # Show other results first
            for table_name, rows, columns in other_results:
                emoji = self._get_table_emoji(table_name)
                response_parts.append(f"\n{emoji} {table_name.title()} ({len(rows)} matches)")
                
                for i, row in enumerate(rows[:5], 1):  # Show max 5 per table
                    row_data = dict(zip(columns, row))
                    
                    # Show most relevant fields
                    important_fields = []
                    for col in columns:
                        if col in row_data and row_data[col]:
                            # Check if this field contains any of our keywords
                            field_str = str(row_data[col]).upper()
                            if any(keyword in field_str for keyword in keywords):
                                important_fields.append(f"{col.replace('_', ' ').title()}: {row_data[col]}")
                    
                    # If no keyword matches, show first few important fields
                    if not important_fields:
                        for col in columns[:3]:
                            if col in row_data and row_data[col]:
                                important_fields.append(f"{col.replace('_', ' ').title()}: {row_data[col]}")
                                if len(important_fields) >= 2:
                                    break
                    
                    if important_fields:
                        response_parts.append(f"  {i}. {' | '.join(important_fields)}")
                
                if len(rows) > 5:
                    response_parts.append(f"  ... and {len(rows) - 5} more matches")
        
        # For sample records, use enhanced formatting with related data
        for table_name, row_data in sample_records:
            # Reconstruct the original question from keywords
            original_question = ' '.join(keywords)
            
            # For screening results, we need to extract the sample_id from tested_sample_id
            if 'screening' in table_name.lower():
                # Try to find the sample_id from the screening record
                tested_sample_id = row_data.get('tested_sample_id') or row_data.get('sample_tube_id')
                if tested_sample_id:
                    # Look up the corresponding sample record
                    try:
                        conn = self.get_connection()
                        cursor = conn.cursor()
                        q = '"' if self.db_type == 'sqlite' else '`'
                        
                        # Find the sample record that contains this tube ID
                        print(f"DEBUG: Looking for sample with tested_sample_id: {tested_sample_id}")
                        cursor.execute(f'''
                            SELECT * FROM {q}samples{q} 
                            WHERE {q}tissue_id{q} = ? OR {q}intestine_id{q} = ? OR {q}plasma_id{q} = ? OR {q}blood_id{q} = ?
                            LIMIT 1
                        ''', (tested_sample_id, tested_sample_id, tested_sample_id, tested_sample_id))
                        
                        sample_row = cursor.fetchone()
                        print(f"DEBUG: Found sample row with specific columns: {sample_row}")
                        
                        # If not found with specific columns, try broader search
                        if not sample_row:
                            print(f"DEBUG: Trying broader search for {tested_sample_id}")
                            cursor.execute(f'''
                                SELECT * FROM {q}samples{q} 
                                WHERE {q}sample_id{q} LIKE ? OR {q}source_id{q} LIKE ? OR {q}tissue_id{q} LIKE ? OR {q}intestine_id{q} LIKE ?
                                LIMIT 1
                            ''', (f'%{tested_sample_id}%', f'%{tested_sample_id}%', f'%{tested_sample_id}%', f'%{tested_sample_id}%'))
                            
                            sample_row = cursor.fetchone()
                            print(f"DEBUG: Found sample row with broader search: {sample_row}")
                        
                        if sample_row:
                            cursor.execute(f'PRAGMA table_info("samples")')
                            sample_columns = [col[1] for col in cursor.fetchall()]
                            sample_data = dict(zip(sample_columns, sample_row))
                            
                            # Use the enhanced _format_single_record method with the sample data
                            enhanced_parts = self._format_single_record('samples', sample_data, original_question)
                            
                            # Add the enhanced formatted data
                            if enhanced_parts:
                                response_parts.extend(enhanced_parts)
                        else:
                            # If no sample found, try to find related data directly from screening result
                            print(f"DEBUG: No sample found, trying direct related data search from screening")
                            
                            # Extract any host_id or location_id from screening result
                            host_id = row_data.get('host_id') or row_data.get('sample_id')
                            location_id = row_data.get('location_id')
                            
                            print(f"DEBUG: Extracted host_id: {host_id}, location_id: {location_id}")
                            
                            if host_id or location_id:
                                # Use the simple related data search with screening data as sample_data
                                mock_sample_data = {
                                    'sample_id': host_id or tested_sample_id,
                                    'host_id': host_id,
                                    'location_id': location_id,
                                    'tissue_id': tested_sample_id,
                                    'intestine_id': tested_sample_id,
                                    'plasma_id': tested_sample_id
                                }
                                
                                print(f"DEBUG: Mock sample data for related search: {mock_sample_data}")
                                
                                related_data = self._simple_related_data_search(cursor, None, mock_sample_data, original_question)
                                
                                print(f"DEBUG: Related data found: {related_data}")
                                
                                if related_data:
                                    print(f"DEBUG: Found related data directly from screening: {list(related_data.keys())}")
                                    
                                    # Format the related data
                                    for table_name, data in related_data.items():
                                        if data and len(data) > 0:
                                            emoji = self._get_table_emoji(table_name)
                                            title = table_name.replace('_', ' ').title()
                                            
                                            response_parts.append(f"{emoji} **{title}**")
                                            
                                            if isinstance(data, list):
                                                response_parts.append(f"({len(data)} records)")
                                                
                                                for i, record in enumerate(data[:3], 1):
                                                    response_parts.append(f"**Record {i}**")
                                                    for key, value in record.items():
                                                        if value and key != 'sample_id':
                                                            display_name = key.replace('_', ' ').title()
                                                            response_parts.append(f"• **{display_name}**: {value}")
                                                
                                                if len(data) > 3:
                                                    response_parts.append(f"... and {len(data) - 3} more records")
                                            else:
                                                for key, value in data.items():
                                                    if value:
                                                        display_name = key.replace('_', ' ').title()
                                                        response_parts.append(f"• **{display_name}**: {value}")
                            else:
                                # No host_id or location_id, but still try to find related data using only tested_sample_id
                                print(f"DEBUG: No host_id/location_id, trying search with only tested_sample_id")
                                
                                mock_sample_data = {
                                    'sample_id': tested_sample_id,
                                    'host_id': None,
                                    'location_id': None,
                                    'tissue_id': tested_sample_id,
                                    'intestine_id': tested_sample_id,
                                    'plasma_id': tested_sample_id
                                }
                                
                                print(f"DEBUG: Mock sample data for related search (tube ID only): {mock_sample_data}")
                                
                                related_data = self._simple_related_data_search(cursor, None, mock_sample_data, original_question)
                                
                                print(f"DEBUG: Related data found with tube ID only: {related_data}")
                                
                                if related_data:
                                    print(f"DEBUG: Found related data directly from screening (tube ID only): {list(related_data.keys())}")
                                    
                                    # Format the related data
                                    for table_name, data in related_data.items():
                                        if data and len(data) > 0:
                                            emoji = self._get_table_emoji(table_name)
                                            title = table_name.replace('_', ' ').title()
                                            
                                            response_parts.append(f"{emoji} **{title}**")
                                            
                                            if isinstance(data, list):
                                                response_parts.append(f"({len(data)} records)")
                                                
                                                for i, record in enumerate(data[:3], 1):
                                                    response_parts.append(f"**Record {i}**")
                                                    for key, value in record.items():
                                                        if value and key != 'sample_id':
                                                            display_name = key.replace('_', ' ').title()
                                                            response_parts.append(f"• **{display_name}**: {value}")
                                                
                                                if len(data) > 3:
                                                    response_parts.append(f"... and {len(data) - 3} more records")
                                            else:
                                                for key, value in data.items():
                                                    if value:
                                                        display_name = key.replace('_', ' ').title()
                                                        response_parts.append(f"• **{display_name}**: {value}")
                            
                            # Show basic screening info as fallback
                            response_parts.append(f"\n🧪 {table_name.title()}")
                            for key, value in row_data.items():
                                if value and key not in ['screening_id', 'excel_id']:
                                    display_name = key.replace('_', ' ').title()
                                    response_parts.append(f"• **{display_name}**: {value}")
                        
                        conn.close()
                    except Exception as e:
                        print(f"DEBUG: Error finding sample for screening result: {e}")
                        # Fallback to basic display
                        response_parts.append(f"\n🧪 {table_name.title()}")
                        for key, value in row_data.items():
                            if value and key not in ['screening_id', 'excel_id']:
                                display_name = key.replace('_', ' ').title()
                                response_parts.append(f"• **{display_name}**: {value}")
                else:
                    # No tested_sample_id found, show basic info
                    response_parts.append(f"\n🧪 {table_name.title()}")
                    for key, value in row_data.items():
                        if value and key not in ['screening_id', 'excel_id']:
                            display_name = key.replace('_', ' ').title()
                            response_parts.append(f"• **{display_name}**: {value}")
            else:
                # For other sample-related tables, use enhanced formatting
                enhanced_parts = self._format_single_record(table_name, row_data, original_question)
                
                # Add the enhanced formatted data
                if enhanced_parts:
                    response_parts.extend(enhanced_parts)
        
        return "\n".join(response_parts)
    
    def _is_empty_result(self, result):
        """Check if a result is essentially empty (no meaningful data)"""
        if not result:
            return True
        
        # Check for common "no results" messages
        empty_indicators = [
            "couldn't find any records",
            "no records matching",
            "found some data but couldn't format",
            "i couldn't find",
            "no matching records"
        ]
        
        result_lower = result.lower()
        return any(indicator in result_lower for indicator in empty_indicators)
    
    def _get_help_message(self, question):
        """Get helpful message when no results found"""
        return f"""🔍 **No Results Found**

I couldn't find any information matching '{question}' in the database.

"""

def get_smart_ai():
    """Get Smart AI instance - now using FREE local AI"""
    db_type = session.get('db_type', 'sqlite')
    
    if db_type == 'sqlite':
        db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
    else:
        db_config = session.get('db_params')
    
    return SmartLocalAI(db_config, db_type)

def get_ai_chat():
    """Get or initialize AI chat instance using current session database"""
    db_type = session.get('db_type', 'sqlite')
    
    if db_type == 'sqlite':
        db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        if not db_config:
            db_config = os.path.join(os.path.dirname(__file__), '..', 'uploads', 'CAN2.db')
    else:
        db_config = session.get('db_params')
        
    print(f"DEBUG: Chat connection request - Type: {db_type}, Config available: {db_config is not None}")
    return SmartLocalAI(db_config, db_type)

@chat_bp.route('/')
def chat_interface():
    """Chat interface page"""
    try:
        # Get current database information from session
        db_name = session.get('db_name', 'Unknown Database')
        db_type = session.get('db_type', 'unknown')
        
        return render_template('chat_interface.html', 
                             db_name=db_name, 
                             db_type=db_type)
    except Exception as e:
        return f"Template error: {str(e)}", 500

@chat_bp.route('/ask', methods=['POST'])
def ask_question():
    """Handle AI chat questions with SmartLocalAI"""
    data = request.get_json()
    question = data.get('question', '')
    
    if not question:
        return jsonify({'error': 'No question provided'}), 400
    
    try:
        # Check if this is an ML prediction request
        question_lower = question.lower()
        ml_keywords = ['predict', 'prediction', 'model', 'ml', 'machine learning']
        
        # Get dynamic table keywords from trained models
        table_keywords = []
        if 'full_ml_models' in session:
            try:
                full_models = session.get('full_ml_models', [])
                for model_pickle in full_models:
                    if isinstance(model_pickle, bytes):
                        model_data = pickle.loads(model_pickle)
                        table_name = model_data.get('table', '')
                        if table_name:
                            # Add multiple variations for each table
                            table_keywords.extend([
                                f'{table_name} table',
                                f'{table_name} model',
                                table_name
                            ])
                # Remove duplicates while preserving order
                seen = set()
                table_keywords = [x for x in table_keywords if not (x in seen or seen.add(x))]
            except Exception as e:
                print(f"DEBUG: Error getting dynamic table keywords: {e}")
                # Fallback to basic keywords
                table_keywords = ['hosts table', 'samples table', 'locations table', 'morphometrics table']
        else:
            # Fallback if no models trained
            table_keywords = ['hosts table', 'samples table', 'locations table', 'morphometrics table']
        
        print(f"DEBUG: Chat question received: {question}")
        print(f"DEBUG: Question lower: {question_lower}")
        print(f"DEBUG: ML keywords found: {[kw for kw in ml_keywords if kw in question_lower]}")
        print(f"DEBUG: Table keywords found: {[tk for tk in table_keywords if tk in question_lower]}")
        
        if (any(keyword in question_lower for keyword in ml_keywords) and 
            any(table in question_lower for table in table_keywords)):
            print("DEBUG: Routing to ML prediction endpoint")
            # Route to ML prediction endpoint with request data
            from routes.ml import ml_predict
            
            # Temporarily set flask.request.data for ML prediction function
            original_request_data = getattr(flask.request, 'data', None)
            flask.request.data = {'query': question}
            print(f"DEBUG: Set flask.request.data to: {question}")
            
            try:
                result = ml_predict()
                # Restore original request data
                if original_request_data is not None:
                    flask.request.data = original_request_data
                else:
                    delattr(flask.request, 'data')
                return result
            except:
                # Restore original request data
                if original_request_data is not None:
                    flask.request.data = original_request_data
                else:
                    delattr(flask.request, 'data')
                raise
        else:
            print("DEBUG: Routing to regular AI")
        
        # Use SmartLocalAI for dynamic, intelligent responses
        smart_ai = get_smart_ai()
        response = smart_ai.ask(question)
        
        return jsonify({'success': True, 'answer': response})
        
    except Exception as e:
        print(f"DEBUG: Chat endpoint error: {str(e)}")
        return jsonify({'success': False, 'answer': f'Sorry, I encountered an error: {str(e)}'}), 500

@chat_bp.route('/upload_excel', methods=['POST'])
def upload_excel():
    """Handle Excel file upload for auto-filling missing data"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
        return jsonify({'error': 'Please upload an Excel file (.xlsx or .xls)'}), 400
    
    try:
        # Read Excel file
        df = pd.read_excel(file)
        print(f"DEBUG: Excel file read successfully. Columns: {list(df.columns)}")
        print(f"DEBUG: Excel file has {len(df)} rows")
        
        # Find sample ID column dynamically
        sample_id_column = None
        
        # Look for any column that might contain sample IDs
        for col in df.columns:
            col_lower = col.lower().strip()
            # Check if column name suggests it contains IDs
            if any(id_word in col_lower for id_word in ['sample', 'id', 'code', 'identifier']):
                # Check if this column actually contains data that looks like sample IDs
                sample_data = df[col].dropna().head(5)
                if len(sample_data) > 0:
                    # Check if values look like sample IDs (contain letters and numbers)
                    for val in sample_data:
                        val_str = str(val).strip()
                        if (any(c.isalpha() for c in val_str) and 
                            any(c.isdigit() for c in val_str) and
                            len(val_str) > 3):  # Reasonable sample ID length
                            sample_id_column = col
                            print(f"DEBUG: Dynamically found sample ID column: {col}")
                            print(f"DEBUG: Sample values: {list(sample_data)}")
                            break
                if sample_id_column:
                    break
        
        if not sample_id_column:
            print(f"DEBUG: No sample ID column found. Available columns: {list(df.columns)}")
            return jsonify({
                'success': True,
                'message': f'I can help you upload Excel files! Your file should have a column named "SampleId", "Sample ID", or similar. Available columns: {list(df.columns)}'
            })
        
        # Extract sample IDs
        sample_ids = [str(sid).strip() for sid in df[sample_id_column].dropna()]
        print(f"DEBUG: Found {len(sample_ids)} sample IDs: {sample_ids[:5]}...")
        
        if not sample_ids:
            return jsonify({'error': 'No sample IDs found in the file'}), 400
        
        # Process each sample and enrich data using simple, reliable approach
        smart_ai = get_smart_ai()
        enriched_data = []
        
        # Get all columns from the original Excel file (except sample ID column)
        excel_columns = [col for col in df.columns if col != sample_id_column]
        print(f"DEBUG: Excel columns to process: {excel_columns}")
        
        for sample_id in sample_ids[:50]:  # Process up to 50 samples
            try:
                print(f"DEBUG: ===== Processing sample ID: {sample_id} =====")
                
                # Start with sample ID
                row_data = {'SampleId': sample_id}
                
                # Use the proven _build_sample_profile approach
                print(f"DEBUG: Building sample profile for {sample_id}...")
                
                conn = smart_ai.get_connection()
                cursor = conn.cursor()
                
                # Build comprehensive sample profile
                profile = smart_ai._build_sample_profile(cursor, sample_id)
                
                conn.close()
                
                # Debug: Show what was found in the profile
                print(f"DEBUG: Profile results for {sample_id}:")
                print(f"  - Sample info: {'✓' if profile['sample_info'] else '✗'}")
                if profile['sample_info']:
                    print(f"    Fields: {list(profile['sample_info'].keys())}")
                print(f"  - Host info: {'✓' if profile['host_info'] else '✗'}")
                if profile['host_info']:
                    print(f"    Fields: {list(profile['host_info'].keys())}")
                print(f"  - Taxonomy info: {'✓' if profile.get('taxonomy_info') else '✗'}")
                if profile.get('taxonomy_info'):
                    print(f"    Fields: {list(profile['taxonomy_info'].keys())}")
                print(f"  - Location info: {'✓' if profile['location_info'] else '✗'}")
                if profile['location_info']:
                    print(f"    Fields: {list(profile['location_info'].keys())}")
                print(f"  - Screening results: {len(profile['screening_results'])}")
                print(f"  - Sequencing data: {len(profile['sequencing_data'])}")
                print(f"  - Storage info: {'✓' if profile['storage_info'] else '✗'}")
                if profile['storage_info']:
                    print(f"    Fields: {list(profile['storage_info'].keys())}")
                
                # Check if we found any data at all
                has_any_data = (
                    profile['sample_info'] or 
                    profile['host_info'] or 
                    profile.get('taxonomy_info') or 
                    profile['location_info'] or 
                    profile['screening_results'] or 
                    profile['sequencing_data'] or 
                    profile['storage_info']
                )
                
                if not has_any_data:
                    print(f"DEBUG: ❌ NO DATA FOUND for {sample_id}")
                    print(f"DEBUG: This sample may not exist in the database")
                    # Still add the row with just SampleId so it shows up in results
                    enriched_data.append(row_data)
                    continue
                
                # Create a simple data dictionary from the profile
                available_data = {}
                
                # Add all data from profile with clear field names
                if profile['sample_info']:
                    for key, value in profile['sample_info'].items():
                        if value:
                            available_data[key] = value
                            available_data[f'sample_{key}'] = value
                
                if profile['host_info']:
                    for key, value in profile['host_info'].items():
                        if value:
                            available_data[key] = value
                            available_data[f'host_{key}'] = value
                
                if profile.get('taxonomy_info'):
                    for key, value in profile['taxonomy_info'].items():
                        if value:
                            available_data[key] = value
                            available_data[f'taxonomy_{key}'] = value
                            # Special handling for scientific_name
                            if key == 'scientific_name':
                                available_data['species'] = value
                
                if profile['location_info']:
                    for key, value in profile['location_info'].items():
                        if value:
                            available_data[key] = value
                            available_data[f'location_{key}'] = value
                            # Special handling for province
                            if key == 'province':
                                available_data['province'] = value
                
                if profile['screening_results']:
                    for i, screening in enumerate(profile['screening_results']):
                        for key, value in screening.items():
                            if value and key not in ['screening_id', 'sample_id']:
                                # Create field name with test type
                                field_name = key
                                available_data[field_name] = value
                                available_data[f'screening_{key}'] = value
                
                if profile['storage_info']:
                    for key, value in profile['storage_info'].items():
                        if value and key not in ['table_source', 'sample_id']:
                            available_data[key] = value
                            available_data[f'storage_{key}'] = value
                            # Special handling for rack_position
                            if key == 'rack_position':
                                available_data['rack_position'] = value
                
                print(f"DEBUG: Available data fields: {list(available_data.keys())}")
                
                # Match Excel columns to available data with enhanced accuracy
                matches_found = 0
                for excel_col in excel_columns:
                    # Skip empty columns
                    if pd.isna(excel_col) or str(excel_col).strip() == '':
                        continue
                    
                    excel_col_original = str(excel_col).strip()
                    excel_col_clean = excel_col_original.lower()
                    
                    print(f"DEBUG: Looking for: '{excel_col_original}'")
                    
                    # Enhanced field matching with priority order
                    found_value = None
                    found_source = None
                    
                    # Priority 1: Exact field name match
                    if excel_col_clean in available_data:
                        found_value = available_data[excel_col_clean]
                        found_source = f"exact: {excel_col_clean}"
                    
                    # Priority 2: Case-insensitive exact match
                    if not found_value:
                        for key, value in available_data.items():
                            if key.lower() == excel_col_clean and value:
                                found_value = value
                                found_source = f"case-insensitive: {key}"
                                break
                    
                    # Priority 3: Special field mappings with high accuracy
                    if not found_value:
                        if excel_col_clean == 'scientific_name':
                            # Look for scientific_name first, then species as fallback
                            if 'scientific_name' in available_data:
                                found_value = available_data['scientific_name']
                                found_source = "special: scientific_name"
                            elif 'species' in available_data:
                                found_value = available_data['species']
                                found_source = "special: species (fallback for scientific_name)"
                        elif excel_col_clean == 'pan_corona':
                            # Look for pan_corona specifically
                            if 'pan_corona' in available_data:
                                found_value = available_data['pan_corona']
                                found_source = "special: pan_corona"
                        elif excel_col_clean == 'province':
                            if 'province' in available_data:
                                found_value = available_data['province']
                                found_source = "special: province"
                        elif excel_col_clean == 'rack_position':
                            # Combine rack and spot_position for rack_position
                            if 'rack' in available_data and 'spot_position' in available_data:
                                rack = available_data['rack']
                                spot = available_data['spot_position']
                                if rack and spot:
                                    found_value = f"{rack}_{spot}"
                                    found_source = "combined: rack + spot_position"
                                elif rack:
                                    found_value = rack
                                    found_source = "partial: rack only"
                            elif 'rack' in available_data:
                                found_value = available_data['rack']
                                found_source = "special: rack"
                            elif 'spot_position' in available_data:
                                found_value = available_data['spot_position']
                                found_source = "special: spot_position"
                    
                    # Priority 4: Partial matching for field names containing the column name
                    if not found_value:
                        matches = []
                        for key, value in available_data.items():
                            if value and excel_col_clean in key.lower():
                                # Calculate match score based on how close the names are
                                key_lower = key.lower()
                                if key_lower == excel_col_clean:
                                    score = 100  # Perfect match (should have been caught above)
                                elif key_lower.endswith(excel_col_clean):
                                    score = 90  # Ends with column name
                                elif key_lower.startswith(excel_col_clean):
                                    score = 80  # Starts with column name
                                elif excel_col_clean in key_lower:
                                    score = 70  # Contains column name
                                else:
                                    score = 0
                                
                                if score > 0:
                                    matches.append((score, key, value))
                        
                        # Sort by score and take the best match
                        if matches:
                            matches.sort(reverse=True)
                            best_score, best_key, best_value = matches[0]
                            found_value = best_value
                            found_source = f"partial (score {best_score}): {best_key}"
                    
                    # Priority 5: Fuzzy matching for common field variations
                    if not found_value:
                        # Common field name variations
                        field_variations = {
                            'scientific_name': ['species', 'scientific_name', 'taxon_name', 'organism'],
                            'pan_corona': ['pan_corona', 'corona', 'coronavirus'],
                            'province': ['province', 'state', 'region', 'location'],
                            'rack_position': ['rack_position', 'position', 'location', 'storage']
                        }
                        
                        if excel_col_clean in field_variations:
                            for variation in field_variations[excel_col_clean]:
                                if variation in available_data:
                                    found_value = available_data[variation]
                                    found_source = f"fuzzy: {variation} (variation of {excel_col_clean})"
                                    break
                    
                    if found_value:
                        row_data[excel_col_original] = found_value
                        matches_found += 1
                        print(f"DEBUG: ✓ Found: {excel_col_original} = {found_value} ({found_source})")
                    else:
                        print(f"DEBUG: ❌ Not found: {excel_col_original}")
                        # Show available fields for debugging
                        available_fields = [k for k in available_data.keys() if excel_col_clean in k.lower()]
                        if available_fields:
                            print(f"DEBUG:    Similar fields available: {available_fields[:3]}")
                
                print(f"DEBUG: Sample {sample_id}: {matches_found}/{len(excel_columns)} columns matched")
                enriched_data.append(row_data)
                
            except Exception as e:
                print(f"DEBUG: Error processing sample {sample_id}: {str(e)}")
                # Include error but still add the sample ID
                row_data = {'SampleId': sample_id}
                # Add any Excel columns that were processed before the error
                for excel_col in excel_columns:
                    if excel_col not in row_data and not pd.isna(excel_col):
                        row_data[excel_col] = 'Error: ' + str(e)
                enriched_data.append(row_data)
        
        print(f"DEBUG: Processing complete. Enriched {len(enriched_data)} samples")
        
        return jsonify({
            'success': True,
            'message': f'Excel uploaded successfully! Found {len(sample_ids)} sample IDs and enriched {len(enriched_data)} with database data. Preview below shows first 5 rows - click "Download Results" to get the complete filled Excel file.',
            'sample_count': len(sample_ids),
            'enriched_count': len([d for d in enriched_data if 'Error' not in str(d)]),
            'data': enriched_data,
            'preview': {
                'headers': list(enriched_data[0].keys()) if enriched_data else [],
                'rows': enriched_data[:5]  # First 5 rows for preview
            }
        })
        
    except Exception as e:
        print(f"DEBUG: Excel upload error: {str(e)}")
        return jsonify({'error': f'Error processing file: {str(e)}'}), 500

@chat_bp.route('/upload_samples', methods=['POST'])
def upload_sample_list():
    """Handle Excel file upload for sample list processing"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({'error': 'Please upload an Excel file (.xlsx or .xls)'}), 400
    
    try:
        # Read Excel file
        df = pd.read_excel(file)
        
        # Find sample ID column dynamically
        sample_id_column = None
        
        # Look for any column that might contain sample IDs
        for col in df.columns:
            col_lower = col.lower().strip()
            # Check if column name suggests it contains IDs
            if any(id_word in col_lower for id_word in ['sample', 'id', 'code', 'identifier']):
                # Check if this column actually contains data that looks like sample IDs
                sample_data = df[col].dropna().head(5)
                if len(sample_data) > 0:
                    # Check if values look like sample IDs (contain letters and numbers)
                    for val in sample_data:
                        val_str = str(val).strip()
                        if (any(c.isalpha() for c in val_str) and 
                            any(c.isdigit() for c in val_str) and
                            len(val_str) > 3):  # Reasonable sample ID length
                            sample_id_column = col
                            break
                if sample_id_column:
                    break
        
        if not sample_id_column:
            return jsonify({'error': 'Could not find sample ID column. Please ensure your Excel has a column named "SampleId", "Sample ID", or similar.'}), 400
        
        # Extract and sanitize sample IDs
        sample_ids = [str(sid).strip() for sid in df[sample_id_column].dropna()]
        
        if not sample_ids:
            return jsonify({'error': 'No sample IDs found in the file'}), 400
        
        # Process samples using the new SmartLocalAI for dynamic queries
        smart_ai = get_smart_ai()
        results = []
        
        for sample_id in sample_ids[:100]:  # Process up to 100 samples
            try:
                # Use SmartLocalAI to find sample information dynamically
                response = smart_ai.ask(f"Find sample {sample_id}")
                results.append({
                    'SampleId': sample_id,
                    'Info': response
                })
            except Exception as e:
                results.append({
                    'SampleId': sample_id,
                    'Error': str(e)
                })
        
        return jsonify({
            'success': True,
            'total_samples': len(sample_ids),
            'processed_samples': len(results),
            'results': results
        })
        
    except Exception as e:
        return jsonify({'error': f'Error processing file: {str(e)}'}), 500

@chat_bp.route('/download_results', methods=['POST'])
def download_results():
    """Generate downloadable Excel file with sample results"""
    data = request.get_json()
    results = data.get('results', [])
    
    if not results:
        return jsonify({'error': 'No results to download'}), 400
    
    try:
        # Dynamically determine headers from results
        if not results:
            return jsonify({'error': 'No results to download'}), 400
            
        # Collect all unique keys across all result dictionaries
        all_keys = set()
        for res in results:
            all_keys.update(res.keys())
            
        # Create headers dynamically from all keys found in results
        # Sort alphabetically for consistent ordering, but put SampleId first
        all_keys = sorted([k for k in all_keys if k != 'SampleId'])
        headers = ['SampleId'] + all_keys
        
        # Create DataFrame from results with dynamic headers
        df = pd.DataFrame(results, columns=headers)
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Sample Results', index=False)
        
        output.seek(0)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'sample_results_{timestamp}.xlsx'
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        return jsonify({'error': f'Error creating download file: {str(e)}'}), 500

@chat_bp.route('/suggestions')
def get_suggestions():
    """Get question suggestions"""
    suggestions = [
        "Find information for sample CANB_ANA25_001",
        "Where is sample CANB_ANA24_1102 stored?",
        "What is the scientific name of sample CANB_ANA25_001?",
        "Show screening results for sample CANB_ANA25_001",
        "Which freezer contains sample CANB_ANA25_001?",
        "What is the RNA plate for sample CANB_ANA25_001?",
        "How many samples are in the database?",
        "How many positive Corona tests were found?",
        "What bat species are most common?",
        "Which provinces have samples?",
        "What are the screening statistics?",
        "How many sequences do we have?",
        "What virus types are present?",
        "When were samples recently uploaded?",
        # New enhanced features
        "Write SQL for counting bat samples",
        "Generate SQL to list all provinces",
        "Explain this SQL: SELECT COUNT(*) FROM bathost",
        "Explain the database schema",
        "Show table structures",
        "What columns are in the screening table?",
        "Fill Excel with sample data",
        "Upload Excel to auto-fill missing data",
        "How does the Excel upload feature work?"
    ]
    
    return jsonify({
        'success': True,
        'suggestions': suggestions
    })

# ML Training Routes
@chat_bp.route('/training')
def training_interface():
    """ML model training interface"""
    try:
        return render_template('ml_training.html')
    except Exception as e:
        return f"Template error: {str(e)}", 500

@chat_bp.route('/training/status', methods=['GET'])
def get_training_status():
    """Get current training status and model information"""
    try:
        db_type = session.get('db_type', 'sqlite')
        
        if db_type == 'sqlite':
            db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        else:
            db_config = session.get('db_params')
        
        if not db_config:
            return jsonify({
                'success': False,
                'error': 'No database connection available'
            }), 400
        
        # Initialize trainer
        trainer = DatabaseTrainer(db_config, db_type)
        
        # Get model information
        model_info = trainer.get_model_info()
        training_status = trainer.get_training_status()
        
        return jsonify({
            'success': True,
            'model_info': model_info,
            'training_status': training_status
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error getting training status: {str(e)}'
        }), 500

@chat_bp.route('/training/start', methods=['POST'])
def start_training():
    """Start ML model training"""
    try:
        data = request.get_json()
        force_retrain = data.get('force_retrain', False)
        
        db_type = session.get('db_type', 'sqlite')
        
        if db_type == 'sqlite':
            db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        else:
            db_config = session.get('db_params')
        
        if not db_config:
            return jsonify({
                'success': False,
                'error': 'No database connection available'
            }), 400
        
        # Initialize trainer
        trainer = DatabaseTrainer(db_config, db_type)
        
        # Check if models already exist
        if not force_retrain:
            model_info = trainer.get_model_info()
            if len(model_info['models_available']) >= 3:
                return jsonify({
                    'success': False,
                    'error': 'Models already trained. Use force_retrain=true to retrain.',
                    'models_available': model_info['models_available']
                }), 400
        
        # Start training (this might take time)
        print("Starting ML model training...")
        success = trainer.train_all_models()
        
        if success:
            # Get updated model info
            model_info = trainer.get_model_info()
            training_status = trainer.get_training_status()
            
            return jsonify({
                'success': True,
                'message': 'Models trained successfully!',
                'model_info': model_info,
                'training_status': training_status
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Training failed. Check database connection and try again.'
            }), 500
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Training error: {str(e)}'
        }), 500

@chat_bp.route('/training/predict', methods=['POST'])
def test_prediction():
    """Test trained models with a sample question"""
    try:
        data = request.get_json()
        question = data.get('question', '')
        
        if not question:
            return jsonify({
                'success': False,
                'error': 'No question provided'
            }), 400
        
        db_type = session.get('db_type', 'sqlite')
        
        if db_type == 'sqlite':
            db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        else:
            db_config = session.get('db_params')
        
        if not db_config:
            return jsonify({
                'success': False,
                'error': 'No database connection available'
            }), 400
        
        # Initialize trainer and load models
        trainer = DatabaseTrainer(db_config, db_type)
        models_loaded = trainer.load_models()
        
        if not models_loaded:
            return jsonify({
                'success': False,
                'error': 'No trained models available. Train models first.'
            }), 400
        
        # Test predictions
        intent_prediction = trainer.predict_intent(question)
        table_prediction = trainer.predict_table(question)
        
        return jsonify({
            'success': True,
            'question': question,
            'intent_prediction': intent_prediction,
            'table_prediction': table_prediction
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Prediction error: {str(e)}'
        }), 500

@chat_bp.route('/training/generate-questions', methods=['GET'])
def generate_test_questions():
    """Generate test questions from the actual database data"""
    try:
        db_type = session.get('db_type', 'sqlite')
        
        if db_type == 'sqlite':
            db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        else:
            db_config = session.get('db_params')
        
        if not db_config:
            return jsonify({
                'success': False,
                'error': 'No database connection available'
            }), 400
        
        # Initialize trainer
        trainer = DatabaseTrainer(db_config, db_type)
        
        # Collect training data (this generates questions from actual data)
        training_data = trainer.collect_training_data()
        
        if not training_data:
            return jsonify({
                'success': False,
                'error': 'No training data could be generated from database'
            }), 400
        
        # Extract unique questions from training data
        unique_questions = []
        seen_questions = set()
        
        for item in training_data:
            if item['question'] not in seen_questions:
                unique_questions.append({
                    'question': item['question'],
                    'category': item['category'],
                    'table': item['table']
                })
                seen_questions.add(item['question'])
        
        # Limit to reasonable number for testing (max 50)
        test_questions = unique_questions[:50]
        
        return jsonify({
            'success': True,
            'questions': test_questions,
            'total_generated': len(unique_questions)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error generating questions: {str(e)}'
        }), 500

@chat_bp.route('/training/models', methods=['GET'])
def list_models():
    """List available trained models"""
    try:
        db_type = session.get('db_type', 'sqlite')
        
        if db_type == 'sqlite':
            db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        else:
            db_config = session.get('db_params')
        
        if not db_config:
            return jsonify({
                'success': False,
                'error': 'No database connection available'
            }), 400
        
        trainer = DatabaseTrainer(db_config, db_type)
        model_info = trainer.get_model_info()
        
        return jsonify({
            'success': True,
            'model_info': model_info
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error listing models: {str(e)}'
        }), 500

@chat_bp.route('/training/delete', methods=['POST'])
def delete_models():
    """Delete trained models"""
    try:
        data = request.get_json()
        model_names = data.get('models', [])
        
        if not model_names:
            return jsonify({
                'success': False,
                'error': 'No models specified for deletion'
            }), 400
        
        db_type = session.get('db_type', 'sqlite')
        
        if db_type == 'sqlite':
            db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        else:
            db_config = session.get('db_params')
        
        if not db_config:
            return jsonify({
                'success': False,
                'error': 'No database connection available'
            }), 400
        
        trainer = DatabaseTrainer(db_config, db_type)
        deleted = []
        
        for model_name in model_names:
            try:
                if model_name == 'response_templates':
                    model_path = os.path.join(trainer.models_dir, f"{model_name}.json")
                else:
                    model_path = os.path.join(trainer.models_dir, f"{model_name}.pkl")
                
                if os.path.exists(model_path):
                    os.remove(model_path)
                    deleted.append(model_name)
            except Exception as e:
                print(f"Error deleting {model_name}: {e}")
        
        # Also delete metadata if all models are deleted
        if len(deleted) >= 3:
            metadata_path = os.path.join(trainer.models_dir, 'training_metadata.json')
            if os.path.exists(metadata_path):
                os.remove(metadata_path)
        
        return jsonify({
            'success': True,
            'deleted': deleted,
            'message': f'Deleted {len(deleted)} models'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error deleting models: {str(e)}'
        }), 500

# Model Versioning Routes
@chat_bp.route('/training/versions', methods=['GET'])
def get_model_versions():
    """Get list of all model versions"""
    try:
        db_type = session.get('db_type', 'sqlite')
        
        if db_type == 'sqlite':
            db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        else:
            db_config = session.get('db_params')
        
        if not db_config:
            return jsonify({
                'success': False,
                'error': 'No database connection available'
            }), 400
        
        trainer = DatabaseTrainer(db_config, db_type)
        versions = trainer.get_model_versions()
        
        return jsonify({
            'success': True,
            'versions': versions
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error getting model versions: {str(e)}'
        }), 500

@chat_bp.route('/training/versions/<version_id>', methods=['GET'])
def get_version_details(version_id):
    """Get detailed information about a specific model version"""
    try:
        db_type = session.get('db_type', 'sqlite')
        
        if db_type == 'sqlite':
            db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        else:
            db_config = session.get('db_params')
        
        if not db_config:
            return jsonify({
                'success': False,
                'error': 'No database connection available'
            }), 400
        
        trainer = DatabaseTrainer(db_config, db_type)
        version_info = trainer.get_version_info(version_id)
        
        if version_info:
            return jsonify({
                'success': True,
                'version_info': version_info
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Version {version_id} not found'
            }), 404
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error getting version details: {str(e)}'
        }), 500

@chat_bp.route('/training/versions/<version_id>/rollback', methods=['POST'])
def rollback_to_version(version_id):
    """Rollback to a specific model version"""
    try:
        db_type = session.get('db_type', 'sqlite')
        
        if db_type == 'sqlite':
            db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        else:
            db_config = session.get('db_params')
        
        if not db_config:
            return jsonify({
                'success': False,
                'error': 'No database connection available'
            }), 400
        
        trainer = DatabaseTrainer(db_config, db_type)
        
        if trainer.rollback_to_version(version_id):
            return jsonify({
                'success': True,
                'message': f'Successfully rolled back to version {version_id}'
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Failed to rollback to version {version_id}'
            }), 500
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error rolling back to version: {str(e)}'
        }), 500

@chat_bp.route('/training/versions/<version_id>/delete', methods=['POST'])
def delete_version(version_id):
    """Delete a specific model version"""
    try:
        data = request.get_json()
        confirm = data.get('confirm', False)
        
        if not confirm:
            return jsonify({
                'success': False,
                'error': 'Please confirm deletion by setting confirm=true'
            }), 400
        
        db_type = session.get('db_type', 'sqlite')
        
        if db_type == 'sqlite':
            db_config = session.get('db_path') or current_app.config.get('DATABASE_PATH')
        else:
            db_config = session.get('db_params')
        
        if not db_config:
            return jsonify({
                'success': False,
                'error': 'No database connection available'
            }), 400
        
        trainer = DatabaseTrainer(db_config, db_type)
        
        if trainer.delete_version(version_id):
            return jsonify({
                'success': True,
                'message': f'Successfully deleted version {version_id}'
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Failed to delete version {version_id}'
            }), 500
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error deleting version: {str(e)}'
        }), 500

