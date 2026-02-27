"""
Excel Import Manager for HaoXai with Security Integration
Handles dynamic column mapping, data validation, and secure data import
"""

import pandas as pd
import sqlite3
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
from .security import DatabaseSecurity

logger = logging.getLogger(__name__)

class ExcelImportManager:
    def __init__(self, db_connection, connection_type="sqlite", user_id=None):
        self.db_connection = db_connection
        self.connection_type = connection_type
        self.cursor = db_connection.cursor()
        self.user_id = user_id
        
        # Tables to exclude from Excel import
        self.excluded_tables = {
            'sequences',           # Sequence analysis data - managed by sequence analyzer
            'consensus_sequences',  # Consensus data - managed by sequence analyzer  
            'blast_results',        # BLAST results - managed by BLAST analyzer
            'blast_hits',          # BLAST hits - managed by BLAST analyzer
            'projects',            # Project management - manual creation
            'security_audit_log',   # Security audit logs - managed by security system
            'security_backup_log',  # Security backup logs - managed by security system
            'security_encrypted_fields', # Security encrypted fields - managed by security system
            'security_roles',       # Security roles - managed by security system
            'security_row_policies', # Security row policies - managed by security system
            'security_schema_protection', # Security schema protection - managed by security system
            'security_users',       # Security users - managed by security system
            'RecycleBin',          # Recycle bin - system managed
            # Add any other tables you want to exclude here
        }
        
        # Initialize security manager with db_path from session
        self.security = None
        if connection_type == "sqlite" and user_id:
            try:
                # Get db_path from the connection if possible, or use a default
                if hasattr(db_connection, 'db_path'):
                    db_path = db_connection.db_path
                else:
                    # For SQLite connections, we need to get the path differently
                    # This is a workaround - in a real implementation, we'd pass the db_path explicitly
                    import os
                    from flask import session
                    db_path = session.get('db_path', 'CAN2Database_v2 - Copy.db')
                
                self.security = DatabaseSecurity(db_path)
            except Exception as e:
                logger.error(f"Failed to initialize security: {e}")
                self.security = None
        
    def close(self): 
        if hasattr(self, 'cursor'):
            self.cursor.close()
    
    def add_excluded_table(self, table_name: str, reason: str = ""):
        """Add a table to the exclusion list"""
        self.excluded_tables.add(table_name)
        logger.info(f"Added table '{table_name}' to exclusion list. Reason: {reason}")
    
    def remove_excluded_table(self, table_name: str):
        """Remove a table from the exclusion list"""
        if table_name in self.excluded_tables:
            self.excluded_tables.remove(table_name)
            logger.info(f"Removed table '{table_name}' from exclusion list")
    
    def get_excluded_tables(self) -> set:
        """Get the current list of excluded tables"""
        return self.excluded_tables.copy()
    
    def set_excluded_tables(self, tables: set):
        """Set the complete list of excluded tables"""
        self.excluded_tables = set(tables)
        logger.info(f"Set excluded tables to: {', '.join(tables)}")
    
    def print_excluded_tables(self):
        """Print the current excluded tables with reasons"""
        print("\n🚫 Excluded Tables from Excel Import:")
        print("=" * 50)
        
        excluded_reasons = {
            'sequences': 'Sequence analysis data - managed by sequence analyzer',
            'consensus_sequences': 'Consensus data - managed by sequence analyzer',  
            'blast_results': 'BLAST results - managed by BLAST analyzer',
            'blast_hits': 'BLAST hits - managed by BLAST analyzer',
            'projects': 'Project management - manual creation',
            'security_audit_log': 'Security audit logs - managed by security system',
            'security_backup_log': 'Security backup logs - managed by security system',
            'security_encrypted_fields': 'Security encrypted fields - managed by security system',
            'security_roles': 'Security roles - managed by security system',
            'security_row_policies': 'Security row policies - managed by security system',
            'security_schema_protection': 'Security schema protection - managed by security system',
            'security_users': 'Security users - managed by security system',
            'RecycleBin': 'Recycle bin - system managed',
        }
        
        for table in sorted(self.excluded_tables):
            reason = excluded_reasons.get(table, 'Custom exclusion')
            print(f"  ❌ {table:<25} - {reason}")
        print("=" * 50)
        print(f"Total excluded tables: {len(self.excluded_tables)}")
        print()
    
    def show_available_tables(self):
        """Show all tables and which ones are available for Excel import"""
        print("\n📊 Database Tables - Excel Import Status:")
        print("=" * 60)
        
        # Get all tables
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        all_tables = [row[0] for row in self.cursor.fetchall()]
        
        included_count = 0
        excluded_count = 0
        
        for table in all_tables:
            if table in self.excluded_tables:
                status = "🚫 EXCLUDED"
                excluded_count += 1
            else:
                status = "✅ AVAILABLE"
                included_count += 1
            
            print(f"  {status:<12} {table}")
        
        print("=" * 60)
        print(f"📊 Summary:")
        print(f"  ✅ Available for import: {included_count} tables")
        print(f"  🚫 Excluded from import: {excluded_count} tables")
        print(f"  📋 Total tables: {len(all_tables)}")
        print()
    
    def validate_and_map_columns(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Dynamically map Excel columns to database tables.
        Returns: {table_name: [list of matched db column names]}
        Also stores detailed mapping info in self._last_mapping_details.
        """
        
        # Get all table schemas from database
        table_schemas = self.get_database_schema()
        
        # Normalize Excel column names 
        available_columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_').tolist()
        df.columns = available_columns
        
        mapped_columns = {}
        # Store detailed mapping: {table: {db_col: {excel_col, confidence}}}
        self._last_mapping_details = {}
        
        for table_name, table_columns in table_schemas.items():
            table_mapped_cols = []
            table_details = {}
            
            # Get FK columns and primary keys for this table to exclude them from mapping
            excluded_columns = set()
            try:
                # Get FK columns
                self.cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                fk_columns = {row[3] for row in self.cursor.fetchall()}  # from column is at index 3
                excluded_columns.update(fk_columns)
                
                # Get primary key columns
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                table_info = self.cursor.fetchall()
                pk_columns = {col[1] for col in table_info if col[5] == 1}  # pk flag is at index 5
                excluded_columns.update(pk_columns)
                
                print(f"[DEBUG] Excluded columns for {table_name}: FK={fk_columns}, PK={pk_columns}")
            except:
                pass  # If table doesn't exist or no constraints, continue
            
            for db_col in table_columns:
                # Skip FK columns and primary keys - they should be auto-resolved/auto-generated
                if db_col in excluded_columns:
                    print(f"[DEBUG] Skipping excluded column: {table_name}.{db_col}")
                    continue
                    
                match_result = self.find_best_column_match(db_col, available_columns)
                if match_result:
                    excel_col, confidence = match_result
                    table_mapped_cols.append(db_col)
                    table_details[db_col] = {
                        'excel_col': excel_col,
                        'confidence': confidence  # 'exact', 'synonym', or 'normalized'
                    }
            
            if table_mapped_cols:
                mapped_columns[table_name] = table_mapped_cols
                self._last_mapping_details[table_name] = table_details
        
        return mapped_columns
    
    def get_mapping_details(self) -> Dict:
        """Return the detailed mapping info from the last validate_and_map_columns call."""
        return getattr(self, '_last_mapping_details', {})
    
    def determine_primary_table_for_sheet(self, sheet_name: str, column_mappings: Dict[str, List[str]], df: pd.DataFrame) -> str:
        """Determine which table should be the primary import target for this sheet"""
        
        # Define sheet name patterns and their primary tables
        sheet_patterns = {
            'hosts': ['host', 'bat_host', 'rodent_host', 'market'],
            'samples': ['sample', 'swab', 'tissue', 'bat_swab', 'bat_tissue', 'rodent_sample'],
            'screening_results': ['screening', 'screen'],
            'storage_locations': ['storage', 'freezer', 'location', 'box', 'cabinet'],
            'taxonomy': ['taxon'],
            'locations': ['location'],
            'teams': ['team']
        }
        
        print(f"[DEBUG] determine_primary_table_for_sheet called for '{sheet_name}'")
        for table, mapped in column_mappings.items():
            if mapped:
                print(f"[DEBUG]   Found {len(mapped)} mapped columns for table {table}")
        
        # Define key columns that must be present for each table type (relaxed)
        key_columns = {
            'hosts': ['scientific_name', 'bag_code', 'bag_id', 'source_id', 'village'], 
            'samples': ['sample_code', 'sample_id', 'saliva_id', 'anal_id', 'blood_id', 'tissue_id', 'bag_id', 'bag_code'],
            'screening_results': ['cdna_date', 'pancorona', 'sample_id', 'sampleid'],
            'storage_locations': ['freezer_name', 'location', 'sample_id', 'sampleid']
        }
        
        sheet_name_lower = sheet_name.lower()
        
        # First, try exact matches based on sheet name patterns
        for primary_table, patterns in sheet_patterns.items():
            for pattern in patterns:
                if pattern in sheet_name_lower:
                    # Check if this table has its key columns mapped
                    if primary_table in column_mappings:
                        mapped_cols = column_mappings[primary_table]
                        required_keys = key_columns.get(primary_table, [])
                        
                        # Check if all required key columns are present and contain valid data
                        valid_key_count = 0
                        for key_col in required_keys:
                            if key_col in mapped_cols:
                                excel_col = self.find_excel_column_for_db_column(key_col, list(df.columns))
                                if excel_col:
                                    # Check first row for valid data
                                    first_value = df.iloc[0].get(excel_col) if len(df) > 0 else None
                                    if pd.notna(first_value):
                                        # Additional validation for key columns
                                        if key_col == 'collectors':
                                            if isinstance(first_value, str) and any(char.isalpha() for char in str(first_value)) and not str(first_value).replace('.', '').replace(' ', '').replace(',', '').isdigit():
                                                valid_key_count += 1
                                        elif key_col == 'collection_date':
                                            if isinstance(first_value, str) and ('-' in str(first_value) or '/' in str(first_value)) or isinstance(first_value, (int, float)) and first_value > 1900:
                                                valid_key_count += 1
                                        else:
                                            if str(first_value).strip():
                                                valid_key_count += 1
                        
                        if valid_key_count >= 1 or len(mapped_cols) >= 3:
                            print(f"[DEBUG]   Accepting table {primary_table} for sheet {sheet_name}")
                            return primary_table
        
        # Fallback: choose the table with the most columns mapped
        if column_mappings:
            valid_tables = {}
            for table_name, mapped_cols in column_mappings.items():
                required_keys = key_columns.get(table_name, [])
                if not required_keys:
                    valid_tables[table_name] = len(mapped_cols)
                else:
                    key_score = sum(1 for key in required_keys if key in mapped_cols)
                    if key_score >= 1 or len(mapped_cols) >= 5:
                        valid_tables[table_name] = len(mapped_cols) + (key_score * 2)
            
            if valid_tables:
                primary_table = max(valid_tables.items(), key=lambda x: x[1])[0]
                print(f"[DEBUG]   Fallback selected table '{primary_table}' with score {valid_tables[primary_table]}")
                return primary_table
        
        return None
    
    def get_database_schema(self) -> Dict[str, List[str]]:
        """Get all table schemas from database dynamically (supports SQLite and MySQL/MariaDB)"""
        schemas = {}
        
        if self.connection_type in ('mysql', 'mariadb'):
            # MySQL/MariaDB path
            self.cursor.execute("SHOW TABLES")
            tables = self.cursor.fetchall()
            
            for row in tables:
                table_name = row[0]
                if table_name in self.excluded_tables:
                    logger.info(f"Excluding table '{table_name}' from Excel import")
                    continue
                
                self.cursor.execute(f"DESCRIBE `{table_name}`")
                columns = self.cursor.fetchall()
                
                table_columns = []
                for col in columns:
                    col_name = col[0]
                    if (col_name != 'id' and
                        col_name != 'created_at' and col_name != 'updated_at' and col_name != 'created_by' and
                        col_name != 'is_encrypted' and col_name != 'access_level'):
                        table_columns.append(col_name)
                
                if table_columns:
                    schemas[table_name] = table_columns
        else:
            # SQLite path
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = self.cursor.fetchall()
            
            for (table_name,) in tables:
                if table_name in self.excluded_tables:
                    logger.info(f"Excluding table '{table_name}' from Excel import")
                    continue
                    
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                columns = self.cursor.fetchall()
                
                table_columns = []
                for col in columns:
                    col_name = col[1]
                    if (col_name != 'id' and
                        col_name != 'created_at' and col_name != 'updated_at' and col_name != 'created_by' and
                        col_name != 'is_encrypted' and col_name != 'access_level'):
                        table_columns.append(col_name)
                
                if table_columns:
                    schemas[table_name] = table_columns
        
        return schemas
    
    def find_best_column_match(self, db_column: str, excel_columns: List[str]):
        """Find the best matching Excel column for a database column.
        
        Uses a strict, tiered approach to avoid false positives:
          Tier 1: Exact match (after normalization)
          Tier 2: Known synonym lookup
          Tier 3: Normalized match (strip all separators)
        
        Returns: tuple(excel_col, confidence) or None
            confidence is 'exact', 'synonym', or 'normalized'
        """
        db_col_lower = db_column.lower()
        
        # ── Tier 1: Exact match ──
        if db_col_lower in excel_columns:
            return (db_col_lower, 'exact')
        
        # ── Tier 2: Known synonym lookup ──
        # Each DB column can have a list of known Excel aliases.
        # These are carefully curated — no wild guessing.
        known_synonyms = {
            'sample_code': ['samplecode', 'sample_no', 'sample_number'],
            'sample_id': ['sampleid', 'sample_no', 'sample_number'],
            'bag_code': ['bagcode', 'bag_no', 'bag_number'],
            'bag_id': ['bagid', 'bag_no', 'bag_number'],
            'field_id': ['fieldid', 'field_no', 'fieldno', 'fld_no'],
            'host_id': ['hostid', 'host_pk'],
            'sample_type': ['sampletype'],
            'collection_date': ['collectiondate', 'date_collected', 'collect_date'],
            'scientific_name': ['scientificname', 'sci_name', 'species_name'],
            'host_type': ['hosttype'],
            'test_type': ['testtype'],
            'test_result': ['testresult'],
            'team_name': ['teamname'],
            'storage_temperature': ['storagetemperature', 'temp', 'temperature'],
            'preservation_method': ['preservationmethod'],
            'freezer_name': ['freezername'],
            'cabinet_no': ['cabinetno', 'cabinet_number'],
            'province': ['prov'],
            'district': ['dist'],
            'village': ['vill'],
            'weight_g': ['weight'],
            'notes': ['note', 'comment', 'comments', 'remark', 'remarks'],
            'source_id': ['sourceid', 'source_no'],
            'sex': ['gender'],
            'age': ['age_class', 'age_group'],
            'forearm_mm': ['forearm', 'fa_mm'],
            'body_mass_g': ['body_mass', 'mass_g', 'mass'],
            'head_body_mm': ['head_body', 'hb_mm'],
            'tail_mm': ['tail', 'tail_length'],
            'ear_mm': ['ear', 'ear_length'],
            'tibia_mm': ['tibia', 'tibia_length'],
            'hind_foot_mm': ['hind_foot', 'hindfoot', 'hf_mm'],
            'collectors': ['collector', 'collected_by'],
            'pancorona': ['pan_corona', 'pcr_result'],
            'cdna_date': ['cdna_synthesis_date'],
            'saliva_id': ['salivaid', 'saliva_no'],
            'anal_id': ['analid', 'anal_no'],
            'blood_id': ['bloodid', 'blood_no'],
            'tissue_id': ['tissueid', 'tissue_no'],
            'urine_id': ['urineid', 'urine_no'],
            'ecto_id': ['ectoid', 'ecto_no'],
            'rna_plate': ['rna_plate_no', 'rnaplate'],
            'project_code': ['projectcode', 'project_no'],
            'project_name': ['projectname'],
        }
        
        synonyms = known_synonyms.get(db_col_lower, [])
        for synonym in synonyms:
            if synonym in excel_columns:
                return (synonym, 'synonym')
        
        # ── Tier 3: Normalized match (strip underscores/spaces/hyphens) ──
        db_normalized = db_col_lower.replace('_', '').replace(' ', '').replace('-', '')
        for excel_col in excel_columns:
            excel_normalized = excel_col.replace('_', '').replace(' ', '').replace('-', '')
            if db_normalized == excel_normalized and len(db_normalized) >= 3:
                return (excel_col, 'normalized')
        
        # No match found — do NOT fall back to substring guessing
        return None
    
    def find_excel_column_for_db_column(self, db_column: str, excel_columns: List[str]) -> str:
        """Find the Excel column that corresponds to a database column.
        Returns just the column name (unwraps the tuple from find_best_column_match).
        """
        result = self.find_best_column_match(db_column, excel_columns)
        return result[0] if result else None
    
    def get_or_create_location(self, province: str, district: str = None, village: str = None, site_name: str = None) -> int:
        """Get or create location record"""
        if not province:
            province = 'Laos'  # Default province
            
        # Check if location exists
        self.cursor.execute("""
            SELECT location_id FROM locations 
            WHERE country = 'Laos' AND province = ? 
            AND (district = ? OR district IS NULL) 
            AND (village = ? OR village IS NULL)
            AND (site_name = ? OR site_name IS NULL)
        """, (province, district, village, site_name))
        
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Create new location
        self.cursor.execute("""
            INSERT INTO locations (country, province, district, village, site_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ('Laos', province, district, village, site_name, datetime.now(), datetime.now()))
        
        return self.cursor.lastrowid
    
    def get_or_create_taxonomy(self, scientific_name: str) -> int:
        """Get existing taxonomy ID or create new one"""
        if not scientific_name:
            return None
        
        # Check if taxonomy exists
        self.cursor.execute("SELECT taxonomy_id FROM taxonomy WHERE scientific_name = ?", (scientific_name,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Extract species from scientific_name (take the part after the last space)
        species = scientific_name.split()[-1] if ' ' in scientific_name else scientific_name
        
        # Create new taxonomy with required fields
        self.cursor.execute("""
            INSERT INTO taxonomy (species, scientific_name, created_at, updated_at)
            VALUES (?, ?, ?, ?)
        """, (species, scientific_name, datetime.now(), datetime.now()))
        
        return self.cursor.lastrowid
    
    def get_or_create_environmental_sample(self, source_id: str = None, pool_id: str = None, 
                                         province: str = None, district: str = None, 
                                         village: str = None, site_name: str = None) -> int:
        """Get existing environmental sample ID or create new one"""
        if not source_id and not pool_id:
            return None
        
        # Check if environmental sample exists
        if source_id:
            self.cursor.execute("SELECT env_sample_id FROM environmental_samples WHERE source_id = ?", (source_id,))
        else:
            self.cursor.execute("SELECT env_sample_id FROM environmental_samples WHERE pool_id = ?", (pool_id,))
        
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Get or create location
        location_id = None
        if province or district or village or site_name:
            location_id = self.get_or_create_location(province, district, village, site_name)
        
        # Create new environmental sample
        self.cursor.execute("""
            INSERT INTO environmental_samples (source_id, pool_id, location_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (source_id, pool_id, location_id, datetime.now(), datetime.now()))
        
        return self.cursor.lastrowid
    
    def discover_dynamic_fk_rules(self):
        """Discover ALL FK relationships from the current database automatically"""
        fk_rules = {}
        
        try:
            # Get all tables
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in self.cursor.fetchall()]
            
            for table in tables:
                # Skip excluded tables
                if table in self.excluded_tables:
                    continue
                    
                self.cursor.execute(f"PRAGMA foreign_key_list({table})")
                fks = self.cursor.fetchall()
                
                if fks:
                    table_fk_rules = {}
                    
                    for fk in fks:
                        # fk format: (id, seq, table, from_col, to_col, on_update, on_delete, match)
                        fk_id, seq, target_table, from_col, to_col, on_update, on_delete, match = fk
                        
                        # DYNAMICALLY discover match columns from target table schema
                        self.cursor.execute(f"PRAGMA table_info({target_table})")
                        target_columns = [col[1] for col in self.cursor.fetchall()]
                        
                        # Smart matching based on column names and common patterns
                        match_cols = []
                        
                        # Primary key column
                        self.cursor.execute(f"PRAGMA table_info({target_table})")
                        pk_cols = [col[1] for col in self.cursor.fetchall() if col[5] == 1]  # col[5] is pk flag
                        
                        # Add primary key as match column
                        if pk_cols:
                            match_cols.extend(pk_cols)
                        
                        # Add common identifier columns
                        id_patterns = ['id', '_id', 'code', 'name', 'number', 'no']
                        for col in target_columns:
                            col_lower = col.lower()
                            if any(pattern in col_lower for pattern in id_patterns):
                                if col not in match_cols:
                                    match_cols.append(col)
                        
                        # Add specific columns based on table name patterns
                        target_lower = target_table.lower()
                        if 'host' in target_lower:
                            host_cols = ['bag_id', 'bag_code', 'source_id', 'field_id', 'field_no', 'host_id']
                            match_cols.extend([col for col in host_cols if col in target_columns])
                        elif 'sample' in target_lower:
                            sample_cols = ['sample_id', 'sample_code', 'saliva_id', 'anal_id', 'urine_id', 'ecto_id', 'blood_id', 'tissue_id', 'rna_plate']
                            match_cols.extend([col for col in sample_cols if col in target_columns])
                        elif 'location' in target_lower:
                            loc_cols = ['province', 'district', 'village', 'site_name', 'country']
                            match_cols.extend([col for col in loc_cols if col in target_columns])
                        elif 'taxonom' in target_lower:
                            tax_cols = ['scientific_name', 'species', 'genus', 'family', 'order', 'class']
                            match_cols.extend([col for col in tax_cols if col in target_columns])
                        elif 'environment' in target_lower:
                            env_cols = ['source_id', 'pool_id', 'env_id']
                            match_cols.extend([col for col in env_cols if col in target_columns])
                        elif 'project' in target_lower:
                            proj_cols = ['project_code', 'project_name', 'project_id']
                            match_cols.extend([col for col in proj_cols if col in target_columns])
                        elif 'team' in target_lower:
                            team_cols = ['team_name', 'team_id', 'team_code']
                            match_cols.extend([col for col in team_cols if col in target_columns])
                        elif 'user' in target_lower:
                            user_cols = ['user_id', 'username', 'email']
                            match_cols.extend([col for col in user_cols if col in target_columns])
                        elif 'dept' in target_lower:
                            dept_cols = ['dept_id', 'dept_name', 'department_id']
                            match_cols.extend([col for col in dept_cols if col in target_columns])
                        elif 'emp' in target_lower:
                            emp_cols = ['emp_id', 'emp_name', 'employee_id']
                            match_cols.extend([col for col in emp_cols if col in target_columns])
                        
                        # Remove duplicates while preserving order
                        match_cols = list(dict.fromkeys(match_cols))
                        
                        table_fk_rules[from_col] = {
                            'target_table': target_table,
                            'match_cols': match_cols,
                            'target_pk': to_col
                        }
                    
                    if table_fk_rules:
                        fk_rules[table] = table_fk_rules
                        
        except Exception as e:
            logger.error(f"Failed to discover FK rules: {e}")
            # Fall back to basic rules
            fk_rules = {
                'hosts': {
                    'location_id': {'target_table': 'locations', 'match_cols': ['province', 'district', 'village', 'site_name']},
                    'taxonomy_id': {'target_table': 'taxonomy', 'match_cols': ['scientific_name', 'species']}
                },
                'samples': {
                    'host_id': {'target_table': 'hosts', 'match_cols': ['bag_id', 'source_id', 'field_id']},
                    'location_id': {'target_table': 'locations', 'match_cols': ['province', 'district', 'village']}
                }
            }
        
        return fk_rules
    
    def get_existing_hosts_count(self) -> int:
        """Get count of existing hosts records"""
        self.cursor.execute("SELECT COUNT(*) FROM hosts")
        return self.cursor.fetchone()[0]
    
    def get_existing_samples_count(self) -> int:
        """Get count of existing samples records"""
        self.cursor.execute("SELECT COUNT(*) FROM samples")
        return self.cursor.fetchone()[0]
    
    def get_existing_screening_count(self) -> int:
        """Get count of existing screening records"""
        self.cursor.execute("SELECT COUNT(*) FROM screening")
        return self.cursor.fetchone()[0]
    
    def get_existing_storage_count(self) -> int:
        """Get count of existing storage records"""
        self.cursor.execute("SELECT COUNT(*) FROM storage")
        return self.cursor.fetchone()[0]
    
    def get_existing_projects_count(self) -> int:
        """Get count of existing projects records"""
        self.cursor.execute("SELECT COUNT(*) FROM projects")
        return self.cursor.fetchone()[0]
    
    def get_existing_table_count(self, table_name: str) -> int:
        """Get count of existing records for any table"""
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"[DEBUG] Error getting count for {table_name}: {e}")
            return 0
    
    def get_required_columns(self, table_name: str) -> List[str]:
        """Get required/important columns for a table from database schema"""
        required_columns = []
        
        if self.connection_type in ('mysql', 'mariadb'):
            # MySQL/MariaDB: DESCRIBE returns (Field, Type, Null, Key, Default, Extra)
            self.cursor.execute(f"DESCRIBE `{table_name}`")
            columns = self.cursor.fetchall()
            
            for col in columns:
                col_name = col[0]
                is_nullable = col[2].upper() == 'YES'
                is_pk = col[3].upper() == 'PRI'
                
                if col_name in ['id', 'host_id', 'screening_id', 'storage_id', 
                              'taxonomy_id', 'location_id', 'team_id', 'project_id',
                              'created_at', 'updated_at', 'created_by']:
                    continue
                
                if col_name == 'sample_id' and not is_pk:
                    if not is_nullable:
                        required_columns.append(col_name)
                    continue
                
                if not is_nullable and not is_pk:
                    required_columns.append(col_name)
        else:
            # SQLite: PRAGMA table_info returns (cid, name, type, notnull, dflt_value, pk)
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            
            for col in columns:
                col_name = col[1]
                not_null = col[3] == 1
                primary_key = col[5] == 1
                
                if col_name in ['id', 'host_id', 'screening_id', 'storage_id', 
                              'taxonomy_id', 'location_id', 'team_id', 'project_id',
                              'created_at', 'updated_at', 'created_by']:
                    continue
                
                if col_name == 'sample_id' and not primary_key:
                    if not_null:
                        required_columns.append(col_name)
                    continue
                
                if not_null and not primary_key:
                    required_columns.append(col_name)
        
        return required_columns
    
    def validate_dynamic_import(self, column_mappings: Dict[str, List[str]]) -> Dict[str, Any]:
        """Dynamic validation that works with any database schema"""
        if not column_mappings:
            return {
                'success': False,
                'error': 'No matching columns found in Excel file'
            }
        
        # Find the most important tables that have data
        available_tables = list(column_mappings.keys())
        
        # Get required columns for each table dynamically
        validation_results = {}
        for table_name, columns in column_mappings.items():
            required_cols = self.get_required_columns(table_name)
            available_required = [col for col in required_cols if col in columns]
            
            # Check if table has any required columns
            if required_cols:
                # Table has required columns - check if they're present
                if available_required:
                    validation_results[table_name] = {
                        'status': 'success',
                        'required_found': available_required,
                        'required_missing': [col for col in required_cols if col not in columns],
                        'columns_count': len(columns)
                    }
                else:
                    validation_results[table_name] = {
                        'status': 'error',
                        'required_found': available_required,
                        'required_missing': required_cols,
                        'columns_count': len(columns),
                        'message': f'Required columns missing for {table_name} table: {", ".join(required_cols)}'
                    }
            else:
                # Table has no required columns - check if it has any columns at all
                if columns:
                    validation_results[table_name] = {
                        'status': 'success',
                        'required_found': [],
                        'required_missing': [],
                        'columns_count': len(columns),
                        'message': f'No required columns for {table_name} table - all columns are optional'
                    }
                else:
                    validation_results[table_name] = {
                        'status': 'warning',
                        'required_found': [],
                        'required_missing': [],
                        'columns_count': 0,
                        'message': f'No columns found for {table_name} table'
                    }
        
        return validation_results
    
    def get_or_create_taxonomy(self, scientific_name: str) -> int:
        """Get existing taxonomy ID or create new one"""
        if not scientific_name:
            return None
        
        # Check if taxonomy exists
        self.cursor.execute("SELECT taxonomy_id FROM taxonomy WHERE scientific_name = ?", (scientific_name,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Extract species from scientific_name (take the part after the last space)
        species = scientific_name.split()[-1] if ' ' in scientific_name else scientific_name
        
        # Create new taxonomy with required fields
        self.cursor.execute("""
            INSERT INTO taxonomy (species, scientific_name, created_at, updated_at)
            VALUES (?, ?, ?, ?)
        """, (species, scientific_name, datetime.now(), datetime.now()))
        
        return self.cursor.lastrowid
    
    def get_or_create_team(self, team_name: str) -> int:
        """Get existing team ID or create new one"""
        if not team_name:
            return None
        
        # Check if team exists
        self.cursor.execute("SELECT team_id FROM teams WHERE team_name = ?", (team_name,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        
        # Create new team entry
        self.cursor.execute("""
            INSERT INTO teams (team_name, created_at, updated_at)
            VALUES (?, ?, ?)
        """, (team_name, datetime.now(), datetime.now()))
        
        return self.cursor.lastrowid
    
    def build_dynamic_insert(self, table_name: str, columns: List[str], additional_columns: List[str] = None) -> tuple:
        """Build dynamic INSERT statement with proper column mapping"""
        # Get table schema to ensure columns exist
        if self.connection_type in ('mysql', 'mariadb'):
            self.cursor.execute(f"DESCRIBE `{table_name}`")
            schema_columns = [col[0] for col in self.cursor.fetchall()]
        else:
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            schema_columns = [col[1] for col in self.cursor.fetchall()]
        
        # Filter columns to only those that exist in the table
        valid_columns = [col for col in columns if col in schema_columns]
        
        # Add additional columns if provided and they exist in schema
        if additional_columns:
            for col in additional_columns:
                if col in schema_columns and col not in valid_columns:
                    valid_columns.append(col)
        
        # Build INSERT statement
        if self.connection_type in ('mysql', 'mariadb'):
            columns_str = ", ".join([f"`{c}`" for c in valid_columns])
            placeholders = ", ".join(["%s" for _ in valid_columns])
            insert_sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        else:
            columns_str = ", ".join(valid_columns)
            placeholders = ", ".join(["?" for _ in valid_columns])
            insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        return insert_sql, valid_columns
    
    def import_hosts_data(self, df: pd.DataFrame, columns: List[str]) -> List[int]:
        """Import hosts data using dynamic column mapping"""
        host_ids = []
        
        # Create column name to Excel column mapping
        column_map = {col: col for col in columns}
        
        for _, row in df.iterrows():
            # Get location_id dynamically
            location_id = None
            province = row.get(column_map.get('province', 'province'))
            district = row.get(column_map.get('district', 'district'))
            village = row.get(column_map.get('village', 'village'))
            
            if province or district or village:
                location_id = self.get_or_create_location(province, district, village)
            else:
                # Create a default location if none provided
                location_id = self.get_or_create_location('Unknown', None, None)
            
            # Get taxonomy_id dynamically
            taxonomy_id = None
            scientific_name = row.get(column_map.get('scientific_name', 'scientific_name'))
            if scientific_name:
                taxonomy_id = self.get_or_create_taxonomy(scientific_name)
            
            # Get team_id dynamically
            team_id = None
            team_name = row.get(column_map.get('team_name', 'team_name'))
            if team_name:
                team_id = self.get_or_create_team(team_name)
            
            # Get source_id (use Excel value or default)
            source_id = row.get(column_map.get('source_id', 'source_id'))
            if not source_id:
                source_id = f"EXCEL_IMPORT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Get host_type (use Excel value or default)
            host_type = row.get(column_map.get('host_type', 'host_type'))
            if not host_type:
                host_type = "other"  # Default value for host_type
            
            # Build dynamic INSERT statement
            additional_cols = ['source_id', 'host_type', 'location_id', 'taxonomy_id', 'team_id', 'created_at', 'updated_at', 'created_by']
            insert_sql, valid_columns = self.build_dynamic_insert('hosts', columns, additional_cols)
            
            # Build values list dynamically
            values = []
            for col in valid_columns:
                if col == 'source_id':
                    values.append(source_id)
                elif col == 'host_type':
                    values.append(host_type)
                elif col == 'location_id':
                    values.append(location_id)
                elif col == 'taxonomy_id':
                    values.append(taxonomy_id)
                elif col == 'team_id':
                    values.append(team_id)
                elif col == 'created_at':
                    values.append(datetime.now())
                elif col == 'updated_at':
                    values.append(datetime.now())
                elif col == 'created_by':
                    values.append(self.user_id)
                elif col == 'notes':
                    notes = row.get(column_map.get(col, col))
                    if self.security and notes:
                        notes = self.security.encrypt_data(notes)
                    values.append(notes)
                else:
                    values.append(row.get(column_map.get(col, col)))
            
            # Execute dynamic insert
            self.cursor.execute(insert_sql, values)
            host_ids.append(self.cursor.lastrowid)
        
        return host_ids
    
    def import_samples_data(self, df: pd.DataFrame, columns: List[str], host_ids: List[int], import_mode: str = 'skip') -> List[int]:
        """Import samples data using dynamic column mapping"""
        sample_ids = []
        
        # Create column name to Excel column mapping
        column_map = {col: col for col in columns}
        
        for i, (_, row) in enumerate(df.iterrows()):
            sample_code = row.get(column_map.get('sample_code', 'sample_code'))
            
            # Ensure sample_code is always provided (it's REQUIRED)
            if not sample_code or pd.isna(sample_code):
                sample_code = f"SAMPLE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i+1:03d}"
            
            # Check if sample already exists
            if sample_code:
                self.cursor.execute("SELECT sample_id FROM samples WHERE sample_code = ?", (sample_code,))
                existing = self.cursor.fetchone()
                if existing:
                    if import_mode == 'update':
                        # Get location_id dynamically
                        location_id = None
                        province = row.get(column_map.get('province', 'province'))
                        district = row.get(column_map.get('district', 'district'))
                        village = row.get(column_map.get('village', 'village'))
                        
                        if province or district or village:
                            location_id = self.get_or_create_location(province, district, village)
                        
                        # Build dynamic UPDATE statement
                        update_columns = [col for col in columns if col not in ['sample_code', 'created_at', 'created_by']]
                        if location_id and 'location_id' not in update_columns:
                            update_columns.append('location_id')
                        update_columns.append('updated_at')
                        
                        # Build SET clause dynamically
                        set_clause = ", ".join([f"{col} = ?" for col in update_columns])
                        update_sql = f"UPDATE samples SET {set_clause} WHERE sample_code = ?"
                        
                        # Build values list dynamically
                        values = []
                        for col in update_columns:
                            if col == 'location_id':
                                values.append(location_id)
                            elif col == 'updated_at':
                                values.append(datetime.now())
                            elif col == 'notes':
                                notes = row.get(column_map.get(col, col))
                                if self.security and notes:
                                    notes = self.security.encrypt_data(notes)
                                values.append(notes)
                            else:
                                values.append(row.get(column_map.get(col, col)))
                        values.append(sample_code)
                        
                        self.cursor.execute(update_sql, values)
                        sample_ids.append(existing[0])
                    else:
                        # Skip existing sample
                        sample_ids.append(existing[0])
                    continue
            
            # Get location_id for sample dynamically
            location_id = None
            province = row.get(column_map.get('province', 'province'))
            district = row.get(column_map.get('district', 'district'))
            village = row.get(column_map.get('village', 'village'))
            
            if province or district or village:
                location_id = self.get_or_create_location(province, district, village)
            
            # Build dynamic INSERT statement
            additional_cols = ['location_id', 'created_at', 'updated_at', 'created_by']
            insert_sql, valid_columns = self.build_dynamic_insert('samples', columns, additional_cols)
            
            # Build values list dynamically
            values = []
            for col in valid_columns:
                if col == 'location_id':
                    values.append(location_id)
                elif col == 'created_at':
                    values.append(datetime.now())
                elif col == 'updated_at':
                    values.append(datetime.now())
                elif col == 'created_by':
                    values.append(self.user_id)
                elif col == 'notes':
                    notes = row.get(column_map.get(col, col))
                    if self.security and notes:
                        notes = self.security.encrypt_data(notes)
                    values.append(notes)
                else:
                    values.append(row.get(column_map.get(col, col)))
            
            # Execute dynamic insert
            self.cursor.execute(insert_sql, values)
            sample_id = self.cursor.lastrowid
            sample_ids.append(sample_id)
            
            # Link to host if available
            if i < len(host_ids) and host_ids[i]:
                self.cursor.execute("""
                    INSERT INTO host_samples (host_id, sample_id, collection_sequence, created_at)
                    VALUES (?, ?, ?, ?)
                """, (host_ids[i], sample_id, 1, datetime.now()))
        
        return sample_ids
    
    def import_projects_data(self, df: pd.DataFrame, columns: List[str]):
        """Import projects data using dynamic column mapping"""
        # Check if we have actual project data
        if not columns or 'project_code' not in columns:
            return []  # No project data to import
        
        project_ids = []
        # Create column name to Excel column mapping
        column_map = {col: col for col in columns}
        
        for i, (_, row) in enumerate(df.iterrows()):
            project_code = row.get(column_map.get('project_code', 'project_code'))
            
            # Skip if project_code is empty/null
            if not project_code or pd.isna(project_code):
                continue
            
            # Check if project already exists
            self.cursor.execute("SELECT project_id FROM projects WHERE project_code = ?", (project_code,))
            existing = self.cursor.fetchone()
            if existing:
                # Skip existing project or update as needed
                project_ids.append(existing[0])
                continue
            
            # Build dynamic INSERT statement
            additional_cols = ['created_at', 'updated_at']
            insert_sql, valid_columns = self.build_dynamic_insert('projects', columns, additional_cols)
            
            # Build values list dynamically
            values = []
            for col in valid_columns:
                if col == 'created_at':
                    values.append(datetime.now())
                elif col == 'updated_at':
                    values.append(datetime.now())
                elif col == 'project_name':
                    # Default to project_code if project_name not provided
                    project_name = row.get(column_map.get(col, col))
                    values.append(project_name if project_name else project_code)
                elif col == 'description':
                    # Default to empty string if description not provided
                    description = row.get(column_map.get(col, col))
                    values.append(description if description else '')
                elif col == 'status':
                    # Default to 'active' if status not provided
                    status = row.get(column_map.get(col, col))
                    values.append(status if status else 'active')
                else:
                    values.append(row.get(column_map.get(col, col)))
            
            # Execute dynamic insert
            self.cursor.execute(insert_sql, values)
            project_id = self.cursor.lastrowid
            project_ids.append(project_id)
        
        return project_ids
    
    def import_taxonomy_data(self, df: pd.DataFrame, columns: List[str]) -> List[int]:
        """Import taxonomy data using dynamic column mapping"""
        taxonomy_ids = []
        
        # Create column name to Excel column mapping
        column_map = {col: col for col in columns}
        
        for _, row in df.iterrows():
            scientific_name = row.get(column_map.get('scientific_name', 'scientific_name'))
            
            # Skip if scientific_name is empty
            if not scientific_name or pd.isna(scientific_name):
                continue
            
            # Extract species from scientific_name if not provided
            species = row.get(column_map.get('species', 'species'))
            if not species or pd.isna(species):
                # Extract species as the last word from scientific_name
                species = scientific_name.split()[-1] if ' ' in scientific_name else scientific_name
            
            # Check if taxonomy already exists
            self.cursor.execute("SELECT taxonomy_id FROM taxonomy WHERE scientific_name = ?", (scientific_name,))
            existing = self.cursor.fetchone()
            if existing:
                taxonomy_ids.append(existing[0])
                continue
            
            # Build dynamic INSERT statement
            additional_cols = ['species', 'created_at', 'updated_at']
            insert_sql, valid_columns = self.build_dynamic_insert('taxonomy', columns, additional_cols)
            
            # Build values list dynamically
            values = []
            for col in valid_columns:
                if col == 'species':
                    values.append(species)
                elif col == 'created_at':
                    values.append(datetime.now())
                elif col == 'updated_at':
                    values.append(datetime.now())
                elif col == 'notes':
                    notes = row.get(column_map.get(col, col))
                    if self.security and notes:
                        notes = self.security.encrypt_data(notes)
                    values.append(notes)
                else:
                    values.append(row.get(column_map.get(col, col)))
            
            # Execute dynamic insert
            self.cursor.execute(insert_sql, values)
            taxonomy_ids.append(self.cursor.lastrowid)
        
        return taxonomy_ids
    
    def import_locations_data(self, df: pd.DataFrame, columns: List[str]) -> List[int]:
        """Import locations data using dynamic column mapping"""
        location_ids = []
        
        # Create column name to Excel column mapping
        column_map = {col: col for col in columns}
        
        for _, row in df.iterrows():
            province = row.get(column_map.get('province', 'province'))
            district = row.get(column_map.get('district', 'district'))
            village = row.get(column_map.get('village', 'village'))
            
            # Skip if all location fields are empty
            if not province and not district and not village:
                continue
            
            # Get or create location
            location_id = self.get_or_create_location(province, district, village)
            location_ids.append(location_id)
        
        return location_ids
    
    def import_screening_data(self, df: pd.DataFrame, columns: List[str], sample_ids: List[int]):
        """Import screening data using dynamic column mapping"""
        # Create column name to Excel column mapping
        column_map = {col: col for col in columns}
        
        for i, (_, row) in enumerate(df.iterrows()):
            if i >= len(sample_ids):
                continue
                
            # Get team_id dynamically
            team_id = None
            team_name = row.get(column_map.get('team_name', 'team_name'))
            if team_name:
                team_id = self.get_or_create_team(team_name)
            
            # Build dynamic INSERT statement
            additional_cols = ['sample_id', 'team_id', 'created_at', 'updated_at']
            insert_sql, valid_columns = self.build_dynamic_insert('screening', columns, additional_cols)
            
            # Build values list dynamically
            values = []
            for col in valid_columns:
                if col == 'sample_id':
                    values.append(sample_ids[i])
                elif col == 'team_id':
                    values.append(team_id)
                elif col == 'created_at':
                    values.append(datetime.now())
                elif col == 'updated_at':
                    values.append(datetime.now())
                elif col == 'notes':
                    notes = row.get(column_map.get(col, col))
                    if self.security and notes:
                        notes = self.security.encrypt_data(notes)
                    values.append(notes)
                else:
                    values.append(row.get(column_map.get(col, col)))
            
            # Execute dynamic insert
            self.cursor.execute(insert_sql, values)
    
    def import_generic_table_data(self, table_name: str, df: pd.DataFrame, columns: List[str], 
                                foreign_key_data: Dict = None, import_mode: str = 'skip', sheet_name: str = '',
                                custom_mappings: Dict = None, excluded_columns: List = None, session_ids: Dict = None) -> Dict[str, Any]:
        """Generic method to import data into any table"""
        if len(df) > 0:
            pass
        
        records_created = 0
        records_updated = 0
        created_ids = []  # Track IDs of created records
        
        # Create column name mapping: database_col -> excel_col
        column_map = {}
        available_excel_columns = df.columns.tolist()
        
        print(f"[DEBUG] Processing table: {table_name}")
        print(f"[DEBUG] Available Excel columns: {available_excel_columns}")
        print(f"[DEBUG] Custom mappings: {custom_mappings}")
        print(f"[DEBUG] Excluded columns: {excluded_columns}")
        
        # Add custom-mapped columns to the columns list if they're not already there
        if custom_mappings:
            for db_col in custom_mappings.keys():
                if db_col not in columns:
                    columns.append(db_col)
                    print(f"[DEBUG] Added custom-mapped column: {db_col}")
        
        for db_col in columns:
            # Check for exclusion first
            if excluded_columns and db_col in excluded_columns:
                print(f"[DEBUG] Excluding column: {db_col}")
                continue

            # Check for custom mapping FIRST (this is the fix)
            if custom_mappings and db_col in custom_mappings:
                excel_col = custom_mappings[db_col]
                # Try exact match first
                if excel_col in available_excel_columns:
                    print(f"[DEBUG] Using custom mapping: {db_col} -> {excel_col}")
                    column_map[db_col] = excel_col
                    continue
                # Try case-insensitive match
                else:
                    for available_col in available_excel_columns:
                        if available_col.lower() == excel_col.lower():
                            print(f"[DEBUG] Using custom mapping (case-insensitive): {db_col} -> {available_col}")
                            column_map[db_col] = available_col
                            break
                    else:
                        print(f"[WARNING] Custom mapping for {db_col} -> {excel_col} not found in Excel file")
                    continue  # Skip default logic if custom mapping was provided to skip
                continue  # This is CRITICAL - skip default logic when custom mapping is provided

            # Default logic: Find the Excel column that maps to this database column
            # ONLY run if no custom mapping was provided
            excel_col = self.find_excel_column_for_db_column(db_col, available_excel_columns)
            if excel_col:
                column_map[db_col] = excel_col
                print(f"[DEBUG] Auto-detected mapping: {db_col} -> {excel_col}")
            else:
                # If no mapping found, use the database column name as-is
                column_map[db_col] = db_col
                print(f"[DEBUG] Using direct mapping: {db_col} -> {db_col}")
        
        print(f"[DEBUG] Final column map for {table_name}: {column_map}")
        
        
        # Get table schema to understand required columns and relationships
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        table_schema = self.cursor.fetchall()
        
        # Build column info dictionary
        column_info = {}
        primary_key = None
        for col in table_schema:
            col_id, col_name, col_type, not_null, default, pk = col
            
            if pk:
                primary_key = col_name
            column_info[col_name] = {
                'col_id': col_id,
                'col_name': col_name,
                'col_type': col_type,
                'not_null': not_null,
                'default': default,
                'primary_key': pk
            }
        
        # Process each row in the DataFrame
        for row_idx, row in df.iterrows():
            # Skip rows that don't have meaningful data in key columns
            has_meaningful_data = False
            for col in columns:
                excel_col = column_map.get(col)
                if excel_col:
                    value = row.get(excel_col)
                    if pd.notna(value) and str(value).strip():
                        has_meaningful_data = True
                        break
            
            if not has_meaningful_data:
                continue
            
            # Additional validation for hosts table - collectors must be valid names
            if table_name == 'hosts':
                collectors_col = self.find_excel_column_for_db_column('collectors', list(df.columns))
                if collectors_col:
                    collectors_value = row.get(collectors_col)
                    if pd.notna(collectors_value):
                        # Collectors should be text containing names, not numbers
                        if not (isinstance(collectors_value, str) and 
                               any(char.isalpha() for char in str(collectors_value)) and 
                               not str(collectors_value).replace('.', '').replace(' ', '').replace(',', '').isdigit()):
                            continue
            # Resolve foreign keys for this specific row
            resolved_fks = self._resolve_foreign_keys_dynamic(table_name, row, available_excel_columns, session_ids) or {}
            
            # Infer host_type if importing into hosts table and it's not provided
            if table_name == 'hosts':
                host_type_col = self.find_excel_column_for_db_column('host_type', list(df.columns))
                if not host_type_col or pd.isna(row.get(host_type_col)):
                    # Infer from sheet name
                    sheet_name_lower = sheet_name.lower()
                    if 'bat' in sheet_name_lower:
                        resolved_fks['host_type'] = 'Bat'
                    elif 'rodent' in sheet_name_lower:
                        resolved_fks['host_type'] = 'Rodent'
                    elif 'market' in sheet_name_lower:
                        resolved_fks['host_type'] = 'Market'

            # Improved duplicate detection using business keys instead of auto-incrementing PKs
            existing_record = None
            
            # Debug why duplicate check might fail or return existing even if new
            
            # Define business key columns for each table to detect duplicates
            business_keys = {
                'hosts': ['bag_id', 'bag_code', 'field_no', 'field_id', 'scientific_name', 'source_id'], # Prioritize bag IDs for hosts
                'samples': ['sample_id', 'sample_code', 'saliva_id', 'anal_id', 'urine_id', 'ecto_id', 'blood_id', 'tissue_id', 'rna_plate'], 
                'screening_results': ['cdna_date', 'pancorona', 'sample_id'],
                'storage_locations': ['freezer_name', 'location', 'sample_id'],
                'morphometrics': ['host_id']  # Each host can only have one morphometrics record
            }
            
            if table_name in business_keys:
                # Build duplicate check query using business keys
                key_cols = business_keys[table_name]
                conditions = []
                values = []
                valid_key_cols = 0  # Initialize counter
                
                for key_col in key_cols:
                    # Check if key_col is in resolved_fks (for FK columns)
                    if key_col in resolved_fks:
                        conditions.append(f"{key_col} = ?")
                        values.append(resolved_fks[key_col])
                        valid_key_cols += 1
                    # Check if key_col is in available columns
                    elif key_col in columns:
                        excel_col = column_map.get(key_col)
                        if excel_col:
                            value = row.get(excel_col)
                            if pd.notna(value):
                                # Validate key column data before adding to query
                                is_valid = True
                                if key_col == 'collectors':
                                    if not (isinstance(value, str) and any(char.isalpha() for char in str(value)) and not str(value).replace('.', '').replace(' ', '').replace(',', '').isdigit()):
                                        is_valid = False
                                elif key_col == 'collection_date':
                                    if not (isinstance(value, str) and ('-' in str(value) or '/' in str(value)) or isinstance(value, (int, float)) and value > 1900):
                                        is_valid = False
                                elif not str(value).strip():
                                    is_valid = False
                                    
                                if is_valid:
                                    conditions.append(f"{key_col} = ?")
                                    values.append(value)
                                    valid_key_cols += 1
                
                if conditions:
                    # Use correct primary key column for each table
                    pk_column = 'id'
                    if table_name == 'locations':
                        pk_column = 'location_id'
                    elif table_name == 'taxonomy':
                        pk_column = 'taxonomy_id'
                    elif table_name == 'environmental_samples':
                        pk_column = 'env_sample_id'
                    elif table_name == 'samples':
                        pk_column = 'sample_id'
                    elif table_name == 'hosts':
                        pk_column = 'host_id'
                    elif table_name == 'projects':
                        pk_column = 'project_id'
                    elif table_name == 'departments':
                        pk_column = 'dept_id'
                    elif table_name == 'employees':
                        pk_column = 'emp_id'
                    elif table_name == 'morphometrics':
                        pk_column = 'morpho_id'
                    
                    duplicate_query = f"SELECT {pk_column} FROM {table_name} WHERE {' AND '.join(conditions)}"
                    try:
                        existing_record = self.cursor.execute(duplicate_query, values).fetchone()
                    except Exception as e:
                        logger.warning(f"Duplicate check failed for {table_name}: {e}")
            
            if existing_record:
                if import_mode == 'update':
                    # Update existing record logic
                    if primary_key:
                        pk_value = existing_record[0]  # id from existing record
                        
                        update_columns = [col for col in columns if col != primary_key]
                        if update_columns:
                            # Add resolved columns to update_columns if missing
                            for fk_col in resolved_fks:
                                if fk_col != primary_key and fk_col not in update_columns:
                                    update_columns.append(fk_col)
                                    
                            final_update_columns = []
                            values = []
                            
                            for col in update_columns:
                                # Priority 1: Resolved FKs (always include if present)
                                if col in resolved_fks:
                                    final_update_columns.append(col)
                                    values.append(resolved_fks[col])
                                    continue
                                    
                                # Priority 2: Excel data (skip if NULL/empty)
                                excel_value = row.get(column_map.get(col, col))
                                
                                # Skip if NULL/empty/whitespace-only to avoid overwriting existing data
                                if pd.isna(excel_value) or excel_value is None or (isinstance(excel_value, str) and not excel_value.strip()):
                                    continue
                                    
                                final_update_columns.append(col)
                                if hasattr(excel_value, 'item'):  # numpy scalar types
                                    values.append(excel_value.item())
                                else:
                                    values.append(excel_value)
                            
                            if final_update_columns:
                                # Check if updated_at column exists in table
                                set_clause = ", ".join([f'"{col}" = ?' for col in final_update_columns])
                                if 'updated_at' in column_info:
                                    update_sql = f"UPDATE {table_name} SET {set_clause}, updated_at = ? WHERE {primary_key} = ?"
                                    values.extend([datetime.now(), pk_value])
                                else:
                                    update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key} = ?"
                                    values.append(pk_value)
                                
                                try:
                                    self.cursor.execute(update_sql, values)
                                    records_updated += 1
                                except Exception as e:
                                    logger.error(f"Failed to update {table_name} record: {e}")
                            else:
                                pass
                elif import_mode == 'skip':
                    records_updated += 1
                    continue
            else:
                insert_columns = list(columns)  # Start with mapped columns
                
                # Add resolved foreign key columns if not already present
                for fk_col in resolved_fks:
                    if fk_col not in insert_columns:
                        insert_columns.append(fk_col)
                        
                foreign_values = {}
                
                # Add required system columns
                system_cols = []
                if 'created_at' in column_info:
                    system_cols.append('created_at')
                if 'updated_at' in column_info:
                    system_cols.append('updated_at')
                if 'created_by' in column_info and self.user_id:
                    system_cols.append('created_by')
                if 'is_encrypted' in column_info:
                    system_cols.append('is_encrypted')
                if 'access_level' in column_info:
                    system_cols.append('access_level')
                
                insert_columns.extend(system_cols)
                
                # Build INSERT statement
                placeholders = ", ".join(["?" for _ in insert_columns])
                columns_str = ", ".join([f'"{c}"' for c in insert_columns])
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                values = []
                for col in insert_columns:
                    # Check if this column was resolved via foreign keys
                    if col in resolved_fks:
                        values.append(resolved_fks[col])
                        continue
                        
                    # Handle other columns
                    if col == 'created_at' or col == 'updated_at':
                        values.append(datetime.now())
                    elif col == 'created_by':
                        values.append(self.user_id)
                    elif col == 'is_encrypted':
                        values.append(0)  # Default to not encrypted
                    elif col == 'access_level':
                        values.append('researcher')  # Default access level
                    elif col in foreign_values:
                        values.append(foreign_values[col])
                        values.append(foreign_values[col])
                    elif col == 'notes' and self.security:
                        notes = row.get(column_map.get(col, col))
                        if notes:
                            notes = self.security.encrypt_data(notes)
                        values.append(notes)
                    else:
                        excel_col = column_map.get(col, col)
                        excel_value = row.get(excel_col)
                        
                        # Special handling for FK columns that were added to dataframe
                        if col in ['host_id', 'sample_id'] and col in df.columns:
                            # Use dataframe direct access for FK columns
                            excel_value = df.loc[row.name if hasattr(row, 'name') else row_idx, col]
                            excel_value = df.loc[row.name if hasattr(row, 'name') else row_idx, col]
                        
                        # Handle pandas data types and NaN values properly
                        if pd.isna(excel_value) or excel_value is None:
                            values.append(None)
                        elif hasattr(excel_value, 'item'):  # numpy scalar types
                            values.append(excel_value.item())
                        else:
                            values.append(excel_value)
                
                try:
                    self.cursor.execute(insert_sql, values)
                    records_created += 1
                    record_id = self.cursor.lastrowid
                    created_ids.append(record_id)
                    
                    # Track created record for session FK resolution
                    if session_ids is None:
                        session_ids = {}
                    if table_name not in session_ids:
                        session_ids[table_name] = []
                    
                    # Create record dict with key identifying fields for FK matching
                    record_data = {'id': record_id}
                    
                    # Comprehensive matching fields for all FK relationships
                    all_match_fields = [
                        'bag_id', 'bag_code', 'source_id', 'field_id', 'field_no',
                        'sample_id', 'sample_code', 'saliva_id', 'anal_id', 'urine_id', 
                        'ecto_id', 'blood_id', 'tissue_id', 'rna_plate',
                        'scientific_name', 'species', 'genus', 'family', 'order_name',
                        'province', 'district', 'village', 'site_name',
                        'pool_id', 'bathost_id', 'rodenthost_id', 'marketsampleandhost_id',
                        'batswab_id', 'battissue_id', 'rodentswab_id'
                    ]
                    
                    for match_col in all_match_fields:
                        if match_col in column_map:
                            excel_value = row.get(column_map[match_col])
                            if pd.notna(excel_value):
                                record_data[match_col] = str(excel_value)
                    
                    session_ids[table_name].append(record_data)
                    print(f"[DEBUG] Tracked {table_name} record {record_id} for session FK resolution")
                    
                except Exception as e:
                    logger.error(f"Failed to create {table_name} record: {e}")
        
        return {
            'records_created': records_created,
            'records_updated': records_updated,
            'total_processed': len(df),
            'created_ids': created_ids
        }
    
    def import_storage_data(self, df: pd.DataFrame, columns: List[str], sample_ids: List[int]):
        """Import storage data using dynamic column mapping"""
        # Create column name to Excel column mapping
        column_map = {col: col for col in columns}
        
        for i, (_, row) in enumerate(df.iterrows()):
            if i >= len(sample_ids):
                continue
            
            # Build dynamic INSERT statement
            additional_cols = ['sample_id', 'created_at', 'updated_at']
            insert_sql, valid_columns = self.build_dynamic_insert('storage', columns, additional_cols)
            
            # Build values list dynamically
            values = []
            for col in valid_columns:
                if col == 'sample_id':
                    values.append(sample_ids[i])
                elif col == 'created_at':
                    values.append(datetime.now())
                elif col == 'updated_at':
                    values.append(datetime.now())
                elif col == 'notes':
                    notes = row.get(column_map.get(col, col))
                    if self.security and notes:
                        notes = self.security.encrypt_data(notes)
                    values.append(notes)
                else:
                    values.append(row.get(column_map.get(col, col)))
            
            # Execute dynamic insert
            self.cursor.execute(insert_sql, values)
    
    def import_excel_file(self, file_path: str, import_mode: str = 'skip', custom_mappings: Dict = None, excluded_columns: Dict = None) -> Dict[str, Any]:
        """Main method to import Excel file with multi-sheet data"""
        try:
            print(f"[DEBUG] Starting multi-sheet import with mode: {import_mode}")
            
            # Read all sheets from Excel file
            all_sheets = pd.read_excel(file_path, sheet_name=None)
            print(f"[DEBUG] Found {len(all_sheets)} sheets: {list(all_sheets.keys())}")
            
            # Track overall results
            overall_validation_results = {}
            overall_modified_tables = {}
            total_rows_processed = 0
            total_hosts_created = 0
            total_samples_created = 0
            total_projects_created = 0
            
            # Track IDs across all sheets for cross-sheet foreign key relationships
            all_host_ids = []
            all_sample_ids = []
            
            # Session tracking for FK resolution within this import
            session_ids = {}
            
            sheet_results = {}
            
            # Process each sheet
            for sheet_name, df in all_sheets.items():
                print(f"[DEBUG] ===== STARTING SHEET: {sheet_name} =====")
                print(f"[DEBUG] Accumulated IDs before sheet: hosts={len(all_host_ids)}, samples={len(all_sample_ids)}")
                
                # Skip empty sheets
                if df.empty:
                    print(f"[DEBUG] Skipping empty sheet: {sheet_name}")
                    sheet_results[sheet_name] = {
                        'success': True,
                        'message': 'Sheet is empty - skipped',
                        'rows_processed': 0,
                        'hosts_created': 0,
                        'samples_created': 0,
                        'projects_created': 0
                    }
                    continue
                    
                print(f"[DEBUG] Sheet '{sheet_name}' loaded with {len(df)} rows and columns: {list(df.columns)}")
                
                # Validate and map columns for this sheet
                column_mappings = self.validate_and_map_columns(df)
                print(f"[DEBUG] Sheet '{sheet_name}' column mappings: {column_mappings}")
                
                # Determine the primary table for this sheet
                primary_table = self.determine_primary_table_for_sheet(sheet_name, column_mappings, df)
                print(f"[DEBUG] Sheet '{sheet_name}' primary table: {primary_table}")
                print(f"[DEBUG] Sheet '{sheet_name}' all column mappings: {column_mappings}")
                
                # Filter column mappings to only include the primary table
                if primary_table:
                    filtered_mappings = {primary_table: column_mappings.get(primary_table, [])}
                    print(f"[DEBUG] Filtered mappings for '{sheet_name}': {filtered_mappings}")
                else:
                    print(f"[DEBUG] No primary table found for '{sheet_name}', skipping sheet")
                    sheet_results[sheet_name] = {
                        'success': False,
                        'error': f'No suitable primary table found for sheet {sheet_name}'
                    }
                    continue
                
                # Check if we have any tables with required columns
                if not filtered_mappings:
                    sheet_results[sheet_name] = {
                        'success': False,
                        'error': 'No matching columns found in this sheet'
                    }
                    continue
                
                # Dynamic validation that works with any database schema
                validation_results = self.validate_dynamic_import(filtered_mappings)
                print(f"[DEBUG] Sheet '{sheet_name}' validation results: {validation_results}")
                
                # For multi-sheet imports, be more lenient - only skip sheets with NO valid mappings
                has_valid_mappings = any(
                    result.get('status') in ['success', 'warning']  # Allow both success and warning
                    for result in validation_results.values()
                )
                
                if not has_valid_mappings:
                    sheet_results[sheet_name] = {
                        'success': False,
                        'error': f'No valid column mappings found for sheet {sheet_name}'
                    }
                    continue
                
                # For truly dynamic imports, don't enforce "critical" table requirements
                # Allow any table with valid mappings to proceed
                # Show warnings for validation issues but don't block import
                
                # Import data dynamically for all available tables in this sheet
                # Use accumulated IDs directly (no copies needed)
                project_ids = []  # Initialize in sheet scope
                
                # Track which tables were actually modified for this sheet
                modified_tables = {}
                
                # Track created vs updated records for this sheet
                created_records = {}
                updated_records = {}
                
                # Process each table for this sheet
                # Sort tables by dependency order to ensure foreign key relationships work
                table_dependency_order = {
                    'hosts': 1,           # Process hosts first
                    'samples': 2,         # Process samples after hosts
                    'screening_results': 3, # Process screening after samples
                    'storage_locations': 3  # Process storage after samples
                }
                
                # Sort filtered_mappings by dependency order
                sorted_tables = sorted(filtered_mappings.items(), 
                                     key=lambda x: table_dependency_order.get(x[0], 99))
                
                # Process each table for this sheet
                # Sort tables by dependency order to ensure foreign key relationships work
                table_dependency_order = {
                    'hosts': 1,           # Process hosts first
                    'samples': 2,         # Process samples after hosts
                    'screening_results': 3, # Process screening after samples
                    'storage_locations': 3  # Process storage after samples
                }
                
                # Sort column_mappings by dependency order
                sorted_tables = sorted(column_mappings.items(), 
                                     key=lambda x: table_dependency_order.get(x[0], 99))
                
                # FIRST: Process all tables and collect IDs for foreign key relationships
                # This ensures IDs are available before FK assignment
                sheet_created_ids = {'hosts': [], 'samples': []}
                
                for table_name, columns in sorted_tables:
                    # Skip FK assignment for now, just collect IDs from previous sheets
                    pass
                
                # Now process tables - linking will be handled at the row level
                for table_name, columns in sorted_tables:
                    try:
                        # Row-level linking is now handled within import_generic_table_data
                        # based on shared identifiers in the data
                        
                        # Use generic import method
                        result = self.import_generic_table_data(
                            table_name=table_name,
                            df=df,
                            columns=columns,
                            foreign_key_data=None,
                            import_mode=import_mode,
                            sheet_name=sheet_name,
                            custom_mappings=custom_mappings.get(table_name) if custom_mappings else None,
                            excluded_columns=excluded_columns.get(table_name) if excluded_columns else None,
                            session_ids=session_ids
                        )
                        
                        # Collect IDs of created records for foreign key relationships
                        if 'created_ids' in result and result['created_ids']:
                            if table_name == 'hosts':
                                all_host_ids.extend(result['created_ids'])
                                print(f"[DEBUG] Added {len(result['created_ids'])} host IDs to accumulated list (total: {len(all_host_ids)})")
                            elif table_name == 'samples':
                                all_sample_ids.extend(result['created_ids'])
                                print(f"[DEBUG] Added {len(result['created_ids'])} sample IDs to accumulated list (total: {len(all_sample_ids)})")
                        
                        modified_tables[table_name] = {
                            'created': result['records_created'],
                            'modified': result['records_updated'] > 0,
                            'total_after': self.get_existing_table_count(table_name),
                            'total_before': self.get_existing_table_count(table_name) - result['total_processed']
                        }
                        
                        print(f"[DEBUG] Generic import for '{table_name}': {result}")
                        
                    except Exception as e:
                        print(f"[ERROR] Failed to import table '{table_name}': {e}")
                        modified_tables[table_name] = {
                            'created': 0,
                            'modified': False,
                            'total_after': self.get_existing_table_count(table_name),
                            'total_before': self.get_existing_table_count(table_name)
                        }
                
                # Commit transaction for this sheet
                self.db_connection.commit()
                
                # Aggregate results for this sheet
                sheet_rows_processed = len(df)
                sheet_hosts_created = len(all_host_ids)
                sheet_samples_created = len(all_sample_ids)
                sheet_projects_created = len(project_ids) if 'projects' in column_mappings else 0
                
                sheet_results[sheet_name] = {
                    'success': True,
                    'message': f'Successfully imported {sheet_rows_processed} rows from sheet {sheet_name}',
                    'rows_processed': sheet_rows_processed,
                    'hosts_created': sheet_hosts_created,
                    'samples_created': sheet_samples_created,
                    'projects_created': sheet_projects_created,
                    'column_mappings': filtered_mappings,  # Use filtered mappings
                    'validation_results': validation_results,
                    'modified_tables': modified_tables
                }
                
                # Aggregate overall results
                total_rows_processed += sheet_rows_processed
                total_hosts_created += sheet_hosts_created
                total_samples_created += sheet_samples_created
                total_projects_created += sheet_projects_created
                
                # Merge validation results
                for table, result in validation_results.items():
                    if table not in overall_validation_results:
                        overall_validation_results[table] = result
                    else:
                        # If table appears in multiple sheets, keep the most recent result
                        overall_validation_results[table] = result
                
                # Merge modified tables
                for table, stats in modified_tables.items():
                    if table not in overall_modified_tables:
                        overall_modified_tables[table] = stats
                    else:
                        # Aggregate stats across sheets
                        overall_modified_tables[table]['created'] += stats['created']
                        overall_modified_tables[table]['modified'] = overall_modified_tables[table]['modified'] or stats['modified']
                        overall_modified_tables[table]['total_after'] = stats['total_after']
            
            return {
                'success': True,
                'message': f'Successfully imported {total_rows_processed} rows across {len(sheet_results)} sheets',
                # Backward compatibility - include old field names
                'rows_processed': total_rows_processed,
                'hosts_created': total_hosts_created,
                'samples_created': total_samples_created,
                'projects_created': total_projects_created,
                # New multi-sheet fields
                'total_sheets_processed': len([s for s in sheet_results.values() if s.get('success', False)]),
                'total_sheets': len(sheet_results),
                'total_rows_processed': total_rows_processed,
                'total_hosts_created': total_hosts_created,
                'total_samples_created': total_samples_created,
                'total_projects_created': total_projects_created,
                'sheet_results': sheet_results,
                'overall_validation_results': overall_validation_results,
                'overall_modified_tables': overall_modified_tables
            }
            
        except Exception as e:
            self.db_connection.rollback()
            return {
                'success': False,
                'error': f'Import failed: {str(e)}',
                'overall_validation_results': overall_validation_results,
                'overall_modified_tables': overall_modified_tables
            }
    
    def preview_excel_file(self, file_path: str) -> Dict[str, Any]:
        """Preview Excel file structure and data for all sheets"""
        try:
            # Read all sheets from Excel file
            all_sheets = pd.read_excel(file_path, sheet_name=None)
            
            sheet_results = {}
            total_rows_all_sheets = 0
            
            for sheet_name, df in all_sheets.items():
                # Skip empty sheets
                if df.empty:
                    continue
                    
                # Get column mappings for this sheet
                column_mappings = self.validate_and_map_columns(df)
                
                # Get sample data (first 5 rows)
                preview_data = df.head(5).fillna('').to_dict('records')
                
                sheet_results[sheet_name] = {
                    'total_rows': len(df),
                    'columns': list(df.columns),
                    'column_mappings': column_mappings,
                    'mapping_details': self.get_mapping_details(),
                    'preview_data': preview_data
                }
                
                total_rows_all_sheets += len(df)
            
            return {
                'success': True,
                'total_sheets': len(sheet_results),
                'total_rows_all_sheets': total_rows_all_sheets,
                'sheets': sheet_results
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Preview failed: {str(e)}'
            }
    def _resolve_foreign_keys_dynamic(self, table_name: str, row: pd.Series, available_excel_columns: List[str], session_ids: Dict = None) -> Dict[str, Any]:
        """Resolve foreign keys for a specific row based on shared identifiers - DYNAMIC"""
        resolved_fks = {}
        
        # Use dynamic FK discovery - works with ANY database!
        fk_rules = self.discover_dynamic_fk_rules()
        
        if table_name in fk_rules:
            for fk_col, rule in fk_rules[table_name].items():
                target_table = rule['target_table']
                match_cols = rule['match_cols']
                
                # Try each possible match column in order of preference
                for db_match_col in match_cols:
                    # Check if this match column exists in the target table schema
                    try:
                        self.cursor.execute(f"PRAGMA table_info({target_table})")
                        target_schema = [c[1] for c in self.cursor.fetchall()]
                        if db_match_col not in target_schema:
                            continue
                    except:
                        continue

                    # Find the Excel column that maps to this database match column
                    excel_match_col = self.find_excel_column_for_db_column(db_match_col, available_excel_columns)
                    if excel_match_col:
                        match_value = row.get(excel_match_col)
                        if pd.notna(match_value) and str(match_value).strip():
                            # First check session IDs (records created in this import session)
                            if session_ids and target_table in session_ids:
                                for session_record in session_ids[target_table]:
                                    if session_record.get(db_match_col) == str(match_value):
                                        resolved_fks[fk_col] = session_record['id']
                                        print(f"[DEBUG] Resolved {fk_col}={session_record['id']} for {table_name} using SESSION {db_match_col}={match_value}")
                                        break
                                if fk_col in resolved_fks:
                                    break
                            
                            # If not found in session, check existing database records
                            # Use correct primary key column name for each table
                            pk_column = 'id'
                            if target_table == 'locations':
                                pk_column = 'location_id'
                            elif target_table == 'taxonomy':
                                pk_column = 'taxonomy_id'
                            elif target_table == 'environmental_samples':
                                pk_column = 'env_sample_id'
                            elif target_table == 'samples':
                                pk_column = 'sample_id'
                            elif target_table == 'hosts':
                                pk_column = 'host_id'
                            elif target_table == 'projects':
                                pk_column = 'project_id'
                            elif target_table == 'departments':
                                pk_column = 'dept_id'
                            elif target_table == 'employees':
                                pk_column = 'emp_id'
                            
                            query = f"SELECT {pk_column} FROM {target_table} WHERE {db_match_col} = ?"
                            try:
                                self.cursor.execute(query, (str(match_value),))
                                res = self.cursor.fetchone()
                                if res:
                                    resolved_fks[fk_col] = res[0]
                                    print(f"[DEBUG] Resolved {fk_col}={res[0]} for {table_name} using DB {db_match_col}={match_value}")
                                    break # Success, move to next FK
                                else:
                                    # Auto-create missing records for supported target tables
                                    if target_table == 'locations' and db_match_col in ['province', 'district', 'village', 'site_name']:
                                        # Extract location data from row
                                        province = None
                                        district = None  
                                        village = None
                                        site_name = None
                                        
                                        for loc_col in ['province', 'district', 'village', 'site_name']:
                                            excel_loc_col = self.find_excel_column_for_db_column(loc_col, available_excel_columns)
                                            if excel_loc_col:
                                                loc_value = row.get(excel_loc_col)
                                                if pd.notna(loc_value) and str(loc_value).strip():
                                                    if loc_col == 'province':
                                                        province = str(loc_value)
                                                    elif loc_col == 'district':
                                                        district = str(loc_value)
                                                    elif loc_col == 'village':
                                                        village = str(loc_value)
                                                    elif loc_col == 'site_name':
                                                        site_name = str(loc_value)
                                        
                                        # Create location record
                                        location_id = self.get_or_create_location(province, district, village, site_name)
                                        if location_id:
                                            resolved_fks[fk_col] = location_id
                                            print(f"[DEBUG] Created and resolved {fk_col}={location_id} for {table_name} using new location {province}/{district}/{village}/{site_name}")
                                            break
                                    
                                    elif target_table == 'taxonomy' and db_match_col in ['scientific_name', 'species']:
                                        # Extract taxonomy data from row
                                        sci_name = str(match_value) if db_match_col == 'scientific_name' else None
                                        if not sci_name:
                                            excel_sci_col = self.find_excel_column_for_db_column('scientific_name', available_excel_columns)
                                            if excel_sci_col:
                                                sci_value = row.get(excel_sci_col)
                                                if pd.notna(sci_value) and str(sci_value).strip():
                                                    sci_name = str(sci_value)
                                        
                                        if sci_name:
                                            # Create taxonomy record
                                            taxonomy_id = self.get_or_create_taxonomy(sci_name)
                                            if taxonomy_id:
                                                resolved_fks[fk_col] = taxonomy_id
                                                print(f"[DEBUG] Created and resolved {fk_col}={taxonomy_id} for {table_name} using new taxonomy {sci_name}")
                                            break
                                    
                                    elif target_table == 'environmental_samples' and db_match_col in ['source_id', 'pool_id']:
                                        # Extract environmental sample data from row
                                        env_source_id = str(match_value) if db_match_col == 'source_id' else None
                                        env_pool_id = None
                                        
                                        # Try to get pool_id from row
                                        if not env_pool_id:
                                            excel_pool_col = self.find_excel_column_for_db_column('pool_id', available_excel_columns)
                                            if excel_pool_col:
                                                pool_value = row.get(excel_pool_col)
                                                if pd.notna(pool_value) and str(pool_value).strip():
                                                    env_pool_id = str(pool_value)
                                        
                                        # Get location data for environmental sample
                                        province = None
                                        district = None
                                        village = None
                                        site_name = None
                                        
                                        for loc_col in ['province', 'district', 'village', 'site_name']:
                                            excel_loc_col = self.find_excel_column_for_db_column(loc_col, available_excel_columns)
                                            if excel_loc_col:
                                                loc_value = row.get(excel_loc_col)
                                                if pd.notna(loc_value) and str(loc_value).strip():
                                                    if loc_col == 'province':
                                                        province = str(loc_value)
                                                    elif loc_col == 'district':
                                                        district = str(loc_value)
                                                    elif loc_col == 'village':
                                                        village = str(loc_value)
                                                    elif loc_col == 'site_name':
                                                        site_name = str(loc_value)
                                        
                                        if env_source_id or env_pool_id:
                                            # Create environmental sample record
                                            env_sample_id = self.get_or_create_environmental_sample(env_source_id, env_pool_id, province, district, village, site_name)
                                            if env_sample_id:
                                                resolved_fks[fk_col] = env_sample_id
                                                print(f"[DEBUG] Created and resolved {fk_col}={env_sample_id} for {table_name} using new environmental sample {env_source_id}/{env_pool_id}")
                                                break
                                    
                            except Exception as e:
                                print(f"[DEBUG] FK resolution failed for {table_name}.{fk_col} using {db_match_col}: {e}")
        
        return resolved_fks
