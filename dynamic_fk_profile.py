#!/usr/bin/env python3
"""
Dynamic Foreign Key Profile Builder
Works with any database schema without hardcoded relationships
"""

class DynamicFKProfileBuilder:
    def __init__(self, db_type='sqlite'):
        self.db_type = db_type
        self.fk_cache = {}  # Cache FK relationships for performance
        self.schema_cache = {}  # Cache table schemas
    
    def get_table_schema(self, cursor, table_name):
        """Get and cache table schema"""
        if table_name not in self.schema_cache:
            self.schema_cache[table_name] = self._discover_table_schema(cursor, table_name)
        return self.schema_cache[table_name]
    
    def _discover_table_schema(self, cursor, table_name):
        """Discover table schema dynamically"""
        try:
            if self.db_type == 'sqlite':
                cursor.execute(f'PRAGMA table_info("{table_name}")')
                columns = [{'name': row[1], 'type': row[2], 'pk': row[5]} for row in cursor.fetchall()]
            else:  # MySQL/MariaDB
                cursor.execute(f'DESCRIBE `{table_name}`')
                columns = [{'name': row[0], 'type': row[1], 'pk': row[3] == 'PRI'} for row in cursor.fetchall()]
            
            return columns
        except Exception as e:
            print(f"Error discovering schema for {table_name}: {e}")
            return []
    
    def get_fk_relationships(self, cursor, table_name):
        """Get and cache foreign key relationships for a table"""
        if table_name not in self.fk_cache:
            self.fk_cache[table_name] = self._discover_fk_relationships(cursor, table_name)
        return self.fk_cache[table_name]
    
    def _discover_fk_relationships(self, cursor, table_name):
        """Discover FK relationships dynamically"""
        relationships = []
        try:
            if self.db_type == 'sqlite':
                # Forward relationships (table references other tables)
                cursor.execute(f'PRAGMA foreign_key_list("{table_name}")')
                forward_fks = cursor.fetchall()
                for fk in forward_fks:
                    relationships.append({
                        'type': 'forward',
                        'from_table': table_name,
                        'from_column': fk[3],
                        'to_table': fk[2],
                        'to_column': fk[4]
                    })
                
                # Reverse relationships (other tables reference this table)
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                all_tables = [t[0] for t in cursor.fetchall()]
                
                for other_table in all_tables:
                    if other_table != table_name:
                        cursor.execute(f'PRAGMA foreign_key_list("{other_table}")')
                        other_fks = cursor.fetchall()
                        for fk in other_fks:
                            if fk[2] == table_name:  # References our table
                                relationships.append({
                                    'type': 'reverse',
                                    'from_table': other_table,
                                    'from_column': fk[3],
                                    'to_table': table_name,
                                    'to_column': fk[4]
                                })
            else:  # MySQL/MariaDB
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
                        'type': 'forward',
                        'from_table': table_name,
                        'from_column': row[1],
                        'to_table': row[0],
                        'to_column': row[2]
                    })
                
                # Reverse relationships
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
                        'type': 'reverse',
                        'from_table': row[0],
                        'from_column': row[1],
                        'to_table': table_name,
                        'to_column': row[2]
                    })
        
        except Exception as e:
            print(f"Error discovering FKs for {table_name}: {e}")
        
        return relationships
    
    def build_dynamic_profile(self, cursor, table_name, record_id, id_column='id'):
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
            fk_relationships = self.get_fk_relationships(cursor, table_name)
            print(f"DEBUG: Found {len(fk_relationships)} FK relationships for {table_name}")
            
            # Step 3: Follow forward relationships (main table -> related tables)
            for fk in fk_relationships:
                if fk['type'] == 'forward':
                    related_data = self._follow_forward_fk(cursor, main_record, fk)
                    if related_data:
                        table_key = f"{fk['to_table']}_info"
                        profile['related_data'][table_key] = related_data
                        profile['fk_paths_used'].append(f"FORWARD: {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}")
                        print(f"DEBUG: Found {fk['to_table']} via forward FK")
            
            # Step 4: Follow reverse relationships (related tables -> main table)
            for fk in fk_relationships:
                if fk['type'] == 'reverse':
                    related_data = self._follow_reverse_fk(cursor, main_record, fk)
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
                    schema = self.get_table_schema(cursor, table_name)
                    columns = [col['name'] for col in schema]
                    record = dict(zip(columns, row))
                    print(f"DEBUG: Found record in {table_name} via {col_to_try}: {record_id}")
                    return record
            
            print(f"DEBUG: No record found in {table_name} with ID {record_id} (tried: {id_columns_to_try})")
        except Exception as e:
            print(f"Error getting record {table_name}.{id_column} = {record_id}: {e}")
        
        return None
    
    def _follow_forward_fk(self, cursor, main_record, fk):
        """Follow forward FK relationship"""
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
                schema = self.get_table_schema(cursor, fk['to_table'])
                columns = [col['name'] for col in schema]
                return dict(zip(columns, row))
        
        except Exception as e:
            print(f"Error following forward FK {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}: {e}")
        
        return None
    
    def _follow_reverse_fk(self, cursor, main_record, fk):
        """Follow reverse FK relationship"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Get the primary key value from main record
            pk_value = self._get_primary_key_value(main_record, fk['to_column'])
            if not pk_value:
                return None
            
            # Query for records that reference this main record
            cursor.execute(f"SELECT * FROM {q}{fk['from_table']}{q} WHERE {q}{fk['from_column']}{q} = {p} LIMIT 10", (pk_value,))
            rows = cursor.fetchall()
            if rows:
                # Get column names dynamically
                schema = self.get_table_schema(cursor, fk['from_table'])
                columns = [col['name'] for col in schema]
                return [dict(zip(columns, row)) for row in rows]
        
        except Exception as e:
            print(f"Error following reverse FK {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}: {e}")
        
        return []
    
    def _get_primary_key_value(self, record, pk_column):
        """Get primary key value from record"""
        # Try the exact PK column first
        if pk_column in record:
            return record[pk_column]
        
        # Try common primary key names
        common_pks = ['id', 'ID', f"{pk_column}", f"{pk_column}_id"]
        for pk in common_pks:
            if pk in record and record[pk]:
                return record[pk]
        
        return None
    
    def _build_dynamic_summary(self, profile):
        """Build summary from dynamically discovered relationships"""
        summary_parts = []
        
        main_record = profile.get('main_record', {})
        related_data = profile.get('related_data', {})
        
        # Main record info
        if main_record:
            # Try to find a meaningful identifier
            for field in ['name', 'title', 'sample_id', 'host_id', 'location_id', 'id']:
                if field in main_record and main_record[field]:
                    summary_parts.append(f"• {field.title()}: {main_record[field]}")
                    break
        
        # Related data summary
        for table_key, data in related_data.items():
            if isinstance(data, list):
                summary_parts.append(f"• {table_key.replace('_data', '').title()}: {len(data)} records")
            elif data:
                # Try to find a meaningful field from single record
                for field in ['name', 'title', 'species', 'scientific_name', 'country', 'province']:
                    if field in data and data[field]:
                        summary_parts.append(f"• {table_key.replace('_info', '').title()}: {data[field]}")
                        break
        
        return summary_parts
    
    def print_schema_analysis(self, cursor, table_name):
        """Print detailed schema and FK analysis"""
        print(f"\n=== Dynamic Schema Analysis for {table_name} ===")
        
        # Table schema
        schema = self.get_table_schema(cursor, table_name)
        print(f"Table Schema ({len(schema)} columns):")
        for col in schema:
            pk_marker = " (PK)" if col['pk'] else ""
            print(f"  • {col['name']}: {col['type']}{pk_marker}")
        
        # FK relationships
        relationships = self.get_fk_relationships(cursor, table_name)
        print(f"\nFK Relationships ({len(relationships)}):")
        for i, fk in enumerate(relationships, 1):
            if fk['type'] == 'forward':
                print(f"  {i}. FORWARD: {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}")
            else:
                print(f"  {i}. REVERSE: {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}")
        
        print()

# Example usage
if __name__ == '__main__':
    import sqlite3
    
    # Test with the correct database
    conn = sqlite3.connect('DataExcel/CAN2-With-Referent-Key.db')
    cursor = conn.cursor()
    
    builder = DynamicFKProfileBuilder('sqlite')
    
    # Analyze samples table
    builder.print_schema_analysis(cursor, 'samples')
    
    # Build a dynamic profile with the correct sample_id
    profile = builder.build_dynamic_profile(cursor, 'samples', '28320', 'sample_id')
    
    print("Dynamic Profile Results:")
    print("Main Record:", profile['main_record'].get('sample_id', 'Unknown'))
    print("FK Paths Used:")
    for path in profile['fk_paths_used']:
        print(f"  • {path}")
    print("Summary:")
    for item in profile['summary']:
        print(f"  {item}")
    
    conn.close()
