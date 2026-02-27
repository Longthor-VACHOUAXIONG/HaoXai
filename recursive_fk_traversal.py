#!/usr/bin/env python3
"""
Recursive Foreign Key Traversal System
Follows FK chains across multiple tables to discover complete data relationships
"""

class RecursiveFKTraversal:
    def __init__(self, db_type='sqlite'):
        self.db_type = db_type
        self.visited_tables = set()
        self.max_depth = 5  # Prevent infinite loops
        self.discovered_paths = []
    
    def build_recursive_profile(self, cursor, table_name, record_id, id_column='id'):
        """Build profile using recursive FK traversal across multiple tables"""
        profile = {
            'main_record': {},
            'discovered_data': {},
            'fk_paths': [],
            'summary': []
        }
        
        try:
            print(f"DEBUG: Building recursive FK profile for {table_name}.{id_column} = {record_id}")
            
            # Start recursive traversal
            self.visited_tables.clear()
            self.discovered_paths.clear()
            
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
            fk_relationships = self._get_fk_relationships(table_name, cursor)
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
                                recursive_result = self._recursive_traverse(
                                    cursor, 
                                    fk['to_table'] if fk['type'] == 'forward' else fk['from_table'], 
                                    related_id, 
                                    'id', 
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
                    cursor.execute(f'PRAGMA table_info("{fk["to_table"] if fk['type'] == 'forward' else fk["from_table"]}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{fk["to_table"] if fk['type'] == 'forward' else fk["from_table"]}`')
                    columns = [c[0] for c in cursor.fetchall()]
                
                return [dict(zip(columns, row)) for row in rows]
            
            return []
            
        except Exception as e:
            print(f"DEBUG: Error following FK {fk['from_table']}.{fk['from_column']} → {fk['to_table']}.{fk['to_column']}: {e}")
            return []
    
    def _get_record_by_id(self, cursor, table_name, id_column, record_id):
        """Get record by ID with multiple column attempts"""
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
    
    def _get_fk_relationships(self, table_name, cursor):
        """Get all FK relationships for a table"""
        relationships = []
        try:
            if self.db_type == 'sqlite':
                # Forward relationships
                cursor.execute(f'PRAGMA foreign_key_list("{table_name}")')
                forward_fks = cursor.fetchall()
                for fk in forward_fks:
                    relationships.append({
                        'type': 'forward',
                        'from_table': table_name,
                        'to_table': fk[2],
                        'from_column': fk[3],
                        'to_column': fk[4]
                    })
                
                # Reverse relationships
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                all_tables = [t[0] for t in cursor.fetchall()]
                
                for other_table in all_tables:
                    if other_table != table_name and other_table.lower() not in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin']:
                        cursor.execute(f'PRAGMA foreign_key_list("{other_table}")')
                        other_fks = cursor.fetchall()
                        for fk in other_fks:
                            if fk[2] == table_name:
                                relationships.append({
                                    'type': 'reverse',
                                    'from_table': other_table,
                                    'to_table': table_name,
                                    'from_column': fk[3],
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
                        'to_table': row[0],
                        'from_column': row[1],
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
                        'to_table': table_name,
                        'from_column': row[1],
                        'to_column': row[2]
                    })
                    
        except Exception as e:
            print(f"Error getting FKs for {table_name}: {e}")
        
        return relationships
    
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

# Example usage
if __name__ == '__main__':
    import sqlite3
    
    # Test with your database
    conn = sqlite3.connect('DataExcel/CAN2-With-Referent-Key.db')
    cursor = conn.cursor()
    
    traversal = RecursiveFKTraversal('sqlite')
    
    # Build recursive profile starting from screening_results
    profile = traversal.build_recursive_profile(cursor, 'screening_results', '2897', 'sample_id')
    
    print("\n=== Recursive FK Profile Results ===")
    print("Main Record:", profile['main_record'].get('sample_id', 'Unknown'))
    print("\nFK Paths Discovered:")
    for path in profile['fk_paths']:
        print(f"  {path}")
    
    print("\nDiscovered Data:")
    for table_key, data in profile['discovered_data'].items():
        if isinstance(data, list):
            print(f"  {table_key}: {len(data)} records")
        else:
            print(f"  {table_key}: 1 record")
    
    print("\nSummary:")
    for item in profile['summary']:
        print(f"  {item}")
    
    conn.close()
