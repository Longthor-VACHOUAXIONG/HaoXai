#!/usr/bin/env python3
"""
Foreign Key Optimized Sample Profile Builder
Uses dynamic FK relationships instead of hardcoded table lookups
"""

class FKOptimizedProfileBuilder:
    def __init__(self, db_type='sqlite'):
        self.db_type = db_type
        self.fk_cache = {}  # Cache FK relationships for performance
    
    def get_fk_relationships(self, cursor, table_name):
        """Get and cache foreign key relationships for a table"""
        if table_name not in self.fk_cache:
            self.fk_cache[table_name] = self._discover_fk_relationships(cursor, table_name)
        return self.fk_cache[table_name]
    
    def _discover_fk_relationships(self, cursor, table_name):
        """Discover FK relationships dynamically"""
        relationships = []
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
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
    
    def build_fk_optimized_profile(self, cursor, sample_id):
        """Build sample profile using FK relationships instead of hardcoded lookups"""
        profile = {
            'sample_info': {},
            'related_data': {},
            'summary': []
        }
        
        try:
            print(f"DEBUG: Building FK-optimized profile for {sample_id}")
            
            # Step 1: Get the main sample record
            sample_info = self._get_record_by_any_id(cursor, 'samples', sample_id)
            if not sample_info:
                print(f"DEBUG: Sample {sample_id} not found")
                return profile
            
            profile['sample_info'] = sample_info
            print(f"DEBUG: Found sample: {sample_info.get('sample_id', 'Unknown')}")
            
            # Step 2: Discover and follow FK relationships
            fk_relationships = self.get_fk_relationships(cursor, 'samples')
            print(f"DEBUG: Found {len(fk_relationships)} FK relationships for samples table")
            
            # Step 3: Follow forward relationships (samples -> other tables)
            for fk in fk_relationships:
                if fk['type'] == 'forward':
                    related_data = self._follow_forward_fk(cursor, sample_info, fk)
                    if related_data:
                        table_key = f"{fk['to_table']}_info"
                        profile['related_data'][table_key] = related_data
                        print(f"DEBUG: Found {fk['to_table']} info via {fk['from_column']} -> {fk['to_column']}")
            
            # Step 4: Follow reverse relationships (other tables -> samples)
            for fk in fk_relationships:
                if fk['type'] == 'reverse':
                    related_data = self._follow_reverse_fk(cursor, sample_info, fk)
                    if related_data:
                        table_key = f"{fk['from_table']}_data"
                        profile['related_data'][table_key] = related_data
                        print(f"DEBUG: Found {len(related_data)} records in {fk['from_table']} referencing sample")
            
            # Step 5: Build summary
            profile['summary'] = self._build_fk_summary(profile)
            
        except Exception as e:
            print(f"DEBUG: Error building FK profile: {e}")
            import traceback
            traceback.print_exc()
        
        return profile
    
    def _get_record_by_any_id(self, cursor, table_name, sample_id):
        """Get record by trying common ID columns"""
        q = '"' if self.db_type == 'sqlite' else '`'
        p = '?' if self.db_type == 'sqlite' else '%s'
        
        # Common ID columns to try
        id_columns = ['sample_id', 'tissue_id', 'intestine_id', 'plasma_id', 'source_id', 'tested_sample_id']
        
        for col in id_columns:
            try:
                cursor.execute(f"SELECT * FROM {q}{table_name}{q} WHERE {q}{col}{q} = {p} LIMIT 1", (sample_id,))
                row = cursor.fetchone()
                if row:
                    # Get column names
                    if self.db_type == 'sqlite':
                        cursor.execute(f'PRAGMA table_info("{table_name}")')
                        columns = [c[1] for c in cursor.fetchall()]
                    else:
                        cursor.execute(f'DESCRIBE `{table_name}`')
                        columns = [c[0] for c in cursor.fetchall()]
                    
                    return dict(zip(columns, row))
            except:
                continue
        
        return None
    
    def _follow_forward_fk(self, cursor, sample_info, fk):
        """Follow forward FK relationship (samples -> related table)"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Get the FK value from sample record
            fk_value = sample_info.get(fk['from_column'])
            if not fk_value:
                return None
            
            # Query the related table
            cursor.execute(f"SELECT * FROM {q}{fk['to_table']}{q} WHERE {q}{fk['to_column']}{q} = {p} LIMIT 1", (fk_value,))
            row = cursor.fetchone()
            if row:
                # Get column names
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{fk["to_table"]}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{fk["to_table"]}`')
                    columns = [c[0] for c in cursor.fetchall()]
                
                return dict(zip(columns, row))
        
        except Exception as e:
            print(f"DEBUG: Error following forward FK {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}: {e}")
        
        return None
    
    def _follow_reverse_fk(self, cursor, sample_info, fk):
        """Follow reverse FK relationship (related table -> samples)"""
        try:
            q = '"' if self.db_type == 'sqlite' else '`'
            p = '?' if self.db_type == 'sqlite' else '%s'
            
            # Get the sample's primary key
            sample_pk = sample_info.get('sample_id') or sample_info.get('tissue_id') or sample_info.get('source_id')
            if not sample_pk:
                return None
            
            # Query for records that reference this sample
            cursor.execute(f"SELECT * FROM {q}{fk['from_table']}{q} WHERE {q}{fk['from_column']}{q} = {p} LIMIT 10", (sample_pk,))
            rows = cursor.fetchall()
            if rows:
                # Get column names
                if self.db_type == 'sqlite':
                    cursor.execute(f'PRAGMA table_info("{fk["from_table"]}")')
                    columns = [c[1] for c in cursor.fetchall()]
                else:
                    cursor.execute(f'DESCRIBE `{fk["from_table"]}`')
                    columns = [c[0] for c in cursor.fetchall()]
                
                return [dict(zip(columns, row)) for row in rows]
        
        except Exception as e:
            print(f"DEBUG: Error following reverse FK {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}: {e}")
        
        return []
    
    def _build_fk_summary(self, profile):
        """Build summary from FK-discovered relationships"""
        summary_parts = []
        
        sample_info = profile.get('sample_info', {})
        related_data = profile.get('related_data', {})
        
        # Sample info
        if sample_info.get('sample_id'):
            summary_parts.append(f"• Sample: {sample_info['sample_id']}")
        
        # Host info
        host_info = related_data.get('hosts_info', {})
        if host_info and host_info.get('species'):
            summary_parts.append(f"• Host: {host_info['species']}")
        
        # Taxonomy info
        taxonomy_info = related_data.get('taxonomy_info', {})
        if taxonomy_info and taxonomy_info.get('scientific_name'):
            summary_parts.append(f"• Species: {taxonomy_info['scientific_name']}")
        
        # Location info
        location_info = related_data.get('locations_info', {})
        if location_info:
            if location_info.get('province'):
                summary_parts.append(f"• Location: {location_info['province']}")
            elif location_info.get('country'):
                summary_parts.append(f"• Location: {location_info['country']}")
        
        # Screening results
        screening_data = related_data.get('screening_results_data', [])
        if screening_data:
            summary_parts.append(f"• Screening Tests: {len(screening_data)}")
        
        # Sequencing data
        sequencing_data = related_data.get('sequencing_data', [])
        if sequencing_data:
            summary_parts.append(f"• Sequencing Records: {len(sequencing_data)}")
        
        # Storage info
        storage_data = related_data.get('storage_locations_data', [])
        if storage_data:
            summary_parts.append(f"• Storage Records: {len(storage_data)}")
        
        return summary_parts
    
    def print_fk_analysis(self, cursor, table_name):
        """Print detailed FK analysis for debugging"""
        print(f"\n=== FK Analysis for {table_name} ===")
        
        relationships = self.get_fk_relationships(cursor, table_name)
        
        if not relationships:
            print("No FK relationships found")
            return
        
        for i, fk in enumerate(relationships, 1):
            if fk['type'] == 'forward':
                print(f"{i}. FORWARD: {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}")
            else:
                print(f"{i}. REVERSE: {fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}")
        
        print()

# Example usage
if __name__ == '__main__':
    import sqlite3
    
    # Test with your database
    conn = sqlite3.connect('CAN2Database_v2.db')
    cursor = conn.cursor()
    
    builder = FKOptimizedProfileBuilder('sqlite')
    
    # Analyze FK relationships
    builder.print_fk_analysis(cursor, 'samples')
    
    # Build a profile
    profile = builder.build_fk_optimized_profile(cursor, 'CANB_TIS23_L_075')
    print("Profile Summary:")
    for item in profile['summary']:
        print(f"  {item}")
    
    conn.close()
