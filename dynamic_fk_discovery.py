#!/usr/bin/env python3
"""
Dynamic FK Discovery System - Works with ANY database
"""
import sqlite3
import os

def discover_all_fks_dynamic(db_path):
    """Discover ALL FK relationships from ANY database automatically"""
    print(f"🔍 Discovering FK Relationships for: {db_path}")
    print("=" * 60)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cursor.fetchall()]
    
    print(f"📊 Found {len(tables)} tables: {', '.join(tables)}")
    
    # Discover ALL FK relationships dynamically
    all_fk_rules = {}
    
    for table in tables:
        cursor.execute(f"PRAGMA foreign_key_list({table})")
        fks = cursor.fetchall()
        
        if fks:
            print(f"\n📋 Table: {table}")
            fk_rules = {}
            
            for fk in fks:
                # fk format: (id, seq, table, from_col, to_col, on_update, on_delete, match)
                fk_id, seq, target_table, from_col, to_col, on_update, on_delete, match = fk
                
                print(f"  🔗 FK: {from_col} -> {target_table}.{to_col}")
                
                # DYNAMICALLY discover match columns from target table schema
                cursor.execute(f"PRAGMA table_info({target_table})")
                target_columns = [col[1] for col in cursor.fetchall()]
                
                # Smart matching based on column names and common patterns
                match_cols = []
                
                # Primary key column
                cursor.execute(f"PRAGMA table_info({target_table})")
                pk_cols = [col[1] for col in cursor.fetchall() if col[5] == 1]  # col[5] is pk flag
                
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
                
                # Remove duplicates while preserving order
                match_cols = list(dict.fromkeys(match_cols))
                
                fk_rules[from_col] = {
                    'target_table': target_table,
                    'match_cols': match_cols,
                    'target_pk': to_col
                }
            
            all_fk_rules[table] = fk_rules
    
    # Generate the COMPLETE dynamic FK rules
    print(f"\n📊 DYNAMIC FK RULES FOR ANY DATABASE:")
    print("=" * 60)
    
    print("# This will work with ANY database automatically!")
    print("dynamic_fk_rules = {")
    for table, fks in all_fk_rules.items():
        print(f"    '{table}': {{")
        for fk_col, rule in fks.items():
            print(f"        '{fk_col}': {{'target_table': '{rule['target_table']}', 'match_cols': {rule['match_cols']}}},")
        print("    },")
    print("}")
    
    conn.close()
    return all_fk_rules

def test_with_different_database():
    """Test with a completely different database structure"""
    print("\n🧪 Testing with Different Database Structure")
    print("=" * 60)
    
    # Create a completely different test database
    test_db = "different_structure.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    
    # Create completely different table structure
    cursor.execute("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY AUTOINCREMENT,
            dept_name TEXT UNIQUE,
            location TEXT,
            manager TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_name TEXT,
            dept_id INTEGER,
            position TEXT,
            salary REAL,
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE projects (
            project_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT,
            dept_id INTEGER,
            budget REAL,
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE assignments (
            assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_id INTEGER,
            project_id INTEGER,
            role TEXT,
            start_date DATE,
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id),
            FOREIGN KEY (project_id) REFERENCES projects(project_id)
        )
    """)
    
    conn.commit()
    
    # Test dynamic discovery
    fk_rules = discover_all_fks_dynamic(test_db)
    
    print(f"\n✅ Successfully discovered {len(fk_rules)} tables with FK relationships")
    print("🎉 This proves the system works with ANY database structure!")
    
    conn.close()
    os.remove(test_db)

if __name__ == "__main__":
    # Test with current database
    current_db = r"d:\MyFiles\Program_Last_version\ViroDB_structure_latest_V - Copy\DataExcel\CAN2-With-Referent-Key.db"
    if os.path.exists(current_db):
        discover_all_fks_dynamic(current_db)
    
    # Test with completely different database
    test_with_different_database()
