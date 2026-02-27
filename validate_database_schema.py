#!/usr/bin/env python3
"""
Database Schema Validator and Fixer
Ensures all required columns exist in sequence database tables
"""

import sqlite3
import sys
import os
from database.db_manager import DatabaseManager

def validate_and_fix_database(db_path):
    """Validate database schema and fix any missing columns"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"🔍 Validating database: {db_path}")
        
        # Define expected schema for sequences table
        expected_sequences_columns = {
            'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
            'filename': 'TEXT NOT NULL',
            'upload_date': 'DATETIME DEFAULT CURRENT_TIMESTAMP',
            'file_hash': 'TEXT UNIQUE',
            'sequence': 'TEXT NOT NULL',
            'sequence_length': 'INTEGER NOT NULL',
            'group_name': 'TEXT',
            'detected_direction': "TEXT CHECK(detected_direction IN ('Forward', 'Reverse', 'Unknown')) DEFAULT 'Unknown'",
            'quality_score': 'REAL',
            'avg_quality': 'REAL',
            'min_quality': 'REAL',
            'max_quality': 'REAL',
            'overall_grade': "TEXT CHECK(overall_grade IN ('Excellent', 'Good', 'Acceptable', 'Poor', 'Needs Work', 'Unknown')) DEFAULT 'Unknown'",
            'grade_score': 'INTEGER',
            'issues': 'TEXT',
            'likely_swapped': 'INTEGER DEFAULT 0',
            'direction_mismatch': 'INTEGER DEFAULT 0',
            'complementarity_score': 'REAL',
            'ambiguity_count': 'INTEGER',
            'ambiguity_percent': 'REAL',
            'virus_type': 'TEXT',
            'reference_used': 'INTEGER DEFAULT 0',
            'processing_method': 'TEXT',
            'sample_id': 'TEXT',
            'target_sequence': 'TEXT',
            'uploaded_by': 'TEXT',
            'project_name': 'TEXT',
            'notes': 'TEXT'
        }
        
        # Define expected schema for consensus_sequences table
        expected_consensus_columns = {
            'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
            'consensus_name': 'TEXT NOT NULL',
            'created_date': 'DATETIME DEFAULT CURRENT_TIMESTAMP',
            'consensus_sequence': 'TEXT NOT NULL',
            'original_length': 'INTEGER NOT NULL',
            'trimmed_length': 'INTEGER NOT NULL',
            'group_name': 'TEXT',
            'file_count': 'INTEGER DEFAULT 1',
            'source_file_ids': 'TEXT',
            'sample_id': 'TEXT',
            'target_sequence': 'TEXT',
            'virus_type': 'TEXT',
            'trim_method': 'TEXT',
            'quality_threshold': 'REAL',
            'uploaded_by': 'TEXT',
            'project_name': 'TEXT',
            'notes': 'TEXT'
        }
        
        # Define expected schema for projects table
        expected_projects_columns = {
            'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
            'project_name': 'TEXT NOT NULL UNIQUE',
            'description': 'TEXT',
            'created_by': 'TEXT',
            'created_date': 'DATETIME DEFAULT CURRENT_TIMESTAMP',
            'updated_date': 'DATETIME DEFAULT CURRENT_TIMESTAMP'
        }
        
        # Check and fix sequences table
        cursor.execute("PRAGMA table_info(sequences)")
        existing_columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        missing_columns = []
        for col_name, col_def in expected_sequences_columns.items():
            if col_name not in existing_columns:
                missing_columns.append((col_name, col_def))
                print(f"❌ Missing column: {col_name}")
        
        if missing_columns:
            print(f"🔧 Adding {len(missing_columns)} missing columns to sequences table...")
            for col_name, col_def in missing_columns:
                try:
                    cursor.execute(f"ALTER TABLE sequences ADD COLUMN {col_def}")
                    print(f"✅ Added column: {col_name}")
                except Exception as e:
                    print(f"❌ Failed to add column {col_name}: {e}")
        else:
            print("✅ All sequences table columns exist")
        
        # Check and fix consensus_sequences table
        cursor.execute("PRAGMA table_info(consensus_sequences)")
        existing_columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        missing_columns = []
        for col_name, col_def in expected_consensus_columns.items():
            if col_name not in existing_columns:
                missing_columns.append((col_name, col_def))
                print(f"❌ Missing column: {col_name}")
        
        if missing_columns:
            print(f"🔧 Adding {len(missing_columns)} missing columns to consensus_sequences table...")
            for col_name, col_def in missing_columns:
                try:
                    cursor.execute(f"ALTER TABLE consensus_sequences ADD COLUMN {col_def}")
                    print(f"✅ Added column: {col_name}")
                except Exception as e:
                    print(f"❌ Failed to add column {col_name}: {e}")
        else:
            print("✅ All consensus_sequences table columns exist")
        
        # Check and fix projects table
        cursor.execute("PRAGMA table_info(projects)")
        existing_columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        missing_columns = []
        for col_name, col_def in expected_projects_columns.items():
            if col_name not in existing_columns:
                missing_columns.append((col_name, col_def))
                print(f"❌ Missing column: {col_name}")
        
        if missing_columns:
            print(f"🔧 Adding {len(missing_columns)} missing columns to projects table...")
            for col_name, col_def in missing_columns:
                try:
                    cursor.execute(f"ALTER TABLE projects ADD COLUMN {col_def}")
                    print(f"✅ Added column: {col_name}")
                except Exception as e:
                    print(f"❌ Failed to add column {col_name}: {e}")
        else:
            print("✅ All projects table columns exist")
        
        # Commit all changes
        conn.commit()
        print("🎉 Database schema validation and fix completed!")
        
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        return False
    finally:
        if conn:
            conn.close()
    
    return True

def create_fresh_database_with_schema(db_path):
    """Create a fresh database with complete schema"""
    try:
        # Remove existing database to start fresh
        if os.path.exists(db_path):
            os.remove(db_path)
            print(f"🗑️ Removed existing database: {db_path}")
        
        # Create new database with complete schema
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"🏗️ Creating fresh database with complete schema: {db_path}")
        
        # Create sequences table with all required columns
        cursor.execute('''
            CREATE TABLE sequences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                file_hash TEXT UNIQUE,
                
                sequence TEXT NOT NULL,
                sequence_length INTEGER NOT NULL,
                
                group_name TEXT,
                detected_direction TEXT CHECK(detected_direction IN ('Forward', 'Reverse', 'Unknown')) DEFAULT 'Unknown',
                
                quality_score REAL,
                avg_quality REAL,
                min_quality REAL,
                max_quality REAL,
                
                overall_grade TEXT CHECK(overall_grade IN ('Excellent', 'Good', 'Acceptable', 'Poor', 'Needs Work', 'Unknown')) DEFAULT 'Unknown',
                grade_score INTEGER,
                
                issues TEXT,
                likely_swapped INTEGER DEFAULT 0,
                direction_mismatch INTEGER DEFAULT 0,
                complementarity_score REAL,
                ambiguity_count INTEGER,
                ambiguity_percent REAL,
                
                virus_type TEXT,
                reference_used INTEGER DEFAULT 0,
                processing_method TEXT,
                
                sample_id TEXT,
                target_sequence TEXT,
                
                uploaded_by TEXT,
                project_name TEXT,
                notes TEXT
            )
        ''')
        
        # Create consensus_sequences table with all required columns
        cursor.execute('''
            CREATE TABLE consensus_sequences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consensus_name TEXT NOT NULL,
                created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                consensus_sequence TEXT NOT NULL,
                original_length INTEGER NOT NULL,
                trimmed_length INTEGER NOT NULL,
                
                group_name TEXT,
                file_count INTEGER DEFAULT 1,
                source_file_ids TEXT,
                
                sample_id TEXT,
                target_sequence TEXT,
                
                virus_type TEXT,
                trim_method TEXT,
                quality_threshold REAL,
                
                uploaded_by TEXT,
                project_name TEXT,
                notes TEXT
            )
        ''')
        
        # Create projects table with all required columns
        cursor.execute('''
            CREATE TABLE projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_name TEXT NOT NULL UNIQUE,
                description TEXT,
                created_by TEXT,
                created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        print("✅ Fresh database created with complete schema!")
        
    except Exception as e:
        print(f"❌ Error creating fresh database: {e}")
        return False
    finally:
        if conn:
            conn.close()
    
    return True

def main():
    """Main function"""
    print("🚀 Database Schema Validator and Fixer")
    print("=" * 50)
    
    db_path = "today.db"
    
    if not os.path.exists(db_path):
        print("📁 Database doesn't exist. Creating fresh database with complete schema...")
        success = create_fresh_database_with_schema(db_path)
        if success:
            print("🎉 Fresh database created successfully!")
            print("📋 The database now has all required columns from the start.")
        else:
            print("❌ Failed to create fresh database.")
    else:
        print("🔍 Database exists. Validating and fixing schema...")
        success = validate_and_fix_database(db_path)
        if success:
            print("🎉 Database schema validation completed!")
            print("📋 All required columns are now present.")
        else:
            print("❌ Database schema validation failed.")
    
    return success

if __name__ == "__main__":
    main()
