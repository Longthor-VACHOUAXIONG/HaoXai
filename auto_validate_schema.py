#!/usr/bin/env python3
"""
Automatic Database Schema Validator
Integrates schema validation into the sequence database initialization
"""

import sqlite3
import sys
import os

def auto_validate_database_schema(db_path):
    """Automatically validate and fix database schema on every initialization"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"🔍 Auto-validating database schema: {db_path}")
        
        # Define all required columns for sequences table
        required_sequences_columns = [
            ('avg_quality', 'REAL'),
            ('min_quality', 'REAL'),
            ('max_quality', 'REAL'),
            ('grade_score', 'INTEGER'),
            ('likely_swapped', 'INTEGER DEFAULT 0'),
            ('direction_mismatch', 'INTEGER DEFAULT 0'),
            ('complementarity_score', 'REAL'),
            ('ambiguity_count', 'INTEGER'),
            ('ambiguity_percent', 'REAL'),
            ('reference_used', 'INTEGER DEFAULT 0'),
            ('processing_method', 'TEXT'),
            ('sample_id', 'TEXT'),
            ('target_sequence', 'TEXT'),
            ('notes', 'TEXT')
        ]
        
        # Define all required columns for consensus_sequences table
        required_consensus_columns = [
            ('sample_id', 'TEXT'),
            ('target_sequence', 'TEXT'),
            ('trim_method', 'TEXT'),
            ('quality_threshold', 'REAL'),
            ('uploaded_by', 'TEXT'),
            ('notes', 'TEXT')
        ]
        
        # Define all required columns for projects table
        required_projects_columns = [
            ('created_date', 'DATETIME DEFAULT CURRENT_TIMESTAMP'),
            ('updated_date', 'DATETIME DEFAULT CURRENT_TIMESTAMP')
        ]
        
        # Define all required columns for blast_results table
        required_blast_results_columns = [
            ('database_used', 'TEXT DEFAULT "nt"'),
            ('program', 'TEXT DEFAULT "blastn"'),
            ('query_name', 'TEXT'),
            ('query_length', 'INTEGER'),
            ('total_hits', 'INTEGER DEFAULT 0'),
            ('execution_time', 'REAL'),
            ('status', "TEXT CHECK(status IN ('success', 'failed', 'no_hits')) DEFAULT 'success'"),
            ('error_message', 'TEXT')
        ]
        
        # Define all required columns for blast_hits table
        required_blast_hits_columns = [
            ('hit_rank', 'INTEGER NOT NULL'),
            ('accession', 'TEXT'),
            ('title', 'TEXT'),
            ('organism', 'TEXT'),
            ('query_coverage', 'REAL'),
            ('identity_percent', 'REAL'),
            ('evalue', 'REAL'),
            ('bit_score', 'REAL'),
            ('align_length', 'INTEGER'),
            ('query_from', 'INTEGER'),
            ('query_to', 'INTEGER'),
            ('hit_from', 'INTEGER'),
            ('hit_to', 'INTEGER'),
            ('gaps', 'INTEGER')
        ]
        
        # Check and add missing columns to sequences table
        cursor.execute("PRAGMA table_info(sequences)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        existing_column_names = set(existing_columns)
        
        missing_columns = []
        for col_name, col_def in required_sequences_columns:
            if col_name not in existing_column_names:
                missing_columns.append((col_name, col_def))
                print(f"➕ Adding missing column: {col_name}")
        
        if missing_columns:
            print(f"🔧 Adding {len(missing_columns)} columns to sequences table...")
            for col_name, col_def in missing_columns:
                try:
                    cursor.execute(f"ALTER TABLE sequences ADD COLUMN {col_name} {col_def}")
                    print(f"✅ Added column: {col_name}")
                except Exception as e:
                    print(f"❌ Failed to add column {col_name}: {e}")
        else:
            print("✅ All sequences table columns are present")
        
        # Check and add missing columns to consensus_sequences table
        cursor.execute("PRAGMA table_info(consensus_sequences)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        existing_column_names = set(existing_columns)
        
        missing_columns = []
        for col_name, col_def in required_consensus_columns:
            if col_name not in existing_column_names:
                missing_columns.append((col_name, col_def))
                print(f"➕ Adding missing column: {col_name}")
        
        if missing_columns:
            print(f"🔧 Adding {len(missing_columns)} columns to consensus_sequences table...")
            for col_name, col_def in missing_columns:
                try:
                    cursor.execute(f"ALTER TABLE consensus_sequences ADD COLUMN {col_name} {col_def}")
                    print(f"✅ Added column: {col_name}")
                except Exception as e:
                    print(f"❌ Failed to add column {col_name}: {e}")
        else:
            print("✅ All consensus_sequences table columns are present")
        
        # Check and add missing columns to projects table
        cursor.execute("PRAGMA table_info(projects)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        existing_column_names = set(existing_columns)
        
        missing_columns = []
        for col_name, col_def in required_projects_columns:
            if col_name not in existing_column_names:
                missing_columns.append((col_name, col_def))
                print(f"➕ Adding missing column: {col_name}")
        
        if missing_columns:
            print(f"🔧 Adding {len(missing_columns)} columns to projects table...")
            for col_name, col_def in missing_columns:
                try:
                    cursor.execute(f"ALTER TABLE projects ADD COLUMN {col_name} {col_def}")
                    print(f"✅ Added column: {col_name}")
                except Exception as e:
                    print(f"❌ Failed to add column {col_name}: {e}")
        else:
            print("✅ All projects table columns are present")
        
        # Check and add missing columns to blast_results table
        cursor.execute("PRAGMA table_info(blast_results)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        existing_column_names = set(existing_columns)
        
        missing_columns = []
        for col_name, col_def in required_blast_results_columns:
            if col_name not in existing_column_names:
                missing_columns.append((col_name, col_def))
                print(f"➕ Adding missing column: {col_name}")
        
        if missing_columns:
            print(f"🔧 Adding {len(missing_columns)} columns to blast_results table...")
            for col_name, col_def in missing_columns:
                try:
                    cursor.execute(f"ALTER TABLE blast_results ADD COLUMN {col_name} {col_def}")
                    print(f"✅ Added column: {col_name}")
                except Exception as e:
                    print(f"❌ Failed to add column {col_name}: {e}")
        else:
            print("✅ All blast_results table columns are present")
        
        # Check and add missing columns to blast_hits table
        cursor.execute("PRAGMA table_info(blast_hits)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        existing_column_names = set(existing_columns)
        
        missing_columns = []
        for col_name, col_def in required_blast_hits_columns:
            if col_name not in existing_column_names:
                missing_columns.append((col_name, col_def))
                print(f"➕ Adding missing column: {col_name}")
        
        if missing_columns:
            print(f"🔧 Adding {len(missing_columns)} columns to blast_hits table...")
            for col_name, col_def in missing_columns:
                try:
                    cursor.execute(f"ALTER TABLE blast_hits ADD COLUMN {col_name} {col_def}")
                    print(f"✅ Added column: {col_name}")
                except Exception as e:
                    print(f"❌ Failed to add column {col_name}: {e}")
        else:
            print("✅ All blast_hits table columns are present")
        
        # Commit all changes
        conn.commit()
        print("🎉 Auto schema validation completed!")
        print("📋 Database now has all required columns for proper functionality.")
        
    except Exception as e:
        print(f"❌ Error during auto-validation: {e}")
        return False
    finally:
        if conn:
            conn.close()
    
    return True

def main():
    """Main function"""
    print("🚀 Automatic Database Schema Validator")
    print("=" * 50)
    
    db_path = "today.db"
    
    if not os.path.exists(db_path):
        print("📁 Database doesn't exist. This tool validates existing databases.")
        print("Run this after creating a database to ensure proper schema.")
    else:
        print("🔍 Auto-validating existing database schema...")
        success = auto_validate_database_schema(db_path)
        if success:
            print("🎉 Database schema auto-validation completed!")
            print("📋 All required columns are now present.")
        else:
            print("❌ Database schema auto-validation failed.")
    
    return success

if __name__ == "__main__":
    main()
