#!/usr/bin/env python3
"""
Excel Import Table Exclusion Manager
Helps configure which tables should be excluded from automatic Excel import
"""

import sqlite3
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.excel_import import ExcelImportManager

def main():
    """Main function to manage table exclusions"""
    
    # Connect to database
    db_path = "today.db"
    if not os.path.exists(db_path):
        print(f"❌ Database '{db_path}' not found!")
        return
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Create Excel import manager
        excel_manager = ExcelImportManager(conn, "sqlite")
        
        print("🔧 Excel Import Table Exclusion Manager")
        print("=" * 50)
        
        # Show current exclusions
        excel_manager.print_excluded_tables()
        
        # Show available tables
        print("\n📊 Available Tables in Database:")
        print("=" * 50)
        
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        tables = cursor.fetchall()
        
        for (table_name,) in tables:
            status = "🚫 EXCLUDED" if table_name in excel_manager.excluded_tables else "✅ INCLUDED"
            print(f"  {status} {table_name}")
        
        print("=" * 50)
        print(f"Total tables: {len(tables)}")
        print()
        
        # Interactive menu
        while True:
            print("🎯 Options:")
            print("  1. Add table to exclusion list")
            print("  2. Remove table from exclusion list") 
            print("  3. Show excluded tables")
            print("  4. Show available tables for import")
            print("  5. Reset to default exclusions")
            print("  6. Exit")
            
            choice = input("\nEnter your choice (1-6): ").strip()
            
            if choice == "1":
                table_name = input("Enter table name to exclude: ").strip()
                reason = input("Enter reason (optional): ").strip()
                excel_manager.add_excluded_table(table_name, reason)
                excel_manager.print_excluded_tables()
                
            elif choice == "2":
                table_name = input("Enter table name to include: ").strip()
                excel_manager.remove_excluded_table(table_name)
                excel_manager.print_excluded_tables()
                
            elif choice == "3":
                excel_manager.print_excluded_tables()
                
            elif choice == "4":
                excel_manager.show_available_tables()
                
            elif choice == "5":
                # Reset to defaults
                default_exclusions = {
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
                }
                excel_manager.set_excluded_tables(default_exclusions)
                print("✅ Reset to default exclusions")
                excel_manager.print_excluded_tables()
                
            elif choice == "6":
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please try again.")
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
