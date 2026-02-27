#!/usr/bin/env python3
"""
Secure Database Initialization Script for HaoXai
Creates a new database with comprehensive security features
"""

import os
import sys
import sqlite3
from datetime import datetime
import logging

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.secure_init import create_secure_database, migrate_existing_database
from database.security import DatabaseSecurity

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main initialization function"""
    print("=" * 60)
    print("HaoXai Secure Database Initialization")
    print("=" * 60)
    
    # Default database path
    db_path = "CAN2Database_v2 - Copy.db"
    
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    print(f"Database path: {db_path}")
    
    # Check if database exists
    if os.path.exists(db_path):
        print("\nDatabase already exists!")
        choice = input("Do you want to: (1) Create new secure database, (2) Add security to existing, (3) Exit: ")
        
        if choice == "1":
            # Backup existing database
            backup_path = f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            os.rename(db_path, backup_path)
            print(f"Existing database backed up to: {backup_path}")
            
            # Create new secure database
            if create_secure_database(db_path):
                print("\n✅ Secure database created successfully!")
                print(f"Database: {db_path}")
                print("\nDefault admin credentials:")
                print("  Username: admin")
                print("  Password: admin123")
                print("\n🔒 Please change the default admin password immediately!")
            else:
                print("❌ Failed to create secure database")
        
        elif choice == "2":
            # Add security to existing database
            if migrate_existing_database(db_path):
                print("\n✅ Security features added to existing database!")
                print("\nDefault admin credentials:")
                print("  Username: admin")
                print("  Password: admin123")
                print("\n🔒 Please change the default admin password immediately!")
            else:
                print("❌ Failed to migrate database")
        
        else:
            print("Exiting...")
            return
    
    else:
        # Create new secure database
        if create_secure_database(db_path):
            print("\n✅ Secure database created successfully!")
            print(f"Database: {db_path}")
            print("\nDefault admin credentials:")
            print("  Username: admin")
            print("  Password: admin123")
            print("\n🔒 Please change the default admin password immediately!")
        else:
            print("❌ Failed to create secure database")
            return
    
    # Display security features
    print("\n" + "=" * 60)
    print("🔐 Security Features Enabled:")
    print("=" * 60)
    print("✅ User Authentication & Role-Based Access Control")
    print("✅ Data Encryption for Sensitive Fields")
    print("✅ Comprehensive Audit Logging")
    print("✅ Automatic Database Backups")
    print("✅ Row-Level Security Policies")
    print("✅ Schema Protection & Validation")
    print("✅ Failed Login Attempt Tracking")
    print("✅ Session Management")
    print("=" * 60)
    
    # Show database structure
    print("\n📊 Database Structure:")
    print("-" * 40)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"📋 {table_name}: {count} records")
    
    conn.close()
    
    print("\n🚀 Your HaoXai database is now secure and ready for use!")
    print("🌐 Access the application at: http://localhost:5000")
    print("🔐 Admin Dashboard: http://localhost:5000/admin/security")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
