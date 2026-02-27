#!/usr/bin/env python3
"""
Initialize security tables for today.db
"""

import os
import sys

# Add current directory to path
sys.path.append('.')

try:
    from database.security import DatabaseSecurity
    
    db_path = "today.db"
    
    if os.path.exists(db_path):
        print(f"🔧 Initializing security tables for: {db_path}")
        
        # This should create all security tables
        security = DatabaseSecurity(db_path)
        
        # Verify tables were created
        cursor = security.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'security_%'")
        tables = cursor.fetchall()
        
        print(f"✅ Security tables created: {[t[0] for t in tables]}")
        
        # Check if backup log table exists and is empty
        cursor.execute("SELECT COUNT(*) FROM security_backup_log")
        backup_count = cursor.fetchone()[0]
        print(f"📝 Backup log entries: {backup_count}")
        
        # Check if audit log table exists and is empty
        cursor.execute("SELECT COUNT(*) FROM security_audit_log")
        audit_count = cursor.fetchone()[0]
        print(f"📋 Audit log entries: {audit_count}")
        
        print("\n🎉 Security tables initialized successfully!")
        print("📌 Quick Stats should now work correctly.")
        
    else:
        print(f"❌ Database not found: {db_path}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
