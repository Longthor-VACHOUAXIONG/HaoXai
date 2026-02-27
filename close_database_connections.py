import sqlite3
import os
import time

def close_database_connections():
    """Close all database connections and clean up"""
    print("=== Closing Database Connections ===")
    
    # Connect to database to check and close any active connections
    db_path = os.path.join('DataExcel', 'CAN2-With-Referent-Key.db')
    
    try:
        # Check if database file exists
        if os.path.exists(db_path):
            print(f"Database file found: {db_path}")
            
            # Try to connect and check for any locks
            conn = sqlite3.connect(db_path, timeout=5)
            cursor = conn.cursor()
            
            # Check database status
            cursor.execute("PRAGMA database_list")
            databases = cursor.fetchall()
            print(f"Database status: {databases}")
            
            # Check for any WAL files (write-ahead logging)
            wal_path = db_path + "-wal"
            shm_path = db_path + "-shm"
            
            if os.path.exists(wal_path):
                print(f"WAL file exists: {wal_path}")
                # Try to close WAL
                cursor.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                print("WAL checkpoint executed")
            
            # Close connection
            conn.close()
            print("Database connection closed")
            
            # Wait a moment for cleanup
            time.sleep(1)
            
            # Check if WAL files still exist
            if os.path.exists(wal_path):
                print(f"WAL file still exists, attempting to remove...")
                try:
                    os.remove(wal_path)
                    print("WAL file removed")
                except Exception as e:
                    print(f"Could not remove WAL file: {e}")
            
            if os.path.exists(shm_path):
                print(f"SHM file exists: {shm_path}")
                try:
                    os.remove(shm_path)
                    print("SHM file removed")
                except Exception as e:
                    print(f"Could not remove SHM file: {e}")
            
            print("✅ Database cleanup complete")
        else:
            print("❌ Database file not found")
            
    except Exception as e:
        print(f"❌ Error during database cleanup: {e}")
    
    # Check for any remaining Python processes
    print("\n=== Checking for Running Processes ===")
    try:
        import subprocess
        result = subprocess.run(['tasklist'], capture_output=True, text=True)
        python_processes = [line for line in result.stdout.split('\n') if 'python.exe' in line.lower()]
        
        if python_processes:
            print("Found Python processes:")
            for process in python_processes:
                print(f"  {process}")
        else:
            print("No Python processes found")
            
    except Exception as e:
        print(f"Could not check processes: {e}")
    
    print("\n=== Cleanup Complete ===")
    print("You can now restart the application.")

if __name__ == "__main__":
    close_database_connections()
