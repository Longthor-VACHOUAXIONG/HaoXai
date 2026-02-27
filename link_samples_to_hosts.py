import sqlite3
import os
import pandas as pd

def link_samples_to_hosts():
    """Link samples to appropriate hosts based on source_id and field_id"""
    print("=== Linking Samples to Hosts ===")
    
    # Connect to database
    db_path = os.path.join(os.path.dirname(__file__), 'DataExcel', 'CAN2-With-Referent-Key.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get samples that need host linking
        cursor.execute("SELECT sample_id, tissue_id, intestine_id, source_id FROM samples WHERE host_id IS NULL")
        samples = cursor.fetchall()
        print(f"Found {len(samples)} samples without host_id")
        
        cursor.execute('PRAGMA table_info("samples")')
        sample_columns = [c[1] for c in cursor.fetchall()]
        
        linked_count = 0
        
        for sample in samples:
            sample_data = dict(zip(sample_columns, sample))
            sample_id = sample_data['sample_id']
            tissue_id = sample_data.get('tissue_id')
            intestine_id = sample_data.get('intestine_id')
            sample_source_id = sample_data.get('source_id')
            
            print(f"\nProcessing sample {sample_id}:")
            print(f"  tissue_id: {tissue_id}")
            print(f"  intestine_id: {intestine_id}")
            print(f"  source_id: {sample_source_id}")
            
            # Try to find matching host by source_id
            host_id = None
            
            if sample_source_id:
                cursor.execute("SELECT host_id, taxonomy_id FROM hosts WHERE source_id = ?", (sample_source_id,))
                host_result = cursor.fetchone()
                if host_result:
                    host_id, taxonomy_id = host_result
                    print(f"  ✅ Found host by source_id: host_id={host_id}, taxonomy_id={taxonomy_id}")
            
            # If not found by source_id, try to find by field_id (for NAHL samples)
            if not host_id:
                # Check if this is a field_id sample like IPLNAHL_ANA25_006
                field_id_candidates = [tissue_id, intestine_id]
                for field_id in field_id_candidates:
                    if field_id and field_id.startswith('IPLNAHL_'):
                        cursor.execute("SELECT host_id, taxonomy_id FROM hosts WHERE field_id = ?", (field_id,))
                        host_result = cursor.fetchone()
                        if host_result:
                            host_id, taxonomy_id = host_result
                            print(f"  ✅ Found host by field_id {field_id}: host_id={host_id}, taxonomy_id={taxonomy_id}")
                            break
            
            # If still not found, try to find rodent host by matching source_id patterns
            if not host_id and sample_source_id:
                # Try to find rodent host with similar source_id
                cursor.execute("SELECT host_id, taxonomy_id FROM hosts WHERE host_type = 'Rodent' LIMIT 10")
                rodent_hosts = cursor.fetchall()
                print(f"  📊 Available rodent hosts: {len(rodent_hosts)}")
                for rh_host_id, rh_taxonomy_id in rodent_hosts[:3]:
                    if rh_taxonomy_id:
                        cursor.execute("SELECT scientific_name FROM taxonomy WHERE taxonomy_id = ?", (rh_taxonomy_id,))
                        sci_name = cursor.fetchone()
                        print(f"    - {rh_host_id}: {sci_name[0] if sci_name else 'Unknown'}")
            
            # Update sample with host_id
            if host_id:
                cursor.execute("UPDATE samples SET host_id = ? WHERE sample_id = ?", (host_id, sample_id))
                linked_count += 1
                print(f"  🔗 Linked sample {sample_id} to host {host_id}")
            else:
                print(f"  ❌ No host found for sample {sample_id}")
        
        print(f"\n✅ Successfully linked {linked_count} samples to hosts")
        conn.commit()
        
    except Exception as e:
        print(f"Error linking samples to hosts: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    link_samples_to_hosts()
