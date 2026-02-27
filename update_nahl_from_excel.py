import pandas as pd
import sqlite3
import os

def update_nahl_from_excel():
    """Update NAHL host taxonomy from Excel file"""
    print("=== Updating NAHL Host Taxonomy from Excel ===")
    
    # Connect to database
    db_path = os.path.join('DataExcel', 'CAN2-With-Referent-Key.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Read Excel file
        excel_path = os.path.join('DataExcel', 'nahl_host.xlsx')
        df = pd.read_excel(excel_path)
        print(f"Read {len(df)} rows from nahl_host.xlsx")
        
        updated_count = 0
        
        for index, row in df.iterrows():
            sample_id = row['SampleId']
            scientific_name = row['ScientificName']
            
            # Skip if no scientific name
            if pd.isna(scientific_name) or not scientific_name:
                continue
            
            # Find the host record
            cursor.execute('SELECT host_id, taxonomy_id FROM hosts WHERE field_id = ?', (sample_id,))
            host = cursor.fetchone()
            
            if host:
                host_id, current_taxonomy_id = host
                
                # Find taxonomy_id for this scientific name
                cursor.execute('SELECT taxonomy_id FROM taxonomy WHERE scientific_name = ?', (scientific_name,))
                taxonomy_result = cursor.fetchone()
                
                if taxonomy_result:
                    new_taxonomy_id = taxonomy_result[0]
                    
                    # Update host with correct taxonomy
                    if new_taxonomy_id != current_taxonomy_id:
                        cursor.execute('UPDATE hosts SET taxonomy_id = ? WHERE host_id = ?', (new_taxonomy_id, host_id))
                        updated_count += 1
                        print(f"✅ Updated {sample_id}: {scientific_name} (taxonomy_id={new_taxonomy_id})")
                    else:
                        print(f"✓ {sample_id}: Already has correct taxonomy")
                else:
                    print(f"❌ {sample_id}: Scientific name '{scientific_name}' not found in taxonomy table")
            else:
                print(f"❌ {sample_id}: Host not found")
        
        print(f"\n✅ Successfully updated {updated_count} NAHL hosts with correct taxonomy")
        conn.commit()
        
        # Verify the updates
        print(f"\n🔍 Verification:")
        test_samples = ['IPLNAHL_ANA23_054', 'IPLNAHL_ANA25_006', 'IPLNAHL_ANA23_001']
        
        for sample_id in test_samples:
            cursor.execute('SELECT h.host_id, h.taxonomy_id, t.scientific_name FROM hosts h LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id WHERE h.field_id = ?', (sample_id,))
            result = cursor.fetchone()
            if result:
                host_id, taxonomy_id, sci_name = result
                print(f"  {sample_id}: {sci_name if sci_name else 'No taxonomy'} (taxonomy_id={taxonomy_id})")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    update_nahl_from_excel()
