import sqlite3
import os
import sys
sys.path.append('.')

# Import the SmartLocalAI class to use the same methods as Excel upload
from routes.chat import SmartLocalAI

def compare_database_vs_excel():
    """Compare database data with Excel upload results for specific samples"""
    print("=== Comparing Database vs Excel Upload Results ===")
    
    # Test samples
    test_samples = ['CANR_TISL24_033', 'CANR_TISL24_054', 'IPLNAHL_ANA25_006', 'CANB_TIS23_L_075']
    
    # Initialize SmartLocalAI
    from config import Config
    smart_ai = SmartLocalAI(Config)
    
    for sample_id in test_samples:
        print(f"\n{'='*60}")
        print(f"TESTING: {sample_id}")
        print(f"{'='*60}")
        
        # Step 1: Get database data directly
        print(f"\n📊 Step 1: Direct Database Query Results")
        conn = smart_ai.get_connection()
        cursor = conn.cursor()
        
        # Check sample
        cursor.execute('SELECT * FROM samples WHERE tissue_id = ? OR intestine_id = ?', (sample_id, sample_id))
        sample = cursor.fetchone()
        if sample:
            cursor.execute('PRAGMA table_info("samples")')
            columns = [c[1] for c in cursor.fetchall()]
            sample_data = dict(zip(columns, sample))
            print(f"✅ Sample: sample_id={sample_data.get('sample_id')}, host_id={sample_data.get('host_id')}")
        
        # Check screening results
        cursor.execute('SELECT * FROM screening_results WHERE tested_sample_id = ?', (sample_id,))
        screening = cursor.fetchone()
        if screening:
            cursor.execute('PRAGMA table_info("screening_results")')
            columns = [c[1] for c in cursor.fetchall()]
            screening_data = dict(zip(columns, screening))
            print(f"✅ Screening: pan_corona={screening_data.get('pan_corona')}")
        else:
            print(f"❌ No screening results found")
        
        # Check host and taxonomy
        if sample_data.get('host_id'):
            cursor.execute('SELECT * FROM hosts WHERE host_id = ?', (sample_data['host_id'],))
            host = cursor.fetchone()
            if host:
                cursor.execute('PRAGMA table_info("hosts")')
                columns = [c[1] for c in cursor.fetchall()]
                host_data = dict(zip(columns, host))
                
                if host_data.get('taxonomy_id'):
                    cursor.execute('SELECT * FROM taxonomy WHERE taxonomy_id = ?', (host_data['taxonomy_id'],))
                    taxonomy = cursor.fetchone()
                    if taxonomy:
                        cursor.execute('PRAGMA table_info("taxonomy")')
                        columns = [c[1] for c in cursor.fetchall()]
                        taxonomy_data = dict(zip(columns, taxonomy))
                        print(f"✅ Taxonomy: scientific_name={taxonomy_data.get('scientific_name')}")
        
        # Check location
        if sample_data.get('location_id'):
            cursor.execute('SELECT * FROM locations WHERE location_id = ?', (sample_data['location_id'],))
            location = cursor.fetchone()
            if location:
                cursor.execute('PRAGMA table_info("locations")')
                columns = [c[1] for c in cursor.fetchall()]
                location_data = dict(zip(columns, location))
                print(f"✅ Location: province={location_data.get('province')}")
        
        # Check storage
        cursor.execute('SELECT * FROM storage_locations WHERE sample_tube_id = ?', (sample_id,))
        storage = cursor.fetchone()
        if storage:
            cursor.execute('PRAGMA table_info("storage_locations")')
            columns = [c[1] for c in cursor.fetchall()]
            storage_data = dict(zip(columns, storage))
            print(f"✅ Storage: rack_position={storage_data.get('rack_position')}")
        
        conn.close()
        
        # Step 2: Simulate Excel upload process
        print(f"\n🔄 Step 2: Excel Upload Process Simulation")
        
        # Build profile (same as Excel upload)
        conn = smart_ai.get_connection()
        cursor = conn.cursor()
        profile = smart_ai._build_sample_profile(cursor, sample_id)
        conn.close()
        
        print(f"Profile built: {profile['sample_id']}")
        print(f"  - Sample info: {'✓' if profile['sample_info'] else '✗'}")
        print(f"  - Host info: {'✓' if profile['host_info'] else '✗'}")
        print(f"  - Taxonomy info: {'✓' if profile.get('taxonomy_info') else '✗'}")
        print(f"  - Location info: {'✓' if profile['location_info'] else '✗'}")
        print(f"  - Screening results: {len(profile['screening_results'])}")
        print(f"  - Storage info: {'✓' if profile['storage_info'] else '✗'}")
        
        # Step 3: Create available_data dictionary (same as Excel upload)
        available_data = {}
        
        # Add sample data
        if profile['sample_info']:
            for key, value in profile['sample_info'].items():
                if value and key not in ['table_source']:
                    available_data[key] = value
                    available_data[f'sample_{key}'] = value
        
        # Add host data
        if profile['host_info']:
            for key, value in profile['host_info'].items():
                if value and key not in ['table_source']:
                    available_data[key] = value
                    available_data[f'host_{key}'] = value
        
        # Add taxonomy data
        if profile.get('taxonomy_info'):
            for key, value in profile['taxonomy_info'].items():
                if value and key not in ['table_source']:
                    available_data[key] = value
                    available_data[f'taxonomy_{key}'] = value
        
        # Add location data
        if profile['location_info']:
            for key, value in profile['location_info'].items():
                if value and key not in ['table_source']:
                    available_data[key] = value
                    available_data[f'location_{key}'] = value
        
        # Add screening data
        if profile['screening_results']:
            for screening in profile['screening_results']:
                for key, value in screening.items():
                    if value and key not in ['table_source']:
                        available_data[key] = value
                        available_data[f'screening_{key}'] = value
        
        # Add storage data
        if profile['storage_info']:
            for key, value in profile['storage_info'].items():
                if value and key not in ['table_source']:
                    available_data[key] = value
                    available_data[f'storage_{key}'] = value
        
        print(f"\n📋 Step 3: Field Matching Results")
        
        # Test field matching (same as Excel upload)
        excel_columns = ['SampleId', 'pan_corona', 'scientific_name', 'province', 'rack_position']
        
        excel_results = {}
        for excel_col in excel_columns:
            excel_col_original = excel_col
            excel_col_clean = excel_col_original.lower()
            
            found_value = None
            found_source = None
            
            # Priority 1: Exact field name match
            if excel_col_clean in available_data:
                found_value = available_data[excel_col_clean]
                found_source = f"exact: {excel_col_clean}"
            
            # Priority 2: Case-insensitive exact match
            if not found_value:
                for key, value in available_data.items():
                    if key.lower() == excel_col_clean and value:
                        found_value = value
                        found_source = f"case-insensitive: {key}"
                        break
            
            # Priority 3: Special field mappings
            if not found_value:
                if excel_col_clean == 'scientific_name':
                    if 'scientific_name' in available_data:
                        found_value = available_data['scientific_name']
                        found_source = "special: scientific_name"
                    elif 'species' in available_data:
                        found_value = available_data['species']
                        found_source = "special: species (fallback)"
                elif excel_col_clean == 'pan_corona':
                    if 'pan_corona' in available_data:
                        found_value = available_data['pan_corona']
                        found_source = "special: pan_corona"
                elif excel_col_clean == 'province':
                    if 'province' in available_data:
                        found_value = available_data['province']
                        found_source = "special: province"
                elif excel_col_clean == 'rack_position':
                    if 'rack_position' in available_data:
                        found_value = available_data['rack_position']
                        found_source = "special: rack_position"
            
            excel_results[excel_col] = found_value
            
            if found_value:
                print(f"  ✅ {excel_col}: {found_value} ({found_source})")
            else:
                print(f"  ❌ {excel_col}: NOT FOUND")
        
        # Step 4: Compare results
        print(f"\n🔍 Step 4: Comparison Summary")
        print(f"Expected Excel Results for {sample_id}:")
        for col, value in excel_results.items():
            status = "✅" if value else "❌"
            print(f"  {status} {col}: {value if value else 'EMPTY'}")

if __name__ == "__main__":
    compare_database_vs_excel()
