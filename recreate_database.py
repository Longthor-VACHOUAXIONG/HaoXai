import sqlite3
import os

def recreate_database():
    """Recreate database with correct structure for Excel upload system"""
    print("=== Recreating Database with Correct Structure ===")
    
    db_path = os.path.join('DataExcel', 'CAN2-With-Referent-Key.db')
    
    # Remove existing database
    if os.path.exists(db_path):
        os.remove(db_path)
        print("Removed existing database")
    
    # Create new database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables with correct structure
    print("Creating tables...")
    
    # Locations table
    cursor.execute('''
        CREATE TABLE locations (
            location_id INTEGER PRIMARY KEY AUTOINCREMENT,
            village TEXT,
            site_name TEXT,
            latitude REAL,
            longitude REAL,
            altitude REAL,
            habitat_description TEXT,
            province TEXT,
            district TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Taxonomy table
    cursor.execute('''
        CREATE TABLE taxonomy (
            taxonomy_id INTEGER PRIMARY KEY AUTOINCREMENT,
            kingdom TEXT,
            phylum TEXT,
            class TEXT,
            order_name TEXT,
            family TEXT,
            genus TEXT,
            species TEXT,
            scientific_name TEXT,
            common_name TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Hosts table
    cursor.execute('''
        CREATE TABLE hosts (
            host_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT,
            host_type TEXT,
            bag_id TEXT,
            field_id TEXT,
            collection_id TEXT,
            location_id INTEGER,
            taxonomy_id INTEGER,
            capture_date DATE,
            capture_time TEXT,
            trap_type TEXT,
            collectors TEXT,
            sex TEXT,
            status TEXT,
            age TEXT,
            ring_no TEXT,
            recapture TEXT,
            photo TEXT,
            material_sample TEXT,
            voucher_code TEXT,
            ecology TEXT,
            interface_type TEXT,
            use_for TEXT,
            notes TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (location_id) REFERENCES locations(location_id),
            FOREIGN KEY (taxonomy_id) REFERENCES taxonomy(taxonomy_id)
        )
    ''')
    
    # Samples table
    cursor.execute('''
        CREATE TABLE samples (
            sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT,
            host_id INTEGER,
            sample_origin TEXT,
            collection_date DATE,
            location_id INTEGER,
            saliva_id TEXT,
            anal_id TEXT,
            urine_id TEXT,
            ecto_id TEXT,
            blood_id TEXT,
            tissue_id TEXT,
            tissue_sample_type TEXT,
            intestine_id TEXT,
            plasma_id TEXT,
            adipose_id TEXT,
            remark TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (host_id) REFERENCES hosts(host_id),
            FOREIGN KEY (location_id) REFERENCES locations(location_id)
        )
    ''')
    
    # Screening results table
    cursor.execute('''
        CREATE TABLE screening_results (
            screening_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_id INTEGER,
            tested_sample_id TEXT,
            sample_type TEXT,
            screening_date DATE,
            screening_method TEXT,
            pan_corona TEXT,
            hantavirus TEXT,
            coronavirus TEXT,
            other_virus TEXT,
            result TEXT,
            notes TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sample_id) REFERENCES samples(sample_id)
        )
    ''')
    
    # Storage locations table
    cursor.execute('''
        CREATE TABLE storage_locations (
            storage_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_tube_id TEXT,
            storage_unit_id TEXT,
            rack_position TEXT,
            spot_position TEXT,
            notes TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Insert some basic taxonomy data
    print("Inserting basic taxonomy data...")
    taxonomy_data = [
        (1, 'Animalia', 'Chordata', 'Mammalia', 'Rodentia', 'Muridae', 'Rattus', 'Rattus cf. tanezumi', 'Rattus cf. tanezumi', 'Rattus'),
        (2, 'Animalia', 'Chordata', 'Mammalia', 'Rodentia', 'Muridae', 'Niviventer', 'Niviventer sp.', 'Niviventer sp.', 'Niviventer'),
        (3, 'Animalia', 'Chordata', 'Mammalia', 'Rodentia', 'Muridae', 'Niviventer', 'Niviventer tenaster', 'Niviventer tenaster', 'Niviventer'),
        (4, 'Animalia', 'Chordata', 'Mammalia', 'Rodentia', 'Sciuridae', 'Callosciurus', 'Callosciurus erythraeus', 'Callosciurus erythraeus', 'Red-bellied squirrel'),
        (5, 'Animalia', 'Chordata', 'Mammalia', 'Chiroptera', 'Hipposideridae', 'Aselliscus', 'Aselliscus stoliczkanus', 'Aselliscus stoliczkanus', 'Stoliczka\'s roundleaf bat'),
        (6, 'Animalia', 'Chordata', 'Mammalia', 'Chiroptera', 'Pteropodidae', 'Eonycteris', 'Eonycteris spelaea', 'Eonycteris spelaea', 'Cave nectar bat'),
    ]
    
    cursor.executemany('''
        INSERT INTO taxonomy (taxonomy_id, kingdom, phylum, class, order_name, family, genus, species, scientific_name, common_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', taxonomy_data)
    
    # Insert some basic location data
    print("Inserting basic location data...")
    location_data = [
        (1, None, None, None, None, None, None, 'Khammouan', None),
        (2, None, None, None, None, None, None, 'Vientiane', None),
    ]
    
    cursor.executemany('''
        INSERT INTO locations (location_id, village, site_name, latitude, longitude, altitude, habitat_description, province, district)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', location_data)
    
    # Insert some basic host data
    print("Inserting basic host data...")
    host_data = [
        (1, 'RodentHost_1', 'Rodent', None, None, None, 1, 1, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
        (2, 'NAHLHost_1', 'Market', None, 'IPLNAHL_ANA25_006', None, 2, 4, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    ]
    
    cursor.executemany('''
        INSERT INTO hosts (host_id, source_id, host_type, bag_id, field_id, collection_id, location_id, taxonomy_id, capture_date, capture_time, trap_type, collectors, sex, status, age, ring_no, recapture, photo, material_sample, voucher_code, ecology, interface_type, use_for, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', host_data)
    
    # Insert some basic sample data
    print("Inserting basic sample data...")
    sample_data = [
        (1, 'RodentSample_1', 1, 'RodentSample', None, 1, None, None, None, None, None, 'CANR_TISL24_054', 'Tissue', None, None, None, 'Rodent sample'),
        (2, 'MarketSample_1', 2, 'MarketSample', None, 2, None, None, None, None, None, None, 'IPLNAHL_ANA25_006', 'Tissue', None, None, None, 'Market sample'),
    ]
    
    cursor.executemany('''
        INSERT INTO samples (sample_id, source_id, host_id, sample_origin, collection_date, location_id, saliva_id, anal_id, urine_id, ecto_id, blood_id, tissue_id, tissue_sample_type, intestine_id, plasma_id, adipose_id, remark)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', sample_data)
    
    # Insert some basic screening data
    print("Inserting basic screening data...")
    screening_data = [
        (1, 1, 'CANR_TISL24_054', 'Tissue', None, 'PCR', 'Negative', 'Negative', 'Negative', None, 'Negative', None),
        (2, 2, 'IPLNAHL_ANA25_006', 'Tissue', None, 'PCR', 'Positive', 'Negative', 'Negative', None, 'Positive', None),
    ]
    
    cursor.executemany('''
        INSERT INTO screening_results (screening_id, sample_id, tested_sample_id, sample_type, screening_date, screening_method, pan_corona, hantavirus, coronavirus, other_virus, result, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', screening_data)
    
    # Insert some basic storage data
    print("Inserting basic storage data...")
    storage_data = [
        (1, 'CANR_TISL24_054', 'Freezer14', 'RACK09_C2R1', 'A1', 'Rodent sample storage'),
        (2, 'IPLNAHL_ANA25_006', 'Freezer14', 'RACK05_C3R5', 'B2', 'Market sample storage'),
    ]
    
    cursor.executemany('''
        INSERT INTO storage_locations (storage_id, sample_tube_id, storage_unit_id, rack_position, spot_position, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', storage_data)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Database recreated successfully at: {db_path}")
    print("✅ Basic test data inserted")
    print("✅ Ready for Excel upload testing")

if __name__ == "__main__":
    recreate_database()
