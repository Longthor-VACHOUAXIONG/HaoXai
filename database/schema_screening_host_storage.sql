-- MySQL/MariaDB Schema for Screening, Host, and Storage Tables

-- Table: screening
-- Stores screening/testing information for virus samples
CREATE TABLE IF NOT EXISTS screening (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sample_id VARCHAR(100) NOT NULL UNIQUE,
    screening_date DATE DEFAULT (CURRENT_DATE),
    received_date DATE,
    
    -- Sample Information
    sample_type ENUM('Blood', 'Serum', 'Tissue', 'Swab', 'Culture', 'Other'),
    sample_source VARCHAR(255),
    collection_method VARCHAR(255),
    
    -- Screening Details
    screening_method ENUM('PCR', 'RT-PCR', 'ELISA', 'IFA', 'Culture', 'NGS', 'Other'),
    virus_tested VARCHAR(100),
    test_result ENUM('Positive', 'Negative', 'Inconclusive', 'Pending'),
    ct_value FLOAT,
    viral_load FLOAT,
    
    -- Quality Control
    control_passed TINYINT(1) DEFAULT 1,
    control_notes TEXT,
    
    -- Personnel and Location
    tested_by VARCHAR(100),
    lab_location VARCHAR(255),
    project_name VARCHAR(255),
    
    -- Additional Information
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_screening_sample_id (sample_id),
    INDEX idx_screening_date (screening_date),
    INDEX idx_screening_result (test_result),
    INDEX idx_screening_virus (virus_tested)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: host
-- Stores information about host organisms/animals
CREATE TABLE IF NOT EXISTS host (
    id INT AUTO_INCREMENT PRIMARY KEY,
    host_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- Host Classification
    host_species VARCHAR(255) NOT NULL,
    common_name VARCHAR(255),
    scientific_name VARCHAR(255),
    host_type ENUM('Human', 'Mammal', 'Bird', 'Reptile', 'Amphibian', 'Insect', 'Other'),
    
    -- Host Details
    age_years FLOAT,
    age_group VARCHAR(50),
    sex ENUM('Male', 'Female', 'Unknown'),
    weight_kg FLOAT,
    
    -- Geographic Information
    capture_location VARCHAR(255),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    country VARCHAR(100),
    region VARCHAR(255),
    
    -- Health Status
    health_status ENUM('Healthy', 'Sick', 'Dead', 'Unknown'),
    clinical_signs TEXT,
    symptoms TEXT,
    
    -- Capture/Collection Information
    capture_date DATE,
    captured_by VARCHAR(100),
    collection_method VARCHAR(255),
    
    -- Additional Information
    habitat VARCHAR(255),
    migration_status VARCHAR(100),
    vaccination_history TEXT,
    notes TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_host_id (host_id),
    INDEX idx_host_species (host_species),
    INDEX idx_host_type (host_type),
    INDEX idx_host_location (capture_location),
    INDEX idx_host_country (country)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: storage
-- Tracks physical storage locations and conditions for samples
CREATE TABLE IF NOT EXISTS storage (
    id INT AUTO_INCREMENT PRIMARY KEY,
    storage_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- Sample Reference
    sample_id VARCHAR(100),
    related_screening_id INT,
    related_host_id INT,
    
    -- Storage Location
    storage_location VARCHAR(255) NOT NULL,
    building VARCHAR(100),
    room VARCHAR(50),
    freezer_name VARCHAR(100),
    rack VARCHAR(50),
    box VARCHAR(50),
    position VARCHAR(50),
    
    -- Storage Conditions
    storage_type ENUM('Ultra-Low (-80°C)', 'Freezer (-20°C)', 'Refrigerator (4°C)', 'Room Temp', 'Liquid Nitrogen', 'Other'),
    temperature_celsius FLOAT,
    storage_medium VARCHAR(255),
    
    -- Sample Details
    sample_type VARCHAR(100),
    volume_ml FLOAT,
    concentration VARCHAR(100),
    aliquots INT DEFAULT 1,
    
    -- Dates
    storage_date DATE DEFAULT (CURRENT_DATE),
    expiry_date DATE,
    last_accessed DATE,
    
    -- Status
    status ENUM('Available', 'In Use', 'Depleted', 'Discarded', 'Reserved') DEFAULT 'Available',
    barcode VARCHAR(100) UNIQUE,
    
    -- Personnel
    stored_by VARCHAR(100),
    accessed_by VARCHAR(100),
    
    -- Additional Information
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_storage_id (storage_id),
    INDEX idx_storage_sample_id (sample_id),
    INDEX idx_storage_location (storage_location),
    INDEX idx_storage_type (storage_type),
    INDEX idx_storage_status (status),
    INDEX idx_storage_barcode (barcode),
    
    FOREIGN KEY (related_screening_id) REFERENCES screening(id) ON DELETE SET NULL,
    FOREIGN KEY (related_host_id) REFERENCES host(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: sample_host_link
-- Links samples/screenings to host organisms (many-to-many relationship)
CREATE TABLE IF NOT EXISTS sample_host_link (
    id INT AUTO_INCREMENT PRIMARY KEY,
    screening_id INT NOT NULL,
    host_id INT NOT NULL,
    
    sample_relationship VARCHAR(255),
    collection_date DATE,
    notes TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_sample_host_screening (screening_id),
    INDEX idx_sample_host_host (host_id),
    UNIQUE KEY unique_screening_host (screening_id, host_id),
    
    FOREIGN KEY (screening_id) REFERENCES screening(id) ON DELETE CASCADE,
    FOREIGN KEY (host_id) REFERENCES host(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: storage_history
-- Tracks movements and changes in storage
CREATE TABLE IF NOT EXISTS storage_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    storage_id INT NOT NULL,
    
    action ENUM('Stored', 'Moved', 'Retrieved', 'Returned', 'Depleted', 'Discarded'),
    previous_location VARCHAR(255),
    new_location VARCHAR(255),
    
    volume_removed_ml FLOAT,
    volume_remaining_ml FLOAT,
    
    action_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    performed_by VARCHAR(100),
    reason VARCHAR(255),
    notes TEXT,
    
    INDEX idx_storage_history_storage_id (storage_id),
    INDEX idx_storage_history_date (action_date),
    
    FOREIGN KEY (storage_id) REFERENCES storage(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;