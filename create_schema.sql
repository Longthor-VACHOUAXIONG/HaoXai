-- ViroDB Database Schema
-- Target: MySQL / MariaDB

CREATE DATABASE IF NOT EXISTS CAN2;
USE CAN2;

-- 1. locations
CREATE TABLE IF NOT EXISTS locations (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    country VARCHAR(100) NOT NULL DEFAULT 'Laos',
    province VARCHAR(100) NOT NULL,
    district VARCHAR(100),
    village VARCHAR(100),
    site_name VARCHAR(255),
    latitude VARCHAR(150),
    longitude VARCHAR(150),
    altitude VARCHAR(100),
    habitat_description TEXT,
    habitat_photo VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_location (province, district, village, site_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. taxonomy
CREATE TABLE IF NOT EXISTS taxonomy (
    taxonomy_id INT AUTO_INCREMENT PRIMARY KEY,
    kingdom VARCHAR(100),
    phylum VARCHAR(100),
    class_name VARCHAR(100),
    order_name VARCHAR(100),
    family VARCHAR(100),
    genus VARCHAR(150),
    species VARCHAR(255),
    scientific_name VARCHAR(255),
    common_name VARCHAR(255),
    english_common_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_scientific (scientific_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. hosts
CREATE TABLE IF NOT EXISTS hosts (
    host_id INT AUTO_INCREMENT PRIMARY KEY,
    source_id VARCHAR(150) NOT NULL,
    host_type ENUM('Bat','Rodent','Market') NOT NULL,
    bag_id VARCHAR(150),
    field_id VARCHAR(150),
    collection_id VARCHAR(150),
    location_id INT,
    taxonomy_id INT,
    capture_date DATE,
    capture_time VARCHAR(100),
    trap_type VARCHAR(150),
    collectors VARCHAR(1000),
    sex VARCHAR(100),
    status VARCHAR(200),
    age VARCHAR(100),
    ring_no VARCHAR(150),
    recapture VARCHAR(50),
    photo VARCHAR(500),
    material_sample VARCHAR(150),
    voucher_code VARCHAR(150),
    ecology TEXT,
    interface_type VARCHAR(150),
    use_for VARCHAR(150),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_source (source_id, host_type),
    KEY idx_bag (bag_id),
    KEY idx_field (field_id),
    KEY idx_location (location_id),
    KEY idx_taxonomy (taxonomy_id),
    CONSTRAINT fk_host_location FOREIGN KEY (location_id) REFERENCES locations(location_id),
    CONSTRAINT fk_host_taxonomy FOREIGN KEY (taxonomy_id) REFERENCES taxonomy(taxonomy_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. morphometrics
CREATE TABLE IF NOT EXISTS morphometrics (
    morpho_id INT AUTO_INCREMENT PRIMARY KEY,
    host_id INT NOT NULL,
    weight_g DECIMAL(12,2),
    head_body_mm DECIMAL(12,2),
    forearm_mm DECIMAL(12,2),
    ear_mm DECIMAL(12,2),
    tail_mm DECIMAL(12,2),
    tibia_mm DECIMAL(12,2),
    hind_foot_mm DECIMAL(12,2),
    third_mt DECIMAL(12,2),
    fourth_mt DECIMAL(12,2),
    fifth_mt DECIMAL(12,2),
    third_d1p DECIMAL(12,2),
    third_d2p DECIMAL(12,2),
    fourth_d1p DECIMAL(12,2),
    fourth_d2p DECIMAL(12,2),
    mammae VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_host (host_id),
    CONSTRAINT fk_morpho_host FOREIGN KEY (host_id) REFERENCES hosts(host_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. samples
CREATE TABLE IF NOT EXISTS samples (
    sample_id INT AUTO_INCREMENT PRIMARY KEY,
    source_id VARCHAR(150) NOT NULL,
    host_id INT,
    sample_origin ENUM('BatSwab','BatTissue','RodentSample') NOT NULL,
    collection_date DATE,
    location_id INT,
    saliva_id VARCHAR(150),
    anal_id VARCHAR(150),
    urine_id VARCHAR(150),
    ecto_id VARCHAR(150),
    blood_id VARCHAR(150),
    tissue_id VARCHAR(150),
    tissue_sample_type VARCHAR(500),
    intestine_id VARCHAR(150),
    plasma_id VARCHAR(150),
    adipose_id VARCHAR(150),
    remark TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_source (source_id, sample_origin),
    KEY idx_host (host_id),
    KEY idx_saliva (saliva_id),
    KEY idx_anal (anal_id),
    KEY idx_tissue (tissue_id),
    KEY idx_blood (blood_id),
    CONSTRAINT fk_sample_host FOREIGN KEY (host_id) REFERENCES hosts(host_id),
    CONSTRAINT fk_sample_location FOREIGN KEY (location_id) REFERENCES locations(location_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6. environmental_samples
CREATE TABLE IF NOT EXISTS environmental_samples (
    env_sample_id INT AUTO_INCREMENT PRIMARY KEY,
    source_id VARCHAR(150) NOT NULL,
    pool_id VARCHAR(150) NOT NULL,
    collection_method VARCHAR(255),
    collection_date DATE,
    location_id INT,
    site_type VARCHAR(150),
    remark TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_source (source_id),
    CONSTRAINT fk_env_location FOREIGN KEY (location_id) REFERENCES locations(location_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7. screening_results
CREATE TABLE IF NOT EXISTS screening_results (
    screening_id INT AUTO_INCREMENT PRIMARY KEY,
    source_id VARCHAR(200),
    sample_id INT,
    env_sample_id INT,
    team VARCHAR(150),
    sample_type VARCHAR(150),
    tested_sample_id VARCHAR(200) NOT NULL,
    pan_corona VARCHAR(100),
    pan_hanta VARCHAR(100),
    pan_paramyxo VARCHAR(100),
    pan_flavi VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_sample (sample_id),
    KEY idx_env (env_sample_id),
    KEY idx_tested (tested_sample_id),
    CONSTRAINT fk_screen_sample FOREIGN KEY (sample_id) REFERENCES samples(sample_id),
    CONSTRAINT fk_screen_env FOREIGN KEY (env_sample_id) REFERENCES environmental_samples(env_sample_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 8. storage_locations
CREATE TABLE IF NOT EXISTS storage_locations (
    storage_id INT AUTO_INCREMENT PRIMARY KEY,
    sample_tube_id VARCHAR(200) NOT NULL,
    rack_position VARCHAR(150) NOT NULL,
    spot_position VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tube_rack (sample_tube_id, rack_position),
    KEY idx_tube (sample_tube_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
