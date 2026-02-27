
-- =========================================
-- Enhanced Storage Management Schema
-- Multi-Freezer Support
-- =========================================

-- 1. FREEZERS TABLE (define each freezer)
CREATE TABLE IF NOT EXISTS freezers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    freezer_name VARCHAR(100) NOT NULL UNIQUE,
    freezer_model VARCHAR(100),
    freezer_type ENUM('ultra_low', 'low_temp', 'refrigerator', 'other') DEFAULT 'ultra_low',
    temperature_range VARCHAR(50), -- e.g., '-80C to -60C'
    location VARCHAR(255), -- building/room location
    manufacturer VARCHAR(100),
    serial_number VARCHAR(100),
    installation_date DATE,
    last_maintenance_date DATE,
    next_maintenance_date DATE,
    status ENUM('active', 'maintenance', 'inactive') DEFAULT 'active',
    capacity_shelves INT DEFAULT 0,
    capacity_racks_per_shelf INT DEFAULT 0,
    capacity_boxes_per_rack INT DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. FREEZER_SHELVES TABLE (shelves within freezers)
CREATE TABLE IF NOT EXISTS freezer_shelves (
    id INT AUTO_INCREMENT PRIMARY KEY,
    freezer_id INT NOT NULL,
    shelf_number INT NOT NULL,
    shelf_name VARCHAR(50),
    temperature_monitor VARCHAR(100), -- temperature sensor ID
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (freezer_id) REFERENCES freezers(id) ON DELETE CASCADE,
    UNIQUE KEY unique_freezer_shelf (freezer_id, shelf_number)
);

-- 3. FREEZER_RACKS TABLE (racks within shelves)
CREATE TABLE IF NOT EXISTS freezer_racks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    shelf_id INT NOT NULL,
    rack_number INT NOT NULL,
    rack_name VARCHAR(50),
    rack_type VARCHAR(50), -- e.g., '96-well', 'cryovial_rack'
    capacity_positions INT DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (shelf_id) REFERENCES freezer_shelves(id) ON DELETE CASCADE,
    UNIQUE KEY unique_shelf_rack (shelf_id, rack_number)
);

-- 4. STORAGE_BOXES TABLE (boxes within racks)
CREATE TABLE IF NOT EXISTS storage_boxes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rack_id INT NOT NULL,
    box_number INT NOT NULL,
    box_name VARCHAR(50),
    box_type VARCHAR(50), -- e.g., '2ml_cryovial', '1.5ml_microcentrifuge'
    capacity_positions INT DEFAULT 0,
    color VARCHAR(50),
    barcode VARCHAR(100) UNIQUE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (rack_id) REFERENCES freezer_racks(id) ON DELETE CASCADE,
    UNIQUE KEY unique_rack_box (rack_id, box_number)
);

-- 5. ENHANCED STORAGE_LOCATIONS TABLE (link samples to specific positions)
CREATE TABLE IF NOT EXISTS storage_locations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sample_id INT NOT NULL,
    box_id INT NOT NULL,
    position_number INT NOT NULL, -- position within the box (1-81 for 9x9 grid, etc.)
    position_coordinates VARCHAR(20), -- e.g., 'A1', 'B5', '3,7'
    storage_date DATE,
    stored_by VARCHAR(100),
    removal_date DATE,
    removed_by VARCHAR(100),
    purpose VARCHAR(100), -- e.g., 'long_term', 'analysis', 'backup'
    status ENUM('stored', 'removed', 'consumed', 'expired') DEFAULT 'stored',
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (sample_id) REFERENCES samples(id),
    FOREIGN KEY (box_id) REFERENCES storage_boxes(id),
    UNIQUE KEY unique_sample_storage (sample_id, box_id, position_number)
);

-- =========================================
-- Indexes for Performance
-- =========================================

-- Freezers indexes
CREATE INDEX idx_freezers_status ON freezers(status);
CREATE INDEX idx_freezers_type ON freezers(freezer_type);
CREATE INDEX idx_freezers_location ON freezers(location);

-- Storage hierarchy indexes
CREATE INDEX idx_shelves_freezer ON freezer_shelves(freezer_id);
CREATE INDEX idx_racks_shelf ON freezer_racks(shelf_id);
CREATE INDEX idx_boxes_rack ON storage_boxes(rack_id);
CREATE INDEX idx_storage_box ON storage_locations(box_id);
CREATE INDEX idx_storage_sample ON storage_locations(sample_id);
CREATE INDEX idx_storage_status ON storage_locations(status);

-- =========================================
-- Sample Data for Testing
-- =========================================

-- Insert sample freezers
INSERT INTO freezers (freezer_name, freezer_model, freezer_type, temperature_range, location, capacity_shelves) VALUES
('Freezer_A_-80C', 'Thermo Fisher ULT', 'ultra_low', '-86C to -60C', 'Lab Room 101', 5),
('Freezer_B_-80C', 'Panasonic VIP', 'ultra_low', '-85C to -50C', 'Lab Room 102', 7),
('Freezer_C_-20C', 'So-Low', 'low_temp', '-30C to -10C', 'Cold Room', 4)
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;

-- Insert sample shelves for first freezer
INSERT INTO freezer_shelves (freezer_id, shelf_number, shelf_name) VALUES
(1, 1, 'Shelf 1 Top'),
(1, 2, 'Shelf 2'),
(1, 3, 'Shelf 3'),
(1, 4, 'Shelf 4'),
(1, 5, 'Shelf 5 Bottom')
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;

-- Insert sample racks for first shelf
INSERT INTO freezer_racks (shelf_id, rack_number, rack_name, capacity_positions) VALUES
(1, 1, 'Rack 1 Left', 81),
(1, 2, 'Rack 2 Center', 81),
(1, 3, 'Rack 3 Right', 81)
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;

-- Insert sample boxes for first rack
INSERT INTO storage_boxes (rack_id, box_number, box_name, box_type, capacity_positions, color) VALUES
(1, 1, 'Box 1-1', '2ml_cryovial', 81, 'Blue'),
(1, 2, 'Box 1-2', '2ml_cryovial', 81, 'Green'),
(1, 3, 'Box 1-3', '2ml_cryovial', 81, 'Yellow')
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;
