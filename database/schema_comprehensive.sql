-- MySQL/MariaDB Comprehensive Schema for Bat, Swab, Tissue, Environmental, Freezer, Market, Rodent, and Screening Tables

-- Table: bat_data
-- Stores bat specimen collection data
CREATE TABLE IF NOT EXISTS bat_data (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    SourceId VARCHAR(100),
    BagId VARCHAR(100),
    FieldId VARCHAR(100),
    CollectionId VARCHAR(100),
    ScientificName VARCHAR(255),
    MaterialSample VARCHAR(255),
    GeneticSequence TEXT,
    RingNo VARCHAR(100),
    ReCapture VARCHAR(50),
    RecorderType VARCHAR(100),
    CallRecord VARCHAR(255),
    RecordType VARCHAR(100),
    Photo TEXT,
    CaptureTime VARCHAR(50),
    TrapType VARCHAR(100),
    CaptureDate DATE,
    Collectors TEXT,
    
    -- Taxonomy
    Kingdom VARCHAR(100),
    Phylum VARCHAR(100),
    Class VARCHAR(100),
    Or_der VARCHAR(100),
    Family VARCHAR(100),
    Genus VARCHAR(100),
    Species VARCHAR(255),
    Sex ENUM('Male', 'Female', 'Unknown', ''),
    Status VARCHAR(100),
    
    -- Location
    Location VARCHAR(255),
    Village VARCHAR(255),
    District VARCHAR(255),
    Province VARCHAR(255),
    Country VARCHAR(100),
    Altitude FLOAT,
    Latitude DECIMAL(10, 8),
    Longitude DECIMAL(11, 8),
    
    -- Habitat
    HabitatDescription TEXT,
    HabitatPhoto TEXT,
    
    -- Storage
    BottleNo VARCHAR(100),
    CarbinetNo VARCHAR(100),
    CarbinetFloor VARCHAR(50),
    FloorRow VARCHAR(50),
    FloorColumn VARCHAR(50),
    BoxNo VARCHAR(100),
    
    -- Measurements (mm)
    HB FLOAT,
    FA FLOAT,
    EL FLOAT,
    TL FLOAT,
    TIB FLOAT,
    HF FLOAT,
    ThreeMT FLOAT,
    FourMT FLOAT,
    FiveMT FLOAT,
    ThreeD1P FLOAT,
    ThreeD2P FLOAT,
    FourD1P FLOAT,
    FourD2P FLOAT,
    W FLOAT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_bat_sourceid (SourceId),
    INDEX idx_bat_species (Species),
    INDEX idx_bat_province (Province),
    INDEX idx_bat_capturedate (CaptureDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: swab_data
-- Stores swab sample collection data
CREATE TABLE IF NOT EXISTS swab_data (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    SourceId VARCHAR(100),
    No VARCHAR(50),
    Date DATE,
    Province VARCHAR(255),
    District VARCHAR(255),
    Village VARCHAR(255),
    Method VARCHAR(255),
    Time VARCHAR(50),
    BagId VARCHAR(100),
    RingId VARCHAR(100),
    SalivaId VARCHAR(100),
    AnalId VARCHAR(100),
    UrineId VARCHAR(100),
    EctoId VARCHAR(100),
    Remark TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_swab_sourceid (SourceId),
    INDEX idx_swab_date (Date),
    INDEX idx_swab_province (Province)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: tissue_data
-- Stores tissue sample collection data
CREATE TABLE IF NOT EXISTS tissue_data (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    SourceId VARCHAR(100),
    No VARCHAR(50),
    Province VARCHAR(255),
    District VARCHAR(255),
    Village VARCHAR(255),
    Date DATE,
    CaptureTime VARCHAR(50),
    BagId VARCHAR(100),
    VoucherCode VARCHAR(100),
    BloodId VARCHAR(100),
    TissueId VARCHAR(100),
    TissueSampleType VARCHAR(255),
    IntestineId VARCHAR(100),
    PlasmaId VARCHAR(100),
    Remark TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_tissue_sourceid (SourceId),
    INDEX idx_tissue_date (Date),
    INDEX idx_tissue_province (Province)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: environmental_data
-- Stores environmental sample collection data
CREATE TABLE IF NOT EXISTS environmental_data (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    SourceID VARCHAR(100),
    Province VARCHAR(255),
    District VARCHAR(255),
    Village VARCHAR(255),
    PoolID VARCHAR(100),
    CollectionMethod VARCHAR(255),
    Date DATE,
    Location VARCHAR(255),
    Remark TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_env_sourceid (SourceID),
    INDEX idx_env_date (Date),
    INDEX idx_env_province (Province)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: freezer_storage
-- Stores freezer storage location data
CREATE TABLE IF NOT EXISTS freezer_storage (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    SampleId VARCHAR(100),
    Location VARCHAR(255),
    SpotPosition VARCHAR(255),
    Notes TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_freezer_sampleid (SampleId),
    INDEX idx_freezer_location (Location)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: market_data
-- Stores market survey/collection data
CREATE TABLE IF NOT EXISTS market_data (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    SourceId VARCHAR(100),
    FieldSampleId VARCHAR(100),
    CommonName VARCHAR(255),
    StatusOfSample VARCHAR(100),
    ScientificName VARCHAR(255),
    EnglishCommonName VARCHAR(255),
    LocationName VARCHAR(255),
    CollectionSampleDate DATE,
    TypeOfInterface VARCHAR(255),
    District VARCHAR(255),
    Province VARCHAR(255),
    Longitude DECIMAL(11, 8),
    Latitute DECIMAL(10, 8),
    Sex VARCHAR(50),
    Age VARCHAR(50),
    Weightg FLOAT,
    HBmm FLOAT,
    Tailmm FLOAT,
    ForeArm FLOAT,
    UseForFoodOrMedicine VARCHAR(255),
    TimeToCollect VARCHAR(100),
    PhotoNumber VARCHAR(100),
    Note TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_market_sourceid (SourceId),
    INDEX idx_market_date (CollectionSampleDate),
    INDEX idx_market_province (Province)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: rodent_host
-- Stores rodent host specimen data
CREATE TABLE IF NOT EXISTS rodent_host (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    FieldId VARCHAR(100),
    Species VARCHAR(255),
    Sex VARCHAR(50),
    Status VARCHAR(100),
    W FLOAT,
    HB FLOAT,
    T FLOAT,
    E FLOAT,
    HF FLOAT,
    Mammae VARCHAR(100),
    Price FLOAT,
    Location VARCHAR(255),
    Note TEXT,
    Ecology TEXT,
    TrapId VARCHAR(100),
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_rodent_fieldid (FieldId),
    INDEX idx_rodent_species (Species),
    INDEX idx_rodent_location (Location)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: rodent_sample
-- Stores rodent sample collection data
CREATE TABLE IF NOT EXISTS rodent_sample (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    SourceId VARCHAR(100),
    No VARCHAR(50),
    Province VARCHAR(255),
    District VARCHAR(255),
    Village VARCHAR(255),
    Date DATE,
    RodentId VARCHAR(100),
    SalivaId VARCHAR(100),
    AnalId VARCHAR(100),
    UrineId VARCHAR(100),
    EctoId VARCHAR(100),
    BloodId VARCHAR(100),
    TissueId VARCHAR(100),
    TissueSampleType VARCHAR(255),
    IntestineId VARCHAR(100),
    AdiposeId VARCHAR(100),
    PlasmaId VARCHAR(100),
    Remark TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_rodent_sample_sourceid (SourceId),
    INDEX idx_rodent_sample_date (Date),
    INDEX idx_rodent_sample_province (Province)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: screening_data
-- Stores virus screening/testing data
CREATE TABLE IF NOT EXISTS screening_data (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    SourceId VARCHAR(100),
    Team VARCHAR(255),
    SampleType VARCHAR(100),
    SampleId VARCHAR(100),
    SampleLocation VARCHAR(255),
    RNAPlate VARCHAR(100),
    
    -- cDNA
    cDNADate DATE,
    
    -- PanCorona
    PanCoronaDate DATE,
    PanCorona VARCHAR(100),
    
    -- PanParamyxo
    PanParamyxoDate DATE,
    PanParamyxo VARCHAR(100),
    
    -- PanHanta
    PanHantaDate DATE,
    PanHanta VARCHAR(100),
    
    -- PanFlavi
    PanFlaviDate DATE,
    PanFlavi VARCHAR(100),
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_screening_sourceid (SourceId),
    INDEX idx_screening_sampleid (SampleId),
    INDEX idx_screening_sampletype (SampleType),
    INDEX idx_screening_team (Team)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ==================== LINKING TABLES ====================

-- Table: bat_screening_link
-- Links bat specimens to screening samples (one bat can have many samples)
CREATE TABLE IF NOT EXISTS bat_screening_link (
    id INT AUTO_INCREMENT PRIMARY KEY,
    bat_data_id INT NOT NULL,
    screening_data_id INT NOT NULL,
    source_id VARCHAR(100),  -- SourceId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_bat_screening_bat (bat_data_id),
    INDEX idx_bat_screening_screening (screening_data_id),
    INDEX idx_bat_screening_source (source_id),
    
    FOREIGN KEY (bat_data_id) REFERENCES bat_data(id) ON DELETE CASCADE,
    FOREIGN KEY (screening_data_id) REFERENCES screening_data(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: market_screening_link
-- Links market specimens to screening samples (one market sample can have many samples)
CREATE TABLE IF NOT EXISTS market_screening_link (
    id INT AUTO_INCREMENT PRIMARY KEY,
    market_data_id INT NOT NULL,
    screening_data_id INT NOT NULL,
    source_id VARCHAR(100),  -- SourceId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_market_screening_market (market_data_id),
    INDEX idx_market_screening_screening (screening_data_id),
    INDEX idx_market_screening_source (source_id),
    
    FOREIGN KEY (market_data_id) REFERENCES market_data(id) ON DELETE CASCADE,
    FOREIGN KEY (screening_data_id) REFERENCES screening_data(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: rodenthost_screening_link
-- Links rodent host to screening samples (one rodent can have many samples)
CREATE TABLE IF NOT EXISTS rodenthost_screening_link (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rodent_host_id INT NOT NULL,
    screening_data_id INT NOT NULL,
    field_id VARCHAR(100),  -- FieldId for matching
    source_id VARCHAR(100),  -- SourceId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_rodenthost_screening_rodent (rodent_host_id),
    INDEX idx_rodenthost_screening_screening (screening_data_id),
    INDEX idx_rodenthost_screening_field (field_id),
    INDEX idx_rodenthost_screening_source (source_id),
    
    FOREIGN KEY (rodent_host_id) REFERENCES rodent_host(id) ON DELETE CASCADE,
    FOREIGN KEY (screening_data_id) REFERENCES screening_data(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: screening_sequence_link
-- Links screening samples to sequences (one sample can have many sequences)
CREATE TABLE IF NOT EXISTS screening_sequence_link (
    id INT AUTO_INCREMENT PRIMARY KEY,
    screening_data_id INT NOT NULL,
    sequence_id INT,
    consensus_id INT,
    blast_result_id INT,
    sample_id VARCHAR(100),  -- SampleId for matching
    virus_type_matched VARCHAR(100),  -- PanCorona, PanHanta, PanFlavi, PanParamyxo
    sequence_confirmed TINYINT(1) DEFAULT 0,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_screening_seq_screening (screening_data_id),
    INDEX idx_screening_seq_sequence (sequence_id),
    INDEX idx_screening_seq_sample (sample_id),
    
    FOREIGN KEY (screening_data_id) REFERENCES screening_data(id) ON DELETE CASCADE,
    FOREIGN KEY (sequence_id) REFERENCES sequences(id) ON DELETE SET NULL,
    FOREIGN KEY (consensus_id) REFERENCES consensus_sequences(id) ON DELETE SET NULL,
    FOREIGN KEY (blast_result_id) REFERENCES blast_results(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: freezer_swab_link
-- Links freezer storage to swab samples
CREATE TABLE IF NOT EXISTS freezer_swab_link (
    id INT AUTO_INCREMENT PRIMARY KEY,
    freezer_storage_id INT NOT NULL,
    swab_data_id INT NOT NULL,
    sample_id VARCHAR(100),  -- SampleId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_freezer_swab_freezer (freezer_storage_id),
    INDEX idx_freezer_swab_swab (swab_data_id),
    INDEX idx_freezer_swab_sample (sample_id),
    
    FOREIGN KEY (freezer_storage_id) REFERENCES freezer_storage(id) ON DELETE CASCADE,
    FOREIGN KEY (swab_data_id) REFERENCES swab_data(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: freezer_tissue_link
-- Links freezer storage to tissue samples
CREATE TABLE IF NOT EXISTS freezer_tissue_link (
    id INT AUTO_INCREMENT PRIMARY KEY,
    freezer_storage_id INT NOT NULL,
    tissue_data_id INT NOT NULL,
    sample_id VARCHAR(100),  -- SampleId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_freezer_tissue_freezer (freezer_storage_id),
    INDEX idx_freezer_tissue_tissue (tissue_data_id),
    INDEX idx_freezer_tissue_sample (sample_id),
    
    FOREIGN KEY (freezer_storage_id) REFERENCES freezer_storage(id) ON DELETE CASCADE,
    FOREIGN KEY (tissue_data_id) REFERENCES tissue_data(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: freezer_environmental_link
-- Links freezer storage to environmental samples
CREATE TABLE IF NOT EXISTS freezer_environmental_link (
    id INT AUTO_INCREMENT PRIMARY KEY,
    freezer_storage_id INT NOT NULL,
    environmental_data_id INT NOT NULL,
    sample_id VARCHAR(100),  -- SampleId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_freezer_env_freezer (freezer_storage_id),
    INDEX idx_freezer_env_env (environmental_data_id),
    INDEX idx_freezer_env_sample (sample_id),
    
    FOREIGN KEY (freezer_storage_id) REFERENCES freezer_storage(id) ON DELETE CASCADE,
    FOREIGN KEY (environmental_data_id) REFERENCES environmental_data(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: freezer_market_link
-- Links freezer storage to market samples
CREATE TABLE IF NOT EXISTS freezer_market_link (
    id INT AUTO_INCREMENT PRIMARY KEY,
    freezer_storage_id INT NOT NULL,
    market_data_id INT NOT NULL,
    sample_id VARCHAR(100),  -- SampleId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_freezer_market_freezer (freezer_storage_id),
    INDEX idx_freezer_market_market (market_data_id),
    INDEX idx_freezer_market_sample (sample_id),
    
    FOREIGN KEY (freezer_storage_id) REFERENCES freezer_storage(id) ON DELETE CASCADE,
    FOREIGN KEY (market_data_id) REFERENCES market_data(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: freezer_rodentsample_link
-- Links freezer storage to rodent samples
CREATE TABLE IF NOT EXISTS freezer_rodentsample_link (
    id INT AUTO_INCREMENT PRIMARY KEY,
    freezer_storage_id INT NOT NULL,
    rodent_sample_id INT NOT NULL,
    sample_id VARCHAR(100),  -- SampleId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_freezer_rodent_freezer (freezer_storage_id),
    INDEX idx_freezer_rodent_rodent (rodent_sample_id),
    INDEX idx_freezer_rodent_sample (sample_id),
    
    FOREIGN KEY (freezer_storage_id) REFERENCES freezer_storage(id) ON DELETE CASCADE,
    FOREIGN KEY (rodent_sample_id) REFERENCES rodent_sample(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
