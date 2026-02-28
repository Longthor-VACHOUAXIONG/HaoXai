-- SQLite Comprehensive Schema for Bat, Swab, Tissue, Environmental, Freezer, Market, Rodent, and Screening Tables

-- Table: bat_data
-- Stores bat specimen collection data
CREATE TABLE IF NOT EXISTS bat_data (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    SourceId TEXT,
    BagId TEXT,
    FieldId TEXT,
    CollectionId TEXT,
    ScientificName TEXT,
    MaterialSample TEXT,
    GeneticSequence TEXT,
    RingNo TEXT,
    ReCapture TEXT,
    RecorderType TEXT,
    CallRecord TEXT,
    RecordType TEXT,
    Photo TEXT,
    CaptureTime TEXT,
    TrapType TEXT,
    CaptureDate DATE,
    Collectors TEXT,
    
    -- Taxonomy
    Kingdom TEXT,
    Phylum TEXT,
    Class TEXT,
    Or_der TEXT,
    Family TEXT,
    Genus TEXT,
    Species TEXT,
    Sex TEXT CHECK(Sex IN ('Male', 'Female', 'Unknown', '')),
    Status TEXT,
    
    -- Location
    Location TEXT,
    Village TEXT,
    District TEXT,
    Province TEXT,
    Country TEXT,
    Altitude REAL,
    Latitude REAL,
    Longitude REAL,
    
    -- Habitat
    HabitatDescription TEXT,
    HabitatPhoto TEXT,
    
    -- Storage
    BottleNo TEXT,
    CarbinetNo TEXT,
    CarbinetFloor TEXT,
    FloorRow TEXT,
    FloorColumn TEXT,
    BoxNo TEXT,
    
    -- Measurements (mm)
    HB REAL,
    FA REAL,
    EL REAL,
    TL REAL,
    TIB REAL,
    HF REAL,
    ThreeMT REAL,
    FourMT REAL,
    FiveMT REAL,
    ThreeD1P REAL,
    ThreeD2P REAL,
    FourD1P REAL,
    FourD2P REAL,
    W REAL,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bat_sourceid ON bat_data(SourceId);
CREATE INDEX IF NOT EXISTS idx_bat_species ON bat_data(Species);
CREATE INDEX IF NOT EXISTS idx_bat_province ON bat_data(Province);
CREATE INDEX IF NOT EXISTS idx_bat_capturedate ON bat_data(CaptureDate);

-- Table: swab_data
-- Stores swab sample collection data
CREATE TABLE IF NOT EXISTS swab_data (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    SourceId TEXT,
    No TEXT,
    Date DATE,
    Province TEXT,
    District TEXT,
    Village TEXT,
    Method TEXT,
    Time TEXT,
    BagId TEXT,
    RingId TEXT,
    SalivaId TEXT,
    AnalId TEXT,
    UrineId TEXT,
    EctoId TEXT,
    Remark TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_swab_sourceid ON swab_data(SourceId);
CREATE INDEX IF NOT EXISTS idx_swab_date ON swab_data(Date);
CREATE INDEX IF NOT EXISTS idx_swab_province ON swab_data(Province);

-- Table: tissue_data
-- Stores tissue sample collection data
CREATE TABLE IF NOT EXISTS tissue_data (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    SourceId TEXT,
    No TEXT,
    Province TEXT,
    District TEXT,
    Village TEXT,
    Date DATE,
    CaptureTime TEXT,
    BagId TEXT,
    VoucherCode TEXT,
    BloodId TEXT,
    TissueId TEXT,
    TissueSampleType TEXT,
    IntestineId TEXT,
    PlasmaId TEXT,
    Remark TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tissue_sourceid ON tissue_data(SourceId);
CREATE INDEX IF NOT EXISTS idx_tissue_date ON tissue_data(Date);
CREATE INDEX IF NOT EXISTS idx_tissue_province ON tissue_data(Province);

-- Table: environmental_data
-- Stores environmental sample collection data
CREATE TABLE IF NOT EXISTS environmental_data (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    SourceID TEXT,
    Province TEXT,
    District TEXT,
    Village TEXT,
    PoolID TEXT,
    CollectionMethod TEXT,
    Date DATE,
    Location TEXT,
    Remark TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_env_sourceid ON environmental_data(SourceID);
CREATE INDEX IF NOT EXISTS idx_env_date ON environmental_data(Date);
CREATE INDEX IF NOT EXISTS idx_env_province ON environmental_data(Province);

-- Table: freezer_storage
-- Stores freezer storage location data
CREATE TABLE IF NOT EXISTS freezer_storage (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    SampleId TEXT,
    Location TEXT,
    SpotPosition TEXT,
    Notes TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_freezer_sampleid ON freezer_storage(SampleId);
CREATE INDEX IF NOT EXISTS idx_freezer_location ON freezer_storage(Location);

-- Table: market_data
-- Stores market survey/collection data
CREATE TABLE IF NOT EXISTS market_data (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    SourceId TEXT,
    FieldSampleId TEXT,
    CommonName TEXT,
    StatusOfSample TEXT,
    ScientificName TEXT,
    EnglishCommonName TEXT,
    LocationName TEXT,
    CollectionSampleDate DATE,
    TypeOfInterface TEXT,
    District TEXT,
    Province TEXT,
    Longitude REAL,
    Latitute REAL,
    Sex TEXT,
    Age TEXT,
    Weightg REAL,
    HBmm REAL,
    Tailmm REAL,
    ForeArm REAL,
    UseForFoodOrMedicine TEXT,
    TimeToCollect TEXT,
    PhotoNumber TEXT,
    Note TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_market_sourceid ON market_data(SourceId);
CREATE INDEX IF NOT EXISTS idx_market_date ON market_data(CollectionSampleDate);
CREATE INDEX IF NOT EXISTS idx_market_province ON market_data(Province);

-- Table: rodent_host
-- Stores rodent host specimen data
CREATE TABLE IF NOT EXISTS rodent_host (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    FieldId TEXT,
    Species TEXT,
    Sex TEXT,
    Status TEXT,
    W REAL,
    HB REAL,
    T REAL,
    E REAL,
    HF REAL,
    Mammae TEXT,
    Price REAL,
    Location TEXT,
    Note TEXT,
    Ecology TEXT,
    TrapId TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rodent_fieldid ON rodent_host(FieldId);
CREATE INDEX IF NOT EXISTS idx_rodent_species ON rodent_host(Species);
CREATE INDEX IF NOT EXISTS idx_rodent_location ON rodent_host(Location);

-- Table: rodent_sample
-- Stores rodent sample collection data
CREATE TABLE IF NOT EXISTS rodent_sample (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    SourceId TEXT,
    No TEXT,
    Province TEXT,
    District TEXT,
    Village TEXT,
    Date DATE,
    RodentId TEXT,
    SalivaId TEXT,
    AnalId TEXT,
    UrineId TEXT,
    EctoId TEXT,
    BloodId TEXT,
    TissueId TEXT,
    TissueSampleType TEXT,
    IntestineId TEXT,
    AdiposeId TEXT,
    PlasmaId TEXT,
    Remark TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rodent_sample_sourceid ON rodent_sample(SourceId);
CREATE INDEX IF NOT EXISTS idx_rodent_sample_date ON rodent_sample(Date);
CREATE INDEX IF NOT EXISTS idx_rodent_sample_province ON rodent_sample(Province);

-- Table: screening_data
-- Stores virus screening/testing data
CREATE TABLE IF NOT EXISTS screening_data (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    SourceId TEXT,
    Team TEXT,
    SampleType TEXT,
    SampleId TEXT,
    SampleLocation TEXT,
    RNAPlate TEXT,
    
    -- cDNA
    cDNADate DATE,
    
    -- PanCorona
    PanCoronaDate DATE,
    PanCorona TEXT,
    
    -- PanParamyxo
    PanParamyxoDate DATE,
    PanParamyxo TEXT,
    
    -- PanHanta
    PanHantaDate DATE,
    PanHanta TEXT,
    
    -- PanFlavi
    PanFlaviDate DATE,
    PanFlavi TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_screening_sourceid ON screening_data(SourceId);
CREATE INDEX IF NOT EXISTS idx_screening_sampleid ON screening_data(SampleId);
CREATE INDEX IF NOT EXISTS idx_screening_sampletype ON screening_data(SampleType);
CREATE INDEX IF NOT EXISTS idx_screening_team ON screening_data(Team);

-- ==================== LINKING TABLES ====================

-- Table: bat_screening_link
-- Links bat specimens to screening samples (one bat can have many samples)
CREATE TABLE IF NOT EXISTS bat_screening_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bat_data_id INTEGER NOT NULL,
    screening_data_id INTEGER NOT NULL,
    source_id TEXT,  -- SourceId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (bat_data_id) REFERENCES bat_data(id) ON DELETE CASCADE,
    FOREIGN KEY (screening_data_id) REFERENCES screening_data(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bat_screening_bat ON bat_screening_link(bat_data_id);
CREATE INDEX IF NOT EXISTS idx_bat_screening_screening ON bat_screening_link(screening_data_id);
CREATE INDEX IF NOT EXISTS idx_bat_screening_source ON bat_screening_link(source_id);

-- Table: market_screening_link
-- Links market specimens to screening samples (one market sample can have many samples)
CREATE TABLE IF NOT EXISTS market_screening_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_data_id INTEGER NOT NULL,
    screening_data_id INTEGER NOT NULL,
    source_id TEXT,  -- SourceId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (market_data_id) REFERENCES market_data(id) ON DELETE CASCADE,
    FOREIGN KEY (screening_data_id) REFERENCES screening_data(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_market_screening_market ON market_screening_link(market_data_id);
CREATE INDEX IF NOT EXISTS idx_market_screening_screening ON market_screening_link(screening_data_id);
CREATE INDEX IF NOT EXISTS idx_market_screening_source ON market_screening_link(source_id);

-- Table: rodenthost_screening_link
-- Links rodent host to screening samples (one rodent can have many samples)
CREATE TABLE IF NOT EXISTS rodenthost_screening_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rodent_host_id INTEGER NOT NULL,
    screening_data_id INTEGER NOT NULL,
    field_id TEXT,  -- FieldId for matching
    source_id TEXT,  -- SourceId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (rodent_host_id) REFERENCES rodent_host(id) ON DELETE CASCADE,
    FOREIGN KEY (screening_data_id) REFERENCES screening_data(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rodenthost_screening_rodent ON rodenthost_screening_link(rodent_host_id);
CREATE INDEX IF NOT EXISTS idx_rodenthost_screening_screening ON rodenthost_screening_link(screening_data_id);
CREATE INDEX IF NOT EXISTS idx_rodenthost_screening_field ON rodenthost_screening_link(field_id);
CREATE INDEX IF NOT EXISTS idx_rodenthost_screening_source ON rodenthost_screening_link(source_id);

-- Table: screening_sequence_link
-- Links screening samples to sequences (one sample can have many sequences)
CREATE TABLE IF NOT EXISTS screening_sequence_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    screening_data_id INTEGER NOT NULL,
    sequence_id INTEGER,
    consensus_id INTEGER,
    blast_result_id INTEGER,
    sample_id TEXT,  -- SampleId for matching
    virus_type_matched TEXT,  -- PanCorona, PanHanta, PanFlavi, PanParamyxo
    sequence_confirmed INTEGER DEFAULT 0,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (screening_data_id) REFERENCES screening_data(id) ON DELETE CASCADE,
    FOREIGN KEY (sequence_id) REFERENCES sequences(id) ON DELETE SET NULL,
    FOREIGN KEY (consensus_id) REFERENCES consensus_sequences(id) ON DELETE SET NULL,
    FOREIGN KEY (blast_result_id) REFERENCES blast_results(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_screening_seq_screening ON screening_sequence_link(screening_data_id);
CREATE INDEX IF NOT EXISTS idx_screening_seq_sequence ON screening_sequence_link(sequence_id);
CREATE INDEX IF NOT EXISTS idx_screening_seq_sample ON screening_sequence_link(sample_id);

-- Table: freezer_swab_link
-- Links freezer storage to swab samples
CREATE TABLE IF NOT EXISTS freezer_swab_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    freezer_storage_id INTEGER NOT NULL,
    swab_data_id INTEGER NOT NULL,
    sample_id TEXT,  -- SampleId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (freezer_storage_id) REFERENCES freezer_storage(id) ON DELETE CASCADE,
    FOREIGN KEY (swab_data_id) REFERENCES swab_data(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_freezer_swab_freezer ON freezer_swab_link(freezer_storage_id);
CREATE INDEX IF NOT EXISTS idx_freezer_swab_swab ON freezer_swab_link(swab_data_id);
CREATE INDEX IF NOT EXISTS idx_freezer_swab_sample ON freezer_swab_link(sample_id);

-- Table: freezer_tissue_link
-- Links freezer storage to tissue samples
CREATE TABLE IF NOT EXISTS freezer_tissue_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    freezer_storage_id INTEGER NOT NULL,
    tissue_data_id INTEGER NOT NULL,
    sample_id TEXT,  -- SampleId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (freezer_storage_id) REFERENCES freezer_storage(id) ON DELETE CASCADE,
    FOREIGN KEY (tissue_data_id) REFERENCES tissue_data(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_freezer_tissue_freezer ON freezer_tissue_link(freezer_storage_id);
CREATE INDEX IF NOT EXISTS idx_freezer_tissue_tissue ON freezer_tissue_link(tissue_data_id);
CREATE INDEX IF NOT EXISTS idx_freezer_tissue_sample ON freezer_tissue_link(sample_id);

-- Table: freezer_environmental_link
-- Links freezer storage to environmental samples
CREATE TABLE IF NOT EXISTS freezer_environmental_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    freezer_storage_id INTEGER NOT NULL,
    environmental_data_id INTEGER NOT NULL,
    sample_id TEXT,  -- SampleId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (freezer_storage_id) REFERENCES freezer_storage(id) ON DELETE CASCADE,
    FOREIGN KEY (environmental_data_id) REFERENCES environmental_data(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_freezer_env_freezer ON freezer_environmental_link(freezer_storage_id);
CREATE INDEX IF NOT EXISTS idx_freezer_env_env ON freezer_environmental_link(environmental_data_id);
CREATE INDEX IF NOT EXISTS idx_freezer_env_sample ON freezer_environmental_link(sample_id);

-- Table: freezer_market_link
-- Links freezer storage to market samples
CREATE TABLE IF NOT EXISTS freezer_market_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    freezer_storage_id INTEGER NOT NULL,
    market_data_id INTEGER NOT NULL,
    sample_id TEXT,  -- SampleId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (freezer_storage_id) REFERENCES freezer_storage(id) ON DELETE CASCADE,
    FOREIGN KEY (market_data_id) REFERENCES market_data(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_freezer_market_freezer ON freezer_market_link(freezer_storage_id);
CREATE INDEX IF NOT EXISTS idx_freezer_market_market ON freezer_market_link(market_data_id);
CREATE INDEX IF NOT EXISTS idx_freezer_market_sample ON freezer_market_link(sample_id);

-- Table: freezer_rodentsample_link
-- Links freezer storage to rodent samples
CREATE TABLE IF NOT EXISTS freezer_rodentsample_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    freezer_storage_id INTEGER NOT NULL,
    rodent_sample_id INTEGER NOT NULL,
    sample_id TEXT,  -- SampleId for matching
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (freezer_storage_id) REFERENCES freezer_storage(id) ON DELETE CASCADE,
    FOREIGN KEY (rodent_sample_id) REFERENCES rodent_sample(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_freezer_rodent_freezer ON freezer_rodentsample_link(freezer_storage_id);
CREATE INDEX IF NOT EXISTS idx_freezer_rodent_rodent ON freezer_rodentsample_link(rodent_sample_id);
CREATE INDEX IF NOT EXISTS idx_freezer_rodent_sample ON freezer_rodentsample_link(sample_id);