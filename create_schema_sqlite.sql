-- =====================================================
-- WILDLIFE DISEASE SURVEILLANCE DATABASE
-- Full Schema with all fixes applied
-- Run with: sqlite3 your_database.db < create_database.sql
-- =====================================================

PRAGMA foreign_keys = ON;

-- =====================================================
-- 1. LOCATIONS
-- FIX: latitude, longitude, altitude changed TEXT → REAL
-- =====================================================
CREATE TABLE locations (
    location_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    country             TEXT,
    province            TEXT,
    district            TEXT,
    village             TEXT,
    site_name           TEXT,
    latitude            REAL,    -- ✅ fixed: was TEXT
    longitude           REAL,    -- ✅ fixed: was TEXT
    altitude            REAL,    -- ✅ fixed: was TEXT
    habitat_description TEXT,
    habitat_photo       TEXT,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (province, district, village, site_name)
);

-- =====================================================
-- 2. TAXONOMY
-- =====================================================
CREATE TABLE taxonomy (
    taxonomy_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    kingdom             TEXT,
    phylum              TEXT,
    class_name          TEXT,
    order_name          TEXT,
    family              TEXT,
    genus               TEXT,
    species             TEXT,
    scientific_name     TEXT UNIQUE,
    common_name         TEXT,
    english_common_name TEXT,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 3. HOSTS
-- FIX: added sample_id FK to link hosts → samples
-- =====================================================
CREATE TABLE hosts (
    host_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id      TEXT NOT NULL UNIQUE,
    sample_id      INTEGER,                              -- ✅ fixed: proper FK to samples
    host_type      TEXT NOT NULL CHECK(host_type IN ('Bat','Rodent','Market')),
    bag_id         TEXT,
    field_id       TEXT,
    collection_id  TEXT,
    location_id    INTEGER,
    taxonomy_id    INTEGER,
    capture_date   DATE,
    capture_time   TIME,                                 -- improved: was TEXT
    trap_type      TEXT,
    collectors     TEXT,
    sex            TEXT,
    status         TEXT,
    age            TEXT,
    ring_no        TEXT,
    recapture      TEXT,
    photo          TEXT,
    voucher_code   TEXT,
    ecology        TEXT,
    interface_type TEXT,
    use_for        TEXT,
    notes          TEXT,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sample_id)   REFERENCES samples(sample_id)   ON DELETE SET NULL,
    FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE SET NULL,
    FOREIGN KEY (taxonomy_id) REFERENCES taxonomy(taxonomy_id)  ON DELETE SET NULL
);

CREATE INDEX idx_hosts_sample_id   ON hosts(sample_id);
CREATE INDEX idx_hosts_location_id ON hosts(location_id);

-- =====================================================
-- 4. MORPHOMETRICS
-- =====================================================
CREATE TABLE morphometrics (
    morpho_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id      INTEGER NOT NULL UNIQUE,
    weight_g     REAL,
    head_body_mm REAL,
    forearm_mm   REAL,
    ear_mm       REAL,
    tail_mm      REAL,
    tibia_mm     REAL,
    hind_foot_mm REAL,
    third_mt     REAL,
    fourth_mt    REAL,
    fifth_mt     REAL,
    third_d1p    REAL,
    third_d2p    REAL,
    fourth_d1p   REAL,
    fourth_d2p   REAL,
    mammae       TEXT,
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (host_id) REFERENCES hosts(host_id) ON DELETE CASCADE
);

-- =====================================================
-- 5. ENVIRONMENTAL SAMPLES
-- =====================================================
CREATE TABLE environmental_samples (
    env_sample_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id         TEXT NOT NULL UNIQUE,
    guano_id          TEXT,
    collection_method TEXT,
    collection_date   DATE,
    location_id       INTEGER,
    site_type         TEXT,
    remark            TEXT,
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE SET NULL
);

-- =====================================================
-- 6. SAMPLES (Sampling Event)
-- FIX: removed duplicate province/district/village columns
--      location_id FK is the single source of truth
-- =====================================================
CREATE TABLE samples (
    sample_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    sample_origin   TEXT NOT NULL CHECK (
                        sample_origin IN ('Bat','Rodent','Market','Environmental')
                    ),
    collection_date DATETIME NOT NULL,
    location_id     INTEGER,                             -- ✅ single source of location truth
    remark          TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id),
    FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE SET NULL
);

CREATE INDEX idx_samples_location ON samples(location_id);

-- =====================================================
-- 7. SAMPLE TUBES (Actual Lab Tubes)
-- =====================================================
CREATE TABLE sample_tubes (
    tube_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id          INTEGER NOT NULL,
    tube_type          TEXT NOT NULL CHECK (
                           tube_type IN (
                               'saliva','anal','urine','ecto',
                               'blood','tissue','intestine','adipose','plasma'
                           )
                       ),
    tube_code          TEXT NOT NULL UNIQUE,
    tissue_sample_type TEXT,
    created_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sample_id) REFERENCES samples(sample_id) ON DELETE CASCADE,
    UNIQUE(sample_id, tube_type)
);

CREATE INDEX idx_tubes_sample ON sample_tubes(sample_id);

-- =====================================================
-- 8. STORAGE (Freezer Tracking)
-- =====================================================
CREATE TABLE storage (
    storage_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    tube_id       INTEGER NOT NULL UNIQUE,
    freezer_no    TEXT NOT NULL,
    shelf         TEXT NOT NULL,
    rack          TEXT NOT NULL,
    box           TEXT NOT NULL,
    position      TEXT NOT NULL,
    spot_position TEXT,
    stored_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP,   -- improved: track tube moves
    FOREIGN KEY (tube_id) REFERENCES sample_tubes(tube_id) ON DELETE CASCADE,
    UNIQUE(freezer_no, shelf, rack, box, position)
);

CREATE INDEX idx_storage_freezer ON storage(freezer_no);

-- =====================================================
-- 9. SCREENING RESULTS
-- FIX: added CHECK constraints on result columns
-- =====================================================
CREATE TABLE screening_results (
    screening_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    tube_id          INTEGER NOT NULL,
    team             TEXT,
    tested_sample_id TEXT NOT NULL,
    pan_corona       TEXT CHECK(pan_corona   IN ('Positive','Negative','Inconclusive','Pending')),  -- ✅ fixed
    pan_hanta        TEXT CHECK(pan_hanta    IN ('Positive','Negative','Inconclusive','Pending')),  -- ✅ fixed
    pan_paramyxo     TEXT CHECK(pan_paramyxo IN ('Positive','Negative','Inconclusive','Pending')),  -- ✅ fixed
    pan_flavi        TEXT CHECK(pan_flavi    IN ('Positive','Negative','Inconclusive','Pending')),  -- ✅ fixed
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tube_id) REFERENCES sample_tubes(tube_id) ON DELETE CASCADE
);

-- =====================================================
-- 10. PROJECTS
-- =====================================================
CREATE TABLE projects (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    project_name     TEXT NOT NULL UNIQUE,
    description      TEXT,
    created_by       TEXT,
    created_date     DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_date     DATETIME DEFAULT CURRENT_TIMESTAMP,
    default_virus_type TEXT,
    is_active        INTEGER DEFAULT 1
);

-- =====================================================
-- 11. SEQUENCES
-- =====================================================
CREATE TABLE sequences (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    filename             TEXT NOT NULL,
    upload_date          DATETIME DEFAULT CURRENT_TIMESTAMP,
    file_hash            TEXT UNIQUE,
    sequence             TEXT NOT NULL,
    sequence_length      INTEGER NOT NULL,
    group_name           TEXT,
    detected_direction   TEXT CHECK(detected_direction IN ('Forward','Reverse','Unknown')) DEFAULT 'Unknown',
    quality_score        REAL,
    avg_quality          REAL,
    min_quality          REAL,
    max_quality          REAL,
    overall_grade        TEXT CHECK(overall_grade IN ('Excellent','Good','Acceptable','Poor','Needs Work','Unknown')) DEFAULT 'Unknown',
    grade_score          INTEGER,
    issues               TEXT,
    likely_swapped       INTEGER DEFAULT 0,
    direction_mismatch   INTEGER DEFAULT 0,
    complementarity_score REAL,
    ambiguity_count      INTEGER,
    ambiguity_percent    REAL,
    virus_type           TEXT,
    reference_used       INTEGER DEFAULT 0,
    processing_method    TEXT,
    db_sample_id         INTEGER,
    target_sequence      TEXT,
    uploaded_by          TEXT,
    project_name         TEXT,
    notes                TEXT,
    FOREIGN KEY (db_sample_id) REFERENCES samples(sample_id) ON DELETE CASCADE
);

-- =====================================================
-- 12. CONSENSUS SEQUENCES
-- =====================================================
CREATE TABLE consensus_sequences (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    consensus_name     TEXT NOT NULL,
    created_date       DATETIME DEFAULT CURRENT_TIMESTAMP,
    consensus_sequence TEXT NOT NULL,
    original_length    INTEGER NOT NULL,
    trimmed_length     INTEGER NOT NULL,
    group_name         TEXT,
    file_count         INTEGER DEFAULT 1,
    source_file_ids    TEXT,
    db_sample_id       INTEGER,
    target_sequence    TEXT,
    virus_type         TEXT,
    trim_method        TEXT,
    quality_threshold  REAL,
    uploaded_by        TEXT,
    project_name       TEXT,
    notes              TEXT,
    FOREIGN KEY (db_sample_id) REFERENCES samples(sample_id) ON DELETE CASCADE
);

-- =====================================================
-- 13. BLAST RESULTS
-- =====================================================
CREATE TABLE blast_results (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    consensus_id   INTEGER NOT NULL,
    blast_date     DATETIME DEFAULT CURRENT_TIMESTAMP,
    blast_mode     TEXT CHECK(blast_mode IN ('viruses','all')) DEFAULT 'viruses',
    database_used  TEXT DEFAULT 'nt',
    program        TEXT DEFAULT 'blastn',
    query_name     TEXT,
    query_length   INTEGER,
    total_hits     INTEGER DEFAULT 0,
    execution_time REAL,
    status         TEXT CHECK(status IN ('success','failed','no_hits')) DEFAULT 'success',
    error_message  TEXT,
    FOREIGN KEY (consensus_id) REFERENCES consensus_sequences(id) ON DELETE CASCADE
);

-- =====================================================
-- 14. BLAST HITS
-- =====================================================
CREATE TABLE blast_hits (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    blast_result_id INTEGER NOT NULL,
    hit_rank        INTEGER NOT NULL,
    accession       TEXT,
    title           TEXT,
    organism        TEXT,
    query_coverage  REAL,
    identity_percent REAL,
    evalue          REAL,
    bit_score       REAL,
    align_length    INTEGER,
    query_from      INTEGER,
    query_to        INTEGER,
    hit_from        INTEGER,
    hit_to          INTEGER,
    gaps            INTEGER,
    FOREIGN KEY (blast_result_id) REFERENCES blast_results(id) ON DELETE CASCADE
);