-- SQLite version of Sequences Database Schema

-- Table: projects
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_by TEXT,
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_project_name ON projects(project_name);
CREATE INDEX IF NOT EXISTS idx_project_created_date ON projects(created_date);

-- Table: sequences
CREATE TABLE IF NOT EXISTS sequences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    file_hash TEXT UNIQUE,
    
    sequence TEXT NOT NULL,
    sequence_length INTEGER NOT NULL,
    
    group_name TEXT,
    detected_direction TEXT CHECK(detected_direction IN ('Forward', 'Reverse', 'Unknown')) DEFAULT 'Unknown',
    
    quality_score REAL,
    avg_quality REAL,
    min_quality REAL,
    max_quality REAL,
    
    overall_grade TEXT CHECK(overall_grade IN ('Excellent', 'Good', 'Acceptable', 'Poor', 'Needs Work', 'Unknown')) DEFAULT 'Unknown',
    grade_score INTEGER,
    
    issues TEXT,  -- JSON string
    likely_swapped INTEGER DEFAULT 0,
    direction_mismatch INTEGER DEFAULT 0,
    complementarity_score REAL,
    
    ambiguity_count INTEGER,
    ambiguity_percent REAL,
    
    virus_type TEXT,
    reference_used INTEGER DEFAULT 0,
    processing_method TEXT,
    
    sample_id TEXT,
    db_sample_id INTEGER,  -- Foreign key to samples.sample_id in main database
    target_sequence TEXT,
    
    uploaded_by TEXT,
    project_name TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_seq_group_name ON sequences(group_name);
CREATE INDEX IF NOT EXISTS idx_seq_sample_id ON sequences(sample_id);
CREATE INDEX IF NOT EXISTS idx_seq_db_sample_id ON sequences(db_sample_id);
CREATE INDEX IF NOT EXISTS idx_seq_target_sequence ON sequences(target_sequence);
CREATE INDEX IF NOT EXISTS idx_seq_upload_date ON sequences(upload_date);
CREATE INDEX IF NOT EXISTS idx_seq_overall_grade ON sequences(overall_grade);

-- Table: consensus_sequences
CREATE TABLE IF NOT EXISTS consensus_sequences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    consensus_name TEXT NOT NULL,
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    consensus_sequence TEXT NOT NULL,
    original_length INTEGER NOT NULL,
    trimmed_length INTEGER NOT NULL,
    
    group_name TEXT,
    file_count INTEGER DEFAULT 1,
    source_file_ids TEXT,  -- JSON array
    
    sample_id TEXT,
    db_sample_id INTEGER,  -- Foreign key to samples.sample_id in main database
    target_sequence TEXT,
    
    virus_type TEXT,
    trim_method TEXT,
    quality_threshold REAL,
    
    uploaded_by TEXT,
    project_name TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_cons_name ON consensus_sequences(consensus_name);
CREATE INDEX IF NOT EXISTS idx_cons_created_date ON consensus_sequences(created_date);
CREATE INDEX IF NOT EXISTS idx_cons_group_name ON consensus_sequences(group_name);
CREATE INDEX IF NOT EXISTS idx_cons_sample_id ON consensus_sequences(sample_id);
CREATE INDEX IF NOT EXISTS idx_cons_db_sample_id ON consensus_sequences(db_sample_id);
CREATE INDEX IF NOT EXISTS idx_cons_target_sequence ON consensus_sequences(target_sequence);

-- Table: blast_results
CREATE TABLE IF NOT EXISTS blast_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    consensus_id INTEGER NOT NULL,
    blast_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    blast_mode TEXT CHECK(blast_mode IN ('viruses', 'all')) DEFAULT 'viruses',
    database_used TEXT DEFAULT 'nt',
    program TEXT DEFAULT 'blastn',
    
    query_name TEXT,
    query_length INTEGER,
    
    total_hits INTEGER DEFAULT 0,
    execution_time REAL,
    
    status TEXT CHECK(status IN ('success', 'failed', 'no_hits')) DEFAULT 'success',
    error_message TEXT,
    
    FOREIGN KEY (consensus_id) REFERENCES consensus_sequences(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_blast_consensus_id ON blast_results(consensus_id);
CREATE INDEX IF NOT EXISTS idx_blast_date ON blast_results(blast_date);

-- Table: blast_hits
CREATE TABLE IF NOT EXISTS blast_hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blast_result_id INTEGER NOT NULL,
    hit_rank INTEGER NOT NULL,
    
    accession TEXT,
    title TEXT,
    organism TEXT,
    
    query_coverage REAL,
    identity_percent REAL,
    evalue REAL,
    bit_score REAL,
    
    align_length INTEGER,
    query_from INTEGER,
    query_to INTEGER,
    hit_from INTEGER,
    hit_to INTEGER,
    gaps INTEGER,
    
    FOREIGN KEY (blast_result_id) REFERENCES blast_results(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_hit_blast_result_id ON blast_hits(blast_result_id);
CREATE INDEX IF NOT EXISTS idx_hit_rank ON blast_hits(hit_rank);
CREATE INDEX IF NOT EXISTS idx_hit_accession ON blast_hits(accession);

-- Table: projects
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    
    default_virus_type TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_proj_name ON projects(project_name);