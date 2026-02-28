-- Sequences Database Schema
-- Stores uploaded sequences, consensus sequences, and BLAST results

-- Table: projects
-- Stores project information for organizing sequences
CREATE TABLE IF NOT EXISTS projects (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_by VARCHAR(100),
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_project_name (project_name),
    INDEX idx_project_created_date (created_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: sequences
-- Stores individual AB1 sequence files with quality metrics
CREATE TABLE IF NOT EXISTS sequences (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    filename VARCHAR(255) NOT NULL,
    upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    file_hash VARCHAR(64) UNIQUE,  -- MD5 hash to prevent duplicates
    
    -- Sequence data
    sequence TEXT NOT NULL,
    sequence_length INT NOT NULL,
    
    -- Grouping and direction
    group_name VARCHAR(255),
    detected_direction ENUM('Forward', 'Reverse', 'Unknown') DEFAULT 'Unknown',
    
    -- Quality metrics
    quality_score FLOAT,
    avg_quality FLOAT,
    min_quality FLOAT,
    max_quality FLOAT,
    
    -- Advanced quality analysis
    overall_grade ENUM('Excellent', 'Good', 'Acceptable', 'Poor', 'Needs Work', 'Unknown') DEFAULT 'Unknown',
    grade_score INT,
    
    -- Issues and flags
    issues JSON,  -- Array of issue descriptions
    likely_swapped BOOLEAN DEFAULT FALSE,
    direction_mismatch BOOLEAN DEFAULT FALSE,
    complementarity_score FLOAT,
    
    -- Ambiguity
    ambiguity_count INT,
    ambiguity_percent FLOAT,
    
    -- Additional metadata
    virus_type VARCHAR(100),
    reference_used BOOLEAN DEFAULT FALSE,
    processing_method VARCHAR(100),
    
    -- Sample identification
    sample_id VARCHAR(200),
    db_sample_id INT,  -- Foreign key to samples.sample_id in CAN2 database
    target_sequence VARCHAR(100),
    
    -- User tracking
    uploaded_by VARCHAR(100),
    project_name VARCHAR(255),
    notes TEXT,
    
    INDEX idx_group_name (group_name),
    INDEX idx_upload_date (upload_date),
    INDEX idx_overall_grade (overall_grade),
    INDEX idx_virus_type (virus_type),
    INDEX idx_sample_id (sample_id),
    INDEX idx_db_sample_id (db_sample_id),
    INDEX idx_target_sequence (target_sequence)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: consensus_sequences
-- Stores generated consensus sequences from paired/grouped AB1 files
CREATE TABLE IF NOT EXISTS consensus_sequences (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    consensus_name VARCHAR(255) NOT NULL,
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- Consensus data
    consensus_sequence TEXT NOT NULL,
    original_length INT NOT NULL,
    trimmed_length INT NOT NULL,
    
    -- Source information
    group_name VARCHAR(255),
    file_count INT DEFAULT 1,
    source_file_ids JSON,  -- Array of sequence IDs used to create this consensus
    
    -- Sample identification
    sample_id VARCHAR(255),
    db_sample_id INT,  -- Foreign key to samples.sample_id in CAN2 database
    target_sequence VARCHAR(255),
    
    -- Processing details
    virus_type VARCHAR(100),
    trim_method VARCHAR(100),
    quality_threshold FLOAT,
    
    -- Metadata
    uploaded_by VARCHAR(100),
    project_name VARCHAR(255),
    notes TEXT,
    
    INDEX idx_consensus_name (consensus_name),
    INDEX idx_created_date (created_date),
    INDEX idx_group_name (group_name),
    INDEX idx_sample_id (sample_id),
    INDEX idx_db_sample_id (db_sample_id),
    INDEX idx_target_sequence (target_sequence),
    INDEX idx_virus_type (virus_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: blast_results
-- Stores BLAST search results for consensus sequences
CREATE TABLE IF NOT EXISTS blast_results (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    consensus_id INT NOT NULL,
    blast_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- BLAST parameters
    blast_mode ENUM('viruses', 'all') DEFAULT 'viruses',
    database_used VARCHAR(50) DEFAULT 'nt',
    program VARCHAR(50) DEFAULT 'blastn',
    
    -- Query information
    query_name VARCHAR(255),
    query_length INT,
    
    -- Result summary
    total_hits INT DEFAULT 0,
    execution_time FLOAT,  -- seconds
    
    -- Status
    status ENUM('success', 'failed', 'no_hits') DEFAULT 'success',
    error_message TEXT,
    
    FOREIGN KEY (consensus_id) REFERENCES consensus_sequences(id) ON DELETE CASCADE,
    INDEX idx_consensus_id (consensus_id),
    INDEX idx_blast_date (blast_date),
    INDEX idx_blast_mode (blast_mode)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: blast_hits
-- Stores individual BLAST hit details
CREATE TABLE IF NOT EXISTS blast_hits (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    blast_result_id INT NOT NULL,
    hit_rank INT NOT NULL,  -- 1 = top hit
    
    -- Hit identification
    accession VARCHAR(100),
    title TEXT,
    organism VARCHAR(255),
    
    -- Alignment metrics
    query_coverage FLOAT,
    identity_percent FLOAT,
    evalue DOUBLE,
    bit_score FLOAT,
    
    -- Alignment details
    align_length INT,
    query_from INT,
    query_to INT,
    hit_from INT,
    hit_to INT,
    gaps INT,
    
    FOREIGN KEY (blast_result_id) REFERENCES blast_results(id) ON DELETE CASCADE,
    INDEX idx_blast_result_id (blast_result_id),
    INDEX idx_hit_rank (hit_rank),
    INDEX idx_accession (accession),
    INDEX idx_organism (organism)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: projects
-- Optional: Organize sequences into projects
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    project_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    
    -- Project settings
    default_virus_type VARCHAR(100),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    INDEX idx_project_name (project_name),
    INDEX idx_created_date (created_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;