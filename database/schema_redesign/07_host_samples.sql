-- Linking Table: Host-Samples
-- Many-to-many relationship between hosts and samples
-- One host can have multiple samples; one sample can come from multiple hosts (pooled)

CREATE TABLE IF NOT EXISTS host_samples (
    host_sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id INTEGER NOT NULL,
    sample_id INTEGER NOT NULL,
    collection_sequence INTEGER,  -- Order of collection (1st, 2nd, 3rd sample from host)
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (host_id) REFERENCES hosts(host_id) ON DELETE CASCADE,
    FOREIGN KEY (sample_id) REFERENCES samples(sample_id) ON DELETE CASCADE,
    UNIQUE(host_id, sample_id)
);

CREATE INDEX IF NOT EXISTS idx_host_samples_host ON host_samples(host_id);
CREATE INDEX IF NOT EXISTS idx_host_samples_sample ON host_samples(sample_id);
