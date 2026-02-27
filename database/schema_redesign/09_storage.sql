-- Core Entity: Storage
-- Freezer storage locations for samples
-- Replaces: freezer14, datatopacks (partially)

CREATE TABLE IF NOT EXISTS storage (
    storage_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id INTEGER NOT NULL,
    
    -- Storage Location
    freezer_name TEXT NOT NULL,  -- Freezer-14, Freezer-80, etc.
    cabinet_no INTEGER,
    cabinet_floor INTEGER,
    floor_row INTEGER,
    floor_column INTEGER,
    box_no INTEGER,
    spot_position INTEGER,
    
    -- Status
    storage_date DATE DEFAULT (DATE('now')),
    removed_date DATE,
    current_location INTEGER DEFAULT 1,  -- Boolean: 1 = currently here, 0 = moved
    
    -- Notes
    notes TEXT,
    
    -- Audit
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (sample_id) REFERENCES samples(sample_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_storage_sample ON storage(sample_id);
CREATE INDEX IF NOT EXISTS idx_storage_freezer ON storage(freezer_name);
CREATE INDEX IF NOT EXISTS idx_storage_current ON storage(current_location);
CREATE INDEX IF NOT EXISTS idx_storage_location ON storage(cabinet_no, cabinet_floor, floor_row, floor_column, box_no, spot_position);
