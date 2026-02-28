-- Core Entity: Screening
-- Virus screening/testing results
-- Normalized from original screening table (one row per test instead of multiple tests per row)

CREATE TABLE IF NOT EXISTS screening (
    screening_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id INTEGER NOT NULL,
    
    -- Test Information
    test_type TEXT NOT NULL CHECK(test_type IN (
        'PanCorona', 'PanParamyxo', 'PanHanta', 'PanFlavi', 'Other'
    )),
    test_date DATE,
    test_result TEXT CHECK(test_result IN ('Positive', 'Negative', 'Inconclusive', 'Pending', NULL)),
    ct_value REAL,  -- Cycle threshold for qPCR (lower = more virus)
    
    -- Team
    team_id INTEGER,
    tested_by TEXT,
    
    -- Notes
    notes TEXT,
    
    -- Audit
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (sample_id) REFERENCES samples(sample_id) ON DELETE CASCADE,
    FOREIGN KEY (team_id) REFERENCES teams(team_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_screening_sample ON screening(sample_id);
CREATE INDEX IF NOT EXISTS idx_screening_test_type ON screening(test_type);
CREATE INDEX IF NOT EXISTS idx_screening_result ON screening(test_result);
CREATE INDEX IF NOT EXISTS idx_screening_date ON screening(test_date);
CREATE INDEX IF NOT EXISTS idx_screening_team ON screening(team_id);