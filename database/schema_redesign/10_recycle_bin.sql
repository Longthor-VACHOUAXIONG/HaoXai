-- Utility Table: RecycleBin
-- Keep existing recycle bin for audit purposes

CREATE TABLE IF NOT EXISTS recycle_bin (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_table TEXT NOT NULL,
    data TEXT NOT NULL,  -- JSON representation of deleted row
    deleted_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_recycle_table ON recycle_bin(original_table);
CREATE INDEX IF NOT EXISTS idx_recycle_date ON recycle_bin(deleted_at);