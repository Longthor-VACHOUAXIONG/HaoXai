-- Reference Table: Taxonomy
-- Centralized species classification data

CREATE TABLE IF NOT EXISTS taxonomy (
    taxonomy_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kingdom TEXT,
    phylum TEXT,
    class TEXT,
    order_name TEXT,  -- 'order' is a reserved keyword in SQL
    family TEXT,
    genus TEXT,
    species TEXT NOT NULL,
    scientific_name TEXT NOT NULL,
    common_name TEXT,
    english_common_name TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scientific_name)
);

CREATE INDEX IF NOT EXISTS idx_taxonomy_species ON taxonomy(species);
CREATE INDEX IF NOT EXISTS idx_taxonomy_genus ON taxonomy(genus);
CREATE INDEX IF NOT EXISTS idx_taxonomy_family ON taxonomy(family);
