-- Reference Table: Locations
-- Centralized location data to eliminate redundancy across tables

CREATE TABLE IF NOT EXISTS locations (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
    country TEXT NOT NULL DEFAULT 'Laos',
    province TEXT NOT NULL,
    district TEXT,
    village TEXT,
    latitude REAL,
    longitude REAL,
    altitude REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country, province, district, village)
);

CREATE INDEX IF NOT EXISTS idx_locations_province ON locations(province);
CREATE INDEX IF NOT EXISTS idx_locations_district ON locations(district);
CREATE INDEX IF NOT EXISTS idx_locations_coords ON locations(latitude, longitude);