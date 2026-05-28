-- NYC 311 Pipeline: Database Initialization


CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;


CREATE TABLE IF NOT EXISTS raw.service_requests (
    unique_key          TEXT PRIMARY KEY,
    created_date        TEXT,
    closed_date         TEXT,
    agency              TEXT,
    agency_name         TEXT,
    complaint_type      TEXT,
    descriptor          TEXT,
    location_type       TEXT,
    incident_zip        TEXT,
    city                TEXT,
    borough             TEXT,
    status              TEXT,
    due_date            TEXT,
    resolution_description TEXT,
    community_board     TEXT,
    open_data_channel_type TEXT,
    park_facility_name  TEXT,
    latitude            TEXT,
    longitude           TEXT,
    raw_loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- STAGING LAYER  (cleaned types, no nulls in key cols)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.stg_service_requests (
    unique_key          TEXT PRIMARY KEY,
    created_date        TIMESTAMPTZ,
    closed_date         TIMESTAMPTZ,
    agency_code         TEXT,
    agency_name         TEXT,
    complaint_type      TEXT,
    descriptor          TEXT,
    borough             TEXT,
    incident_zip        TEXT,
    status              TEXT,
    open_channel        TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    resolution_hours    DOUBLE PRECISION,  -- derived: hours from open to close
    is_overdue          BOOLEAN,           -- closed_date > due_date
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
);


-- DIMENSION TABLES

CREATE TABLE IF NOT EXISTS marts.dim_agency (
    agency_id   SERIAL PRIMARY KEY,
    agency_code TEXT UNIQUE NOT NULL,
    agency_name TEXT
);

CREATE TABLE IF NOT EXISTS marts.dim_complaint_type (
    complaint_type_id SERIAL PRIMARY KEY,
    complaint_type    TEXT UNIQUE NOT NULL,
    category          TEXT  -- broad grouping (Noise, Infrastructure, etc.)
);

CREATE TABLE IF NOT EXISTS marts.dim_borough (
    borough_id  SERIAL PRIMARY KEY,
    borough     TEXT UNIQUE NOT NULL,
    region      TEXT
);


-- FACT TABLE

CREATE TABLE IF NOT EXISTS marts.fct_requests (
    request_id          TEXT PRIMARY KEY,
    created_date        TIMESTAMPTZ,
    closed_date         TIMESTAMPTZ,
    agency_id           INT REFERENCES marts.dim_agency(agency_id),
    complaint_type_id   INT REFERENCES marts.dim_complaint_type(complaint_type_id),
    borough_id          INT REFERENCES marts.dim_borough(borough_id),
    status              TEXT,
    open_channel        TEXT,
    resolution_hours    DOUBLE PRECISION,
    is_overdue          BOOLEAN,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    created_year        INT,
    created_month       INT,
    created_day_of_week INT,
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
);

-- PIPELINE AUDIT TABLE  (track every run)
CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    run_started_at  TIMESTAMPTZ DEFAULT NOW(),
    run_finished_at TIMESTAMPTZ,
    status          TEXT,  -- 'running', 'success', 'failed'
    rows_fetched    INT,
    rows_loaded     INT,
    rows_failed_validation INT,
    error_message   TEXT
);

-- Indexes for common dashboard queries
CREATE INDEX IF NOT EXISTS idx_fct_created_date   ON marts.fct_requests(created_date);
CREATE INDEX IF NOT EXISTS idx_fct_borough        ON marts.fct_requests(borough_id);
CREATE INDEX IF NOT EXISTS idx_fct_complaint_type ON marts.fct_requests(complaint_type_id);
CREATE INDEX IF NOT EXISTS idx_fct_status         ON marts.fct_requests(status);

-- Seed dimension tables with known boroughs
INSERT INTO marts.dim_borough (borough, region) VALUES
    ('MANHATTAN',     'New York City'),
    ('BROOKLYN',      'New York City'),
    ('QUEENS',        'New York City'),
    ('BRONX',         'New York City'),
    ('STATEN ISLAND', 'New York City'),
    ('Unspecified',   'New York City')
ON CONFLICT (borough) DO NOTHING;

-- Seed broad complaint categories
INSERT INTO marts.dim_complaint_type (complaint_type, category) VALUES
    ('Noise - Residential',       'Noise'),
    ('Noise - Street/Sidewalk',   'Noise'),
    ('Noise - Commercial',        'Noise'),
    ('HEAT/HOT WATER',            'Housing'),
    ('Blocked Driveway',          'Traffic'),
    ('Illegal Parking',           'Traffic'),
    ('Street Light Condition',    'Infrastructure'),
    ('PLUMBING',                  'Housing'),
    ('Dirty Conditions',          'Sanitation'),
    ('Rodent',                    'Sanitation')
ON CONFLICT (complaint_type) DO NOTHING;
