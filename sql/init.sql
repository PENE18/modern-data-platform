-- ============================================
-- Database Initialization Script
-- Creates databases and schemas for the platform
-- ============================================

-- Create Airflow database
CREATE DATABASE airflow;

-- Create Iceberg catalog database
CREATE DATABASE iceberg_catalog;

-- Connect to iceberg_catalog and set up schemas
\c iceberg_catalog;

-- Create iceberg schema
CREATE SCHEMA IF NOT EXISTS iceberg;

-- Grant all privileges
GRANT ALL PRIVILEGES ON SCHEMA iceberg TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iceberg TO admin;

-- Create Iceberg tables metadata storage
CREATE TABLE IF NOT EXISTS iceberg.iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location TEXT,
    previous_metadata_location TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg.iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255),
    property_value TEXT,
    PRIMARY KEY (catalog_name, namespace, property_key)
);

-- Create audit table for tracking changes
CREATE TABLE IF NOT EXISTS iceberg.audit_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(500),
    operation VARCHAR(50),
    username VARCHAR(100),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON iceberg.audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_table ON iceberg.audit_log(table_name);

-- Create materialized view for table statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS iceberg.table_stats AS
SELECT 
    table_namespace,
    table_name,
    COUNT(*) as version_count,
    MAX(last_updated) as last_modified
FROM iceberg.iceberg_tables
GROUP BY table_namespace, table_name;

-- Function to refresh stats
CREATE OR REPLACE FUNCTION refresh_table_stats()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW iceberg.table_stats;
END;
$$ LANGUAGE plpgsql;

-- Insert initial namespace
INSERT INTO iceberg.iceberg_namespace_properties (catalog_name, namespace, property_key, property_value)
VALUES 
    ('prod', 'bronze', 'location', 's3a://lakehouse/bronze'),
    ('prod', 'silver', 'location', 's3a://lakehouse/silver'),
    ('prod', 'gold', 'location', 's3a://lakehouse/gold')
ON CONFLICT DO NOTHING;

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Database initialization completed successfully!';
    RAISE NOTICE 'Databases created: airflow, iceberg_catalog';
    RAISE NOTICE 'Schemas created: iceberg';
END $$;
