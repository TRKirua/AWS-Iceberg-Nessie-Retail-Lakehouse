-- ============================================================================
-- Apache Iceberg Metadata Queries
-- ============================================================================
-- These queries demonstrate Iceberg's time travel and metadata capabilities.

-- ----------------------------------------------------------------------------
-- Snapshot History
-- ----------------------------------------------------------------------------

-- Bronze table history
SELECT * FROM nessie.bronze.sales.history
ORDER BY made_current_at DESC;

-- Silver table history
SELECT * FROM nessie.silver.sales.history
ORDER BY made_current_at DESC;

-- Gold table history
SELECT * FROM nessie.gold.sales_by_category_region.history
ORDER BY made_current_at DESC;

-- ----------------------------------------------------------------------------
-- Snapshot Details
-- ----------------------------------------------------------------------------

-- Bronze snapshots with operation types
SELECT
    committed_at,
    snapshot_id,
    operation,
    CAST(summary['added-records'] AS BIGINT) AS added_records,
    CAST(summary['total-records'] AS BIGINT) AS total_records,
    CAST(summary['added-files-size'] AS BIGINT) AS added_file_size
FROM nessie.bronze.sales.snapshots
ORDER BY committed_at DESC;

-- Silver snapshots
SELECT
    committed_at,
    snapshot_id,
    operation,
    CAST(summary['added-records'] AS BIGINT) AS added_records,
    CAST(summary['total-records'] AS BIGINT) AS total_records
FROM nessie.silver.sales.snapshots
ORDER BY committed_at DESC;

-- Gold snapshots
SELECT
    committed_at,
    snapshot_id,
    operation,
    CAST(summary['added-records'] AS BIGINT) AS added_records,
    CAST(summary['total-records'] AS BIGINT) AS total_records
FROM nessie.gold.sales_by_category_region.snapshots
ORDER BY committed_at DESC;

-- ----------------------------------------------------------------------------
-- Manifest Information
-- ----------------------------------------------------------------------------

-- Bronze manifests
SELECT
    path,
    length,
    partition_spec_id,
    added_snapshot_id
FROM nessie.bronze.sales.manifests
ORDER BY added_snapshot_id DESC;

-- ----------------------------------------------------------------------------
-- Partitions
-- ----------------------------------------------------------------------------

-- Bronze partitions
SELECT * FROM nessie.bronze.sales.partitions;

-- Silver partitions
SELECT * FROM nessie.silver.sales.partitions;

-- ----------------------------------------------------------------------------
-- Time Travel Examples
-- ----------------------------------------------------------------------------

-- To query a specific snapshot, replace SNAPSHOT_ID with an actual ID from the history table

-- Example: Query Bronze at a specific snapshot
-- SELECT * FROM nessie.bronze.sales VERSION AS OF '1234567890123456789' LIMIT 10;

-- Example: Query Silver at a specific timestamp
-- SELECT * FROM nessie.silver.sales TIMESTAMP AS OF '2024-01-01 12:00:00';

-- Example: Compare current vs previous snapshot
-- WITH current AS (
--     SELECT COUNT(*) AS count FROM nessie.bronze.sales
-- ),
-- previous AS (
--     SELECT COUNT(*) AS count FROM nessie.bronze.sales VERSION AS OF 'PREVIOUS_SNAPSHOT_ID'
-- )
-- SELECT
--     (SELECT count FROM current) - (SELECT count FROM previous) AS records_added;

-- ----------------------------------------------------------------------------
-- File Metadata
-- ----------------------------------------------------------------------------

-- All data files for Bronze
SELECT
    file_path,
    file_format,
    record_count,
    file_size_in_bytes
FROM nessie.bronze.sales.files
ORDER BY file_path;

-- Files by partition for Bronze
SELECT
    partition.ingestion_date,
    COUNT(*) AS file_count,
    SUM(record_count) AS total_records,
    SUM(file_size_in_bytes) AS total_size_bytes
FROM nessie.bronze.sales.files
GROUP BY partition.ingestion_date
ORDER BY partition.ingestion_date DESC;

-- ----------------------------------------------------------------------------
-- References (current snapshot pointers)
-- ----------------------------------------------------------------------------

-- All references (branches, tags, etc.)
SELECT * FROM nessie.bronze.sales.references;

-- ----------------------------------------------------------------------------
-- Rollback Examples (use with caution)
-- ----------------------------------------------------------------------------

-- To roll back to a previous snapshot:
-- CALL nessie.system.rollback_to_snapshot('bronze.sales', SNAPSHOT_ID);

-- To create a branch from a specific snapshot:
-- CALL nessie.system.create_branch('bronze.sales', SNAPSHOT_ID);

-- ----------------------------------------------------------------------------
-- Nessie Catalog Queries
-- ----------------------------------------------------------------------------

-- List all namespaces
SHOW NAMESPACES IN nessie;

-- List all tables
SHOW TABLES IN nessie;

-- List tables in bronze namespace
SHOW TABLES IN nessie.bronze;

-- List tables in silver namespace
SHOW TABLES IN nessie.silver;

-- List tables in gold namespace
SHOW TABLES IN nessie.gold;
