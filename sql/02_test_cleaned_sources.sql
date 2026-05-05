-- sql/02_test_cleaned_sources.sql
-- Purpose:
-- Test the cleaned ERP, Web, and Liaison datasets after the cleaning/deduplication step.
--
-- This file validates:
-- 1. Expected row counts
-- 2. Absence of missing primary/business keys
-- 3. Absence of duplicate primary/business keys
--
-- Important:
-- We do NOT force liaison.id_web to be non-null here.
-- Some ERP products may not have a matching web product.
-- Join consistency is validated later in 04_test_merge.sql.


-- --------------------------------------------------
-- 1. Reload cleaned files produced by 01_clean_dedup_sources.sql
-- --------------------------------------------------

CREATE OR REPLACE TABLE erp_test_data AS
SELECT *
FROM read_csv_auto('/app/data/processed/erp_clean.csv');

CREATE OR REPLACE TABLE web_test_data AS
SELECT *
FROM read_csv_auto('/app/data/processed/web_clean.csv');

CREATE OR REPLACE TABLE liaison_test_data AS
SELECT *
FROM read_csv_auto('/app/data/processed/liaison_clean.csv');


-- --------------------------------------------------
-- 2. Run quality checks
-- --------------------------------------------------

WITH checks AS (
  SELECT
    -- Row counts
    (SELECT COUNT(*) FROM erp_test_data) AS erp_count,
    (SELECT COUNT(*) FROM web_test_data) AS web_count,
    (SELECT COUNT(*) FROM liaison_test_data) AS liaison_count,

    -- Missing key values
    (SELECT COUNT(*) 
     FROM erp_test_data 
     WHERE product_id IS NULL 
        OR TRIM(CAST(product_id AS VARCHAR)) = ''
    ) AS erp_null_keys,

    (SELECT COUNT(*) 
     FROM web_test_data 
     WHERE sku IS NULL 
        OR TRIM(CAST(sku AS VARCHAR)) = ''
    ) AS web_null_keys,

    (SELECT COUNT(*) 
     FROM liaison_test_data 
     WHERE product_id IS NULL 
        OR TRIM(CAST(product_id AS VARCHAR)) = ''
    ) AS liaison_null_keys,

    -- Duplicate key values
    (SELECT COUNT(*) FROM (
      SELECT product_id
      FROM erp_test_data
      GROUP BY product_id
      HAVING COUNT(*) > 1
    )) AS erp_duplicate_keys,

    (SELECT COUNT(*) FROM (
      SELECT sku
      FROM web_test_data
      GROUP BY sku
      HAVING COUNT(*) > 1
    )) AS web_duplicate_keys,

    (SELECT COUNT(*) FROM (
      SELECT product_id
      FROM liaison_test_data
      GROUP BY product_id
      HAVING COUNT(*) > 1
    )) AS liaison_duplicate_keys
)

SELECT
  CASE
    WHEN erp_count = 825
     AND web_count = 714
     AND liaison_count = 825

     AND erp_null_keys = 0
     AND web_null_keys = 0
     AND liaison_null_keys = 0

     AND erp_duplicate_keys = 0
     AND web_duplicate_keys = 0
     AND liaison_duplicate_keys = 0

    THEN
      'TEST PASSED: no missing keys, no duplicate keys. Row counts: ERP=825, WEB=714, LIAISON=825'

    ELSE
      error('TEST FAILED: cleaned source files contain wrong row counts, missing keys, or duplicate keys.')
  END AS test_result
FROM checks;