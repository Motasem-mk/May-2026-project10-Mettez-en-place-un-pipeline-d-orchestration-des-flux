-- 1. Clean and deduplicate ERP source

CREATE OR REPLACE TABLE erp_clean AS
SELECT *
FROM read_csv_auto('erp.csv')
WHERE product_id IS NOT NULL;

CREATE OR REPLACE TABLE erp_deduped AS
SELECT DISTINCT *
FROM erp_clean;

COPY erp_deduped
TO '/app/data/processed/erp_clean.csv'
(HEADER, DELIMITER ',');


-- 2. Clean and deduplicate Web source

CREATE OR REPLACE TABLE web_clean AS
SELECT *
FROM read_csv_auto('web.csv')
WHERE sku IS NOT NULL;

CREATE OR REPLACE TABLE web_sorted AS
SELECT *,
       ROW_NUMBER() OVER (
         PARTITION BY sku
         ORDER BY total_sales DESC
       ) AS rn
FROM web_clean;

CREATE OR REPLACE TABLE web_deduped AS
SELECT * EXCLUDE(rn)
FROM web_sorted
WHERE rn = 1;

COPY web_deduped
TO '/app/data/processed/web_clean.csv'
(HEADER, DELIMITER ',');


-- 3. Clean and deduplicate Liaison source

CREATE OR REPLACE TABLE liaison_clean AS
SELECT *
FROM read_csv_auto('liaison.csv');

CREATE OR REPLACE TABLE liaison_deduped AS
SELECT DISTINCT *
FROM liaison_clean;

COPY liaison_deduped
TO '/app/data/processed/liaison_clean.csv'
(HEADER, DELIMITER ',');