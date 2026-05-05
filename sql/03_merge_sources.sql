CREATE OR REPLACE TABLE erp AS
SELECT *
FROM read_csv_auto('/app/data/processed/erp_clean.csv');

CREATE OR REPLACE TABLE web AS
SELECT *
FROM read_csv_auto('/app/data/processed/web_clean.csv');

CREATE OR REPLACE TABLE liaison AS
SELECT *
FROM read_csv_auto('/app/data/processed/liaison_clean.csv');

CREATE OR REPLACE TABLE merged AS
SELECT
  erp.*,
  liaison.*,
  web.*
FROM erp
JOIN liaison
  ON erp.product_id = liaison.product_id
JOIN web
  ON liaison.id_web = web.sku;

COPY merged
TO '/app/data/processed/merged.csv'
(HEADER, DELIMITER ',');