CREATE OR REPLACE TABLE premium_wines AS
SELECT
  product_id,
  sku,
  post_title,
  price,
  total_sales,
  revenue,
  stock_status,
  z_score,
  is_vintage,
  guid
FROM read_csv_auto('/app/data/processed/premium_flagged.csv')
WHERE is_vintage = TRUE
ORDER BY price DESC;

COPY premium_wines
TO '/app/data/outputs/premium_wines.csv'
(HEADER, DELIMITER ',');