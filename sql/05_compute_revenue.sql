CREATE OR REPLACE TABLE merged_rev AS
SELECT
  *,
  price * total_sales AS revenue
FROM read_csv_auto('/app/data/processed/merged.csv');

-- Full technical dataset with revenue
COPY merged_rev
TO 'merged_with_revenue.csv'
(HEADER, DELIMITER ',');

COPY merged_rev
TO '/app/data/processed/merged_with_revenue.csv'
(HEADER, DELIMITER ',');


-- Business revenue report
CREATE OR REPLACE TABLE revenue_per_product AS
SELECT
  product_id,
  sku,
  post_title,
  total_sales,
  revenue
FROM merged_rev
ORDER BY revenue DESC;

COPY revenue_per_product
TO 'revenue_per_product.csv'
(HEADER, DELIMITER ',');

COPY revenue_per_product
TO '/app/data/processed/revenue_per_product.csv'
(HEADER, DELIMITER ',');