CREATE OR REPLACE TABLE merged_revenue_test AS
SELECT *
FROM read_csv_auto('/app/data/processed/merged_with_revenue.csv');

CREATE OR REPLACE TABLE revenue_report_test AS
SELECT *
FROM read_csv_auto('/app/data/processed/revenue_per_product.csv');

SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM merged_revenue_test) = 714
     AND (SELECT COUNT(*) FROM revenue_report_test) = 714
     AND (SELECT COUNT(*) FROM merged_revenue_test WHERE revenue IS NULL) = 0
     AND ABS((SELECT SUM(revenue) FROM revenue_report_test) - 70568.60) < 0.01
    THEN 'REVENUE test passed: 714 rows, no missing revenue, total revenue = 70568.60'
    ELSE error('REVENUE test failed')
  END AS test_result;