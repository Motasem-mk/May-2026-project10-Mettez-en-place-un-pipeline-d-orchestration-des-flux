CREATE OR REPLACE TABLE merged_test_data AS
SELECT *
FROM read_csv_auto('/app/data/processed/merged.csv');

SELECT
  CASE
    WHEN COUNT(*) = 714
    THEN 'MERGE row count test passed: 714 rows'
    ELSE error('MERGE row count test failed')
  END AS test_result
FROM merged_test_data;